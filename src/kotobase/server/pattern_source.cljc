(ns kotobase.server.pattern-source
  "`datom.source/IPatternSource` over `hot-datoms` — the in-process half of
  the Datomic-API-as-an-index seam (superproject ADR-2608039970).

  ## What it replaces, and why that matters

  Compiled-query reads historically went through `hot-db`: hydrate the whole
  chain into a db value, then run an algebra over it. That is O(database) per
  query regardless of how few rows come back — `datom.source`'s own docstring
  names this as the reason it exists. SPARQL and Cypher now consume this
  source; Datomic-shaped `do-q` remains on `hot-db` until its broader grammar
  has an equivalent source-backed plan.

  `hot-datoms` is already the shape that avoids it: a filtered index read
  (`:index` + prefix `:components`, snapshot + novelty merge, range-pruned on
  the snapshot side, never a whole-graph rehydrate). One triple pattern is
  one `hot-datoms` call. A query naming two predicates reads two index
  ranges, whatever the graph's size.

  ## The plan is shared, deliberately

  Which index answers which bound positions lives in
  `kotobase.datom-plan` (`kotoba-lang/kotobase-client`), the same namespace
  `kotobase.datom-source` uses for the HTTP transport. Two transports, one
  plan: if the in-process reads and the remote reads ever disagreed about
  which index answers `[_ p o]`, the two would return different answers to
  the same query and nothing would say so.

  ## Async construction, synchronous scanning

  `-scan` is synchronous by contract and `kotoba-lang/sparql`'s algebra walks
  it synchronously, while `hot-datoms` is a value on JVM and a Promise on
  cljs (ADR-2607051000). So `source-for` PREFETCHES the patterns a query
  names and hands back a plain source over the result — the same split
  `kotobase-protocols-worker` uses for documents, and the same one
  `kotobase.datom-source` uses over HTTP.

  What is read is the query's patterns. Not the database.

  ## The object position IS pushed down, and here is the measurement

  It was withheld at first because a one-datom corpus cannot tell `applied
  and matched` from `ignored`, and a value component read wrongly returns
  FEWER rows — a wrong answer that looks like an empty result. Measured
  2026-08-04 against three datoms on one attribute, two of them sharing a
  value (`e1 :role admin`, `e2 :role user`, `e3 :role admin`):

    :avet (no components)                -> 3 rows
    :avet components [\":role\"]           -> 3 rows
    :avet components [\":role\" \"admin\"]   -> 2 rows, exactly e1 and e3
    :avet components [\":role\" \"\\\"admin\\\"\"] -> 0 rows
    :aevt components [\":role\"]           -> 3 rows

  So the value component IS applied, it filters correctly, and it wants the
  STORED value — which is exactly what quads carry here. `[_ p o]` is one
  `:avet` range read now instead of a predicate scan filtered afterwards.

  ## Two contracts that look alike and are not

  **Quads carry STORED VALUES, not `v_edn`.** A row reports the object as
  `v_edn` — its EDN encoding — and this decodes one level, so `:o` is the
  value the write path actually stored. The write path stringifies, so
  `:sp/age 30` is the string `\"30\"` here, not the number.

  That is the representation a consumer can work in: a FILTER comparing
  `?v > 25` can parse `\"30\"`, and cannot do anything sensible with the
  seven characters of `v_edn`. Pattern components are in the same
  representation — a caller holding a logical value passes `(str v)`, not
  `(pr-str v)`.

  **`visible?` here is the ROW-shaped predicate**, over
  `{:e :a :v_edn :added}` — that is what `hot-datoms` forwards to both its
  snapshot and novelty halves. It is NOT `datom.source`'s `{:s :p :o}`
  predicate, despite the identical name and the identical purpose. A
  `{:s :p :o}` predicate handed to `source-for` silently matches nothing and
  therefore hides nothing."
  (:require #?(:clj [clojure.edn :as edn] :cljs [cljs.reader :as edn])
            [datom.source :as src]
            [kotobase-peer.core :as eng]
            [kotobase.datom-plan :as plan]))

(defn- then*
  "The platform split, same as `kotobase.server.handler`'s: kotobase-peer's
  crypto-touching fns are values on JVM and `js/Promise`s on cljs."
  [x f]
  #?(:clj (f x)
     :cljs (.then (js/Promise.resolve x) f)))

(defn- all*
  "Await a seq of per-read results into a seq of results."
  [xs]
  #?(:clj (vec xs)
     :cljs (.then (js/Promise.all (clj->js (vec xs))) #(vec (array-seq %)))))

(defn read-quads
  "One planned read -> its asserted quads.

  `visible?` is threaded into `hot-datoms` itself rather than applied after,
  so a row the viewer may not see is never materialized into a quad — the
  same place `do-datoms` applies it (ADR-2607050500, ADR-2607174500 3b)."
  [store chain {:keys [index components]} visible?]
  (then* (eng/hot-datoms (:get-fn store) chain
                         {:index (keyword index) :components (vec components)}
                         visible? (:blind-fn store) (:decrypt-fn store)
                         (:async-get-fn store))
         (fn [rows]
           (into #{}
                 (map (fn [q] (update q :o #(when (string? %) (edn/read-string %)))))
                 (plan/rows->quads identity rows)))))

(defn source-for
  "Prefetch `patterns` from `chain` -> an `IPatternSource` over their union.
  A value on JVM, a Promise on cljs.

  Patterns planning to the same read are issued once: two patterns of one
  query routinely differ only in a position that index's prefix cannot bind,
  and issuing it twice is two reads for one answer.

  A nil `chain` (a graph with nothing written yet) is an empty source, not an
  error — the same posture `do-datoms` takes."
  ([store chain patterns] (source-for store chain patterns (constantly true)))
  ([store chain patterns visible?]
   (if (nil? chain)
     (then* nil (fn [_] (src/of-quads [])))
     (then* (all* (map #(read-quads store chain % visible?) (plan/reads patterns)))
            (fn [quad-sets] (src/of-quads (plan/union-quads quad-sets)))))))
