(ns kotobase.server.handler
  "Pure XRPC dispatch for a kotobase-peer-backed datom-plane worker --
  platform- and storage-independent by construction. Everything here is a
  pure function of an injected `store` and the parsed request body, so it's
  node-testable with an in-memory store and portable to ANY storage backend
  (Cloudflare R2, Backblaze B2, IndexedDB, a plain filesystem, ...) via a
  thin adapter the caller supplies -- no Cloudflare-specific code lives
  here, and no crypto POLICY is hardwired: kotobase-peer's own
  blind-fn/encrypt-fn/decrypt-fn seam (ADR-2607051000, no silent default)
  is threaded through as part of `store` too, so a caller can choose a
  plaintext-passthrough profile (the pre-encryption interim posture) or a
  real AEAD profile without forking this namespace.

  store keys:
    :get-fn      (fn [cid])          -> block bytes | nil (sync)
    :put!        (fn [cid bytes])    -> _ (writes block, sync)
    :head-get    (fn [graph])        -> chain-cid string | nil (sync)
    :head-put!   (fn [graph chain])  -> _ (sync; a caller doing CAS-with-
                                          retry against real storage, e.g.
                                          R2's onlyIf.etagMatches, does that
                                          OUTSIDE this namespace -- see
                                          `do-transact`'s docstring)
    :blind-fn    (fn [component])    -> blinded string | Promise<string>
    :encrypt-fn  (fn [bytes])        -> ciphertext | Promise<ciphertext>
    :decrypt-fn  (fn [bytes])        -> plaintext | Promise<plaintext>

  Extracted 2026-07-07 from `kotoba-lang/kotobase-cljc-worker`'s
  `handler.cljc` (the live implementation behind `kotobase.aozora.app` as of
  this writing) -- same dispatch/error semantics, generalized so
  `net-kotobase`'s own Cloudflare deployment (and any future consumer) can
  share ONE implementation instead of re-deriving it. The only behavioral
  change from the original: crypto fns move from a hardwired `require` into
  `store`, matching how storage was already injected."
  (:require #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [kotobase-peer.core :as eng]
            [kotobase-peer.policy :as policy]))

(def datomic-ns "ai.gftd.apps.kotobase.datomic")

(defn- then*
  "Thread `x` through `f` across the engine's platform split (ADR-2607051000:
  kotobase-peer's crypto-touching fns are synchronous values on JVM but
  `js/Promise`s on cljs). On cljs every handler response is therefore a
  Promise -- a caller's HTTP shell resolves it same as any other Promise;
  `js/Promise.resolve` flattens the non-promise case."
  [x f]
  #?(:clj (f x)
     :cljs (.then (js/Promise.resolve x) f)))

;; ── tx_edn (vector of entity maps) → engine quads ────────────────────────────

(defn tx-edn->quads
  "Parse a kotobase `tx_edn` string -- a vector of entity maps
  `[{:db/id \"e\" :ns/attr v …} …]` OR standard Datomic list-form assertions
  `[:db/add e a v]` -- into `{:s :p :o}` quads. Datafication of the map form
  goes through the engine's canonical datom model (`eng/entities->datoms` =
  `datom.core/eavt`), the SAME `[e a v]` representation kgraph (kotoba's
  in-mem view) speaks -- ONE shared datom model across transport, DB, and
  language (ADR-2607032500). List-form `:db/add` already IS an `[e a v]`
  triple, so it's stringified the same way `(str e)`/`(str a)`/`(str v)`
  without a round-trip through `entities->datoms`. Reads the whole string as
  EDN (no brace-splitting) so map/vector values with literal braces are
  safe. `(str :ns/attr)` keeps the leading ':' -- PDS datom consumers key on
  \":ns/attr\".

  Retraction forms ride the same tx_edn vector: `[:db/retract e a v]` -> an
  :op :retract quad, `[:db/retractEntity e]` -> an :op :retract-entity quad
  -- dispatched BEFORE eavt (which is the pure entity-map->[e a v] datafier
  and stays map-only).

  `:db/cas` (compare-and-swap) is deliberately NOT supported here -- CAS
  needs an atomicity guarantee this engine's append-only novelty-block write
  (`do-transact`, O(|tx|), no read-check) does not provide; a caller relying
  on `:db/cas` (e.g. cloud-murakumo's worker job-claim race) gets the same
  `unrecognized tx_edn item` rejection :db/add used to get, not silent
  non-atomic best-effort semantics -- confirmed 2026-07-12 while diagnosing
  every cloud-murakumo.queue-kotoba write returning a generic 400
  \"InternalError\" (this fn's own `:else` throw, swallowed by `handle`'s
  outer try/catch): queue-kotoba's job->tx/event->tx build plain `:db/add`
  list-form tuples, which this fn rejected outright before this fix -- ANY
  transact from that client failed, not just CAS-shaped ones."
  [tx-edn]
  (mapcat (fn [item]
            (cond
              (and (vector? item) (= 4 (count item)) (= :db/retract (first item)))
              (let [[_ e a v] item]
                [{:s (str e) :p (str a) :o (str v) :op :retract}])

              (and (vector? item) (= 2 (count item)) (= :db/retractEntity (first item)))
              [{:s (str (second item)) :op :retract-entity}]

              (and (vector? item) (= 4 (count item)) (= :db/add (first item)))
              (let [[_ e a v] item]
                [{:s (str e) :p (str a) :o (str v)}])

              (map? item)
              (map (fn [[e a v]] {:s (str e) :p (str a) :o (str v)})
                   (eng/entities->datoms [item]))

              :else
              (throw (ex-info "kotobase: unrecognized tx_edn item" {:item item}))))
          (edn/read-string tx-edn)))

;; ── read/write against the graph's persisted chain (ADR-2607032430 D1) ──────
;; The chain's head IS the unit of state now (snapshot + pending novelty), not
;; just its folded snapshot -- every read goes through `eng/hot-datoms` so it
;; never misses data written since the last fold. `do-transact` never
;; hydrates the graph: it appends one novelty tx block (O(|tx|), independent
;; of graph size). Folding is a separate, explicit operation (`do-fold`) a
;; cron/ops caller invokes -- never inline in a write's own request, so no
;; single write's latency/CPU budget can include an O(graph) compaction.

(defn- index-kw [index]
  (when (seq index) (keyword (cond-> index (str/starts-with? index ":") (subs 1)))))

(defn- visible-of
  "The per-request row filter `handle` computed from (graph policy × viewer
  CACAO capabilities), ADR-2607174500 Phase 3b; absent (writes, tests
  calling do-* directly) → fully public, the pre-Phase-3 behavior."
  [store]
  (or (:visible? store) (constantly true)))

(defn- visible-attr? [store attr]
  ((visible-of store) {:a (str attr)}))

(defn- redact-pulled
  "Attr-level redaction over eng/pull / eng/entity result trees — those
  read a hydrated db VALUE and bypass the row-level visible? seam, so the
  handler filters their attr-keyed maps (recursively: nested pull
  patterns return nested attr maps)."
  [store v]
  (cond
    (map? v) (into {} (keep (fn [[k vv]]
                              (when (visible-attr? store k)
                                [k (redact-pulled store vv)]))
                            v))
    (set? v) (into #{} (map #(redact-pulled store %)) v)
    (sequential? v) (mapv #(redact-pulled store %) v)
    :else v))

(defn- request-visible?
  "policy entity の 1 narrow read × viewer caps → visible? (ADR-2607174500)。
  nil chain → public. Promise on cljs."
  [store caps]
  (fn [graph]
    (let [chain ((:head-get store) graph)]
      (if (nil? chain)
        #?(:clj (constantly true) :cljs (js/Promise.resolve (constantly true)))
        (then* (eng/hot-datoms (:get-fn store) chain
                               {:index :eavt :components [policy/policy-entity]}
                               (constantly true) (:blind-fn store) (:decrypt-fn store))
               (fn [rows] (policy/visible-for (policy/policy-of rows) caps)))))))

(defn do-datoms
  "`datomic.datoms` -- filtered read via hot-datoms (snapshot + novelty
  merge, range-pruned on the snapshot side; never a whole-graph rehydrate).
  body: {:graph :index :components_edn :limit}.

  `eng/hot-datoms`'s `visible?` is required (ADR-2607050500, same reasoning
  as `do-q`'s `eng/q` call below) -- this handler has no capability/purpose-
  scoped redaction wired in yet, so it passes `(constantly true)`
  explicitly: today's behavior is unchanged, stated instead of assumed."
  [store {:keys [graph index components_edn limit]}]
  (let [chain ((:head-get store) graph)]
    (then* (eng/hot-datoms (:get-fn store) chain
                           {:index (or (index-kw index) :eavt)
                            :components (vec components_edn)
                            :limit limit}
                           (visible-of store) (:blind-fn store) (:decrypt-fn store))
           (fn [rows] {:ok true :graph graph :datoms (vec rows)}))))

(defn do-transact
  "`datomic.transact` -- append the tx quads as ONE novelty block and
  advance the chain. O(|tx_edn|) -- independent of graph size; never
  hydrates or rebuilds an index (ADR-2607032430 D1). `auth-did` is the
  CACAO-verified issuer (the caller verifies it == graph owner, before
  calling this); nil here means the caller already gated it. `novelty_size`
  in the response is an observability signal for when a `fold` (see
  `do-fold`) is worth invoking -- this handler never folds itself.

  `((:head-put! store) graph chain)` here is NOT the durable, concurrency-
  safe head write -- it's a hook the caller uses however it needs
  (typically: accumulate into a buffer, then CAS the durable head pointer
  AFTER this fn returns, retrying the WHOLE call on a lost race). See
  `kotoba-lang/kotobase-cljc-worker`'s `worker.cljc` (`run-write-attempt`/
  `run-write`) for the reference orchestration this was extracted from."
  [store {:keys [graph tx_edn]} _auth-did]
  (let [get-fn (:get-fn store)
        prev-chain ((:head-get store) graph)
        quads (tx-edn->quads tx_edn)]
    (then* (eng/commit! (:put! store) get-fn quads prev-chain (:encrypt-fn store))
           (fn [chain]
             ((:head-put! store) graph chain)
             {:ok true :graph graph :commit chain
              :datom_count (count quads)
              :novelty_size (eng/novelty-size get-fn chain)}))))

(defn- hot-db
  "The full hot db as of `chain` (snapshot + novelty merged) -- for `do-q`/
  `do-pull`/`do-entity`/`do-entid`/`do-ident`/`do-pull-many`, which need an
  actual db value (to route a multi-attribute pattern through
  arrangement.query/datalog, or to navigate ref-valued attrs via `pull`/
  `entity`). Delegates directly to kotobase-peer's own `hydrate-chain` --
  this fn used to hand-roll the equivalent via hot-datoms + a fresh
  `transact` (a workaround from before kotobase-peer had this primitive
  itself, see git history); `hydrate-chain` is kotobase-peer's own, more
  direct implementation of exactly this, now used instead."
  [store chain]
  (eng/hydrate-chain (:get-fn store) chain (:blind-fn store) (:decrypt-fn store)))

;; ── query-literal normalization (wire symmetry with tx-edn->quads) ──────────
;; The write path stringifies EVERY non-Link datom position before it reaches
;; the engine (`tx-edn->quads` above: `(str a)` / `(str v)` -- a transacted
;; `:gh.genko/title` is STORED as the string ":gh.genko/title", `:status
;; :draft` as ":draft", `:score 10` as "10"). The read path previously passed
;; `query_edn`'s parsed literals through to arrangement UNCHANGED, where
;; matching is plain `=` -- so a natural Datomic-style query written with
;; keyword attributes (`[?e :gh.genko/title ?t]`) compared a KEYWORD against
;; the stored STRING, matched nothing, and returned `{:ok true :rows []}`:
;; success-shaped silence for correctly-written queries over data `datoms`
;; plainly showed (confirmed live against backend.kotobase.net, 2026-07-09;
;; the smoke tests never caught it because they all pre-stringify attrs,
;; `":yoro.post/text"`). These fns close that asymmetry at the SAME layer
;; that created it: every query literal in a datom position is coerced with
;; the write side's own `(str x)` before it reaches the engine.
;;
;; What is deliberately NOT coerced:
;;   - logic vars (`?x`) / the wildcard `_` / nil -- pattern syntax, not data
;;   - strings -- already the stored representation ((str s) = s anyway)
;;   - collections -- `["ipld/link" cid]` (a Link's wire form) and any other
;;     structured literal keep today's behavior; Link-valued query matching
;;     is a separate, pre-existing gap this change doesn't paper over
;;   - predicate/function clause args (`[(> ?n 18)]`) -- those evaluate over
;;     already-bound values, they are not matched against stored datoms

(defn- lvar? [x] (and (symbol? x) (str/starts-with? (name x) "?")))

(defn- wire-literal
  "One query literal -> the engine's stored representation (the write
  side's own `(str x)` coercion). Pattern syntax (lvars/`_`/nil), strings,
  and collections pass through unchanged -- see the section comment."
  [x]
  (if (or (nil? x) (string? x) (coll? x) (lvar? x) (= '_ x))
    x
    (str x)))

(defn- normalize-clause
  "Normalize the datom-position literals of ONE `:where`/rule-body clause,
  preserving `arrangement.datalog`'s clause taxonomy exactly: triple
  patterns, `(not [e a v])`, `(or ...)`/`(or-join [vars] ...)` branches,
  and rule-invocation args are coerced; predicate/function clauses
  (`[(fn-sym arg...)]`, distinguished the same way arrangement does -- a
  vector whose first element is a seq) are left untouched."
  [clause]
  (cond
    (and (vector? clause) (seq? (first clause))) clause
    (vector? clause) (mapv wire-literal clause)
    (seq? clause)
    (let [[head & more] clause]
      (cond
        (= 'not head)     (list 'not (normalize-clause (first more)))
        (= 'or head)      (cons 'or (map normalize-clause more))
        (= 'or-join head) (list* 'or-join (first more) (map normalize-clause (rest more)))
        :else             (cons head (map wire-literal more))))
    :else clause))

(defn- normalize-rule
  "`[(rule-name ?param ...) body-clause ...]` -- the head (params are
  lvars by construction) stays; body clauses normalize like `:where`'s."
  [rule]
  (into [(first rule)] (map normalize-clause (rest rule))))

(defn normalize-query-literals
  "Coerce every datom-position literal of a parsed `query_edn` (either
  shape: `[s p o]` triple pattern or `{:find ... :where ... :rules ...}`
  Datalog map) to the stored string representation -- see the section
  comment above. Public + pure so the symmetry with `tx-edn->quads` is
  directly unit-testable."
  [pat]
  (if (map? pat)
    (cond-> pat
      (:where pat) (update :where #(mapv normalize-clause %))
      (:rules pat) (update :rules #(mapv normalize-rule %)))
    (mapv wire-literal pat)))

(defn do-q
  "`datomic.q` -- routes to ONE of two engines depending on `query_edn`'s
  shape: a plain `[s p o]` vector (nil = wildcard) is a triple-pattern
  query (`eng/q`, arrangement.query -- no joins, returns a set of `{:s :p
  :o}` quads); a map with `:find`/`:where` keys is a full conjunctive
  Datalog query (`eng/query`, arrangement.datalog -- multi-clause joins,
  negation, aggregates, recursive rules; returns a set of `:find`-ordered
  VECTORS, a different row shape from the triple-pattern branch -- the
  caller already knows which it sent and therefore which shape comes
  back). Previously this ALWAYS called `eng/q`, silently mistreating any
  map-shaped query as a malformed/empty triple pattern instead of routing
  it to the engine that actually understands it -- a real, if latent,
  bug (see git history): `net-kotobase`'s `geo.search`/`web.search`
  (domain_search.cljc's own \"candidate query\") send exactly this map
  shape today. body: {:graph :query_edn :inputs_edn} -- `inputs_edn`
  (optional) is an EDN vector matching `query_edn`'s own `:in` clause
  order, for the Datalog form only (see `eng/query`'s docstring); ignored
  for a triple-pattern query.

  Both query shapes (and `inputs_edn`'s scalar values) pass through
  `normalize-query-literals` first, so a query written the natural
  Datomic way (keyword attributes, keyword/number value literals) matches
  the stringified representation the write path actually stored -- see
  the normalization section comment above for the live bug this fixes."
  [store {:keys [graph query_edn inputs_edn]}]
  (let [chain ((:head-get store) graph)
        pat (normalize-query-literals (edn/read-string query_edn))]
    (then* (hot-db store chain)
           (fn [db]
             (let [rows (if (and (map? pat) (or (contains? pat :find) (contains? pat :where)))
                          (if inputs_edn
                            (eng/query db pat (visible-of store)
                                       (mapv wire-literal (edn/read-string inputs_edn)))
                            (eng/query db pat (visible-of store)))
                          (eng/q db pat (visible-of store)))]
               {:ok true :graph graph :rows (vec rows)})))))

(defn do-pull
  "`datomic.pull` -- all attrs of one entity via hot-datoms (snapshot +
  novelty merge), UNLESS the caller supplies `:pattern_edn` (a Datomic
  pull-pattern EDN string -- see kotobase-peer's `pull` docstring for the
  grammar: plain attrs, `'*` wildcard, `\"_attr\"` reverse nav, `{attr
  [sub-pattern]}` nested pull, `{attr n}`/`{attr '...}` recursion), in
  which case this now actually honors it (previously silently ignored --
  kotobase-client sends the field, the handler dropped it on the floor;
  see git history) by materializing a db (`hot-db`) and running
  kotobase-peer's real pattern-aware `pull`.

  The two response shapes are DIFFERENT and not interchangeable: the
  no-pattern legacy path returns `:attrs {a [v_edn...]}` (per-attribute
  wire-encoded value strings, hot-datoms rows regrouped -- unchanged, for
  every existing caller that doesn't send `pattern_edn`); the pattern
  path returns `:result_edn` (the WHOLE pull-pattern result tree, pr-str'd
  once -- a pattern can nest/wildcard/reverse-nav arbitrarily, so there is
  no single flat per-leaf v_edn encoding that composes across that the
  way there is for a flat attr map). body: {:graph :entity :pattern_edn}."
  [store {:keys [graph entity pattern_edn]}]
  (if pattern_edn
    (let [chain ((:head-get store) graph)
          pattern (edn/read-string pattern_edn)]
      (then* (hot-db store chain)
             (fn [db] {:ok true :graph graph :entity entity
                       :result_edn (pr-str (redact-pulled store (eng/pull db entity pattern)))})))
    (let [chain ((:head-get store) graph)]
      (then* (eng/hot-datoms (:get-fn store) chain {:index :eavt :components [entity]}
                             (visible-of store) (:blind-fn store) (:decrypt-fn store))
             (fn [rows]
               {:ok true :graph graph :entity entity
                :attrs (reduce (fn [m {:keys [a v_edn]}] (update m a (fnil conj []) v_edn)) {} rows)})))))

(defn do-pull-many
  "`datomic.pullMany` -- the same pull pattern applied to a LIST of
  entities. body: {:graph :entities :pattern_edn} -- `:entities` a vector
  of entity ids, `:pattern_edn` required (unlike `do-pull`, pullMany has
  no flat-attrs legacy shape to fall back to)."
  [store {:keys [graph entities pattern_edn]}]
  (let [chain ((:head-get store) graph)
        pattern (edn/read-string pattern_edn)]
    (then* (hot-db store chain)
           (fn [db]
             {:ok true :graph graph
              :results_edn (mapv #(pr-str (redact-pulled store (eng/pull db % pattern))) entities)}))))

(defn do-index-pull
  "`datomic.indexPull` -- Datomic's index-based pull, a comparatively
  obscure/rarely-used corner of the real Datomic API. No distinct mapping
  onto this substrate's primitives exists beyond what `datomic.pullMany`
  already does (pull the same pattern over a caller-given list of
  entities) -- implemented as a direct alias rather than inventing a
  different, unproven contract for a method with no pinned wire shape and
  no clear semantic gap `pullMany` doesn't already cover."
  [store body]
  (do-pull-many store body))

(defn do-entity
  "`datomic.entity` -- `pull`'s flat 2-arg form (`{p #{o...}}`) under
  Datomic's own name. Unlike `do-pull`'s no-pattern path (hot-datoms
  based, per-attribute `v_edn` wire strings), this needs a fully
  materialized db (kotobase-peer's `entity` is the pull-flat-form
  entry point a caller can then `entity-attr`-navigate further, though
  that on-demand navigation itself isn't wired to a separate NSID here --
  see `datomic.pull`'s `{attr [...]}` nested-pull pattern for the
  equivalent eager form). Returns the WHOLE entity map pr-str'd
  (`:entity_edn`), same reasoning as `do-pull`'s pattern path: raw
  decoded values, not per-attribute wire strings. body: {:graph :entity}."
  [store {:keys [graph entity]}]
  (let [chain ((:head-get store) graph)]
    (then* (hot-db store chain)
           (fn [db] {:ok true :graph graph :entity entity
                     :entity_edn (pr-str (redact-pulled store (eng/entity db entity)))}))))

(defn do-entid
  "`datomic.entid` -- resolve an id-or-ident to the actual entity id
  (kotobase-peer's `entid`: a plain string id passes through unchanged;
  a keyword id resolves via a `:db/ident` attribute lookup -- see its
  docstring for why this substrate needs no numeric-id resolution the
  way real Datomic's `entid` does). body: {:graph :ident_edn} --
  `:ident_edn` is an EDN string (so the caller can send either a plain
  string id or a keyword ident through the same field, unambiguously)."
  [store {:keys [graph ident_edn]}]
  (let [chain ((:head-get store) graph)]
    (then* (hot-db store chain)
           (fn [db] {:ok true :graph graph
                     :entity_id (eng/entid db (edn/read-string ident_edn))}))))

(defn do-ident
  "`datomic.ident` -- the inverse of `entid`: the `:db/ident` keyword an
  entity has asserted on itself, or nil. body: {:graph :entity}.
  `:ident_edn` in the response is pr-str'd (nil pr-str's to the string
  \"nil\", NOT omitted -- a caller distinguishes \"no ident\" from a
  malformed response by the field always being present)."
  [store {:keys [graph entity]}]
  (let [chain ((:head-get store) graph)]
    (then* (hot-db store chain)
           (fn [db] {:ok true :graph graph :entity entity
                     :ident_edn (pr-str (eng/ident db entity))}))))

(defn do-as-of
  "`datomic.asOf` -- a `datoms`-shaped read of the graph AS OF commit `t`
  (Datomic's own term; kotobase-peer's chain calls the same value `seq`).
  body: {:graph :t :index :components_edn :limit}, same read shape as
  `do-datoms` beyond `:t`. A `t` beyond the chain's own tip clamps to the
  tip (kotobase-peer's `as-of`'s own \"as-of the future just means now\"
  behavior, matching Datomic); a nil/absent chain (fresh graph) resolves
  to `{:datoms []}`, same as `do-datoms` on a fresh graph."
  [store {:keys [graph t index components_edn limit]}]
  (let [chain ((:head-get store) graph)
        as-of-chain (eng/as-of (:get-fn store) chain t)]
    (then* (eng/hot-datoms (:get-fn store) as-of-chain
                           {:index (or (index-kw index) :eavt)
                            :components (vec components_edn)
                            :limit limit}
                           (visible-of store) (:blind-fn store) (:decrypt-fn store))
           (fn [rows] {:ok true :graph graph :t t :datoms (vec rows)}))))

(defn do-since
  "`datomic.since` -- quads from commits with `:seq` greater than `t`
  (Datomic's `since`; a DIFF view, NOT merged with prior state -- see
  `datomic.asOf`/plain `datomic.datoms` for the merged view). body:
  {:graph :t}."
  [store {:keys [graph t]}]
  (let [chain ((:head-get store) graph)]
    (then* (eng/since (:get-fn store) chain t (:decrypt-fn store))
           (fn [db] {:ok true :graph graph :t t
                     :datoms (vec (eng/datoms db (visible-of store)))}))))

(defn do-history
  "`datomic.history` -- the full audit-log view: every quad EVER asserted
  across the whole chain, including later-retracted facts, as
  `{:e :a :v_edn :added}` rows distinguishing assert from retract
  (Datomic's `(d/datoms (d/history db) ...)`, here via kotobase-peer's
  `history-datoms`). body: {:graph :entity} -- `:entity` (optional)
  narrows to one entity's history (the single most common ask, \"what
  happened to THIS entity\"); omitting it is O(all history) -- see
  kotobase-peer's own `history-datoms` docstring, which also documents an
  honest limitation around `snapshot!`-seeded (migration-imported)
  content with no individual assert event."
  [store {:keys [graph entity]}]
  (let [chain ((:head-get store) graph)]
    (then* (eng/history-datoms (:get-fn store) chain entity (visible-of store) (:decrypt-fn store))
           (fn [rows] {:ok true :graph graph :datoms (vec rows)}))))

(defn do-basis-t
  "`datomic.basisT` -- the current basis (chain tip `:seq`), a monotonic
  value comparable across reads of the same graph (Datomic's own basisT
  contract: a value you can compare, not a wall-clock time -- matches
  kotobase-peer's own design note on why commits are keyed by `:seq`, not
  a timestamp). body: {:graph}. A fresh, never-written graph reports
  `:t nil` (no basis yet), not an error -- same posture `do-datoms`
  already takes for `{:datoms []}` on a nil chain. Pure/sync -- `chain`
  touches no crypto, so this needs no `then*` wrapping on either
  platform."
  [store {:keys [graph]}]
  (let [get-fn (:get-fn store)
        chain ((:head-get store) graph)
        entries (when chain (eng/chain get-fn chain))]
    {:ok true :graph graph :t (:seq (last entries))}))

(defn do-db-stats
  "`datomic.dbStats` -- cheap graph-shape metadata without a full read:
  current basis (`:t`, same value `do-basis-t` reports), whether anything
  has been folded yet (`:has_snapshot`), and how many not-yet-folded tx
  blocks sit on top (`:novelty_size`, the same `fold`-worthiness signal
  `do-transact`'s own response already surfaces per-write). body:
  {:graph}. Pure/sync, same reasoning as `do-basis-t`."
  [store {:keys [graph]}]
  (let [get-fn (:get-fn store)
        chain ((:head-get store) graph)
        entries (when chain (eng/chain get-fn chain))]
    {:ok true :graph graph
     :t (:seq (last entries))
     :has_snapshot (boolean (and chain (eng/latest-snapshot-cid get-fn chain)))
     :novelty_size (if chain (eng/novelty-size get-fn chain) 0)}))

(defn do-seek-datoms
  "`datomic.seekDatoms` -- Datomic's index-scan-from-a-position read.
  HONEST APPROXIMATION: this substrate's `datoms`/`hot-datoms` already do
  an index scan filtered by a `:components` PREFIX match (not a true
  ordered \"seek to >= these components and continue\" the way Datomic's
  own seekDatoms works) -- for the common case (seek to an exact entity/
  attribute prefix and read forward) this is equivalent; it is NOT
  equivalent for a genuine \">= partial value\" ordered seek. A real
  ordered seek would need a new kotobase-peer primitive, not implemented
  here -- this is a thin alias of `do-datoms`, not a distinct
  implementation. body: identical to `do-datoms`."
  [store body]
  (do-datoms store body))

(defn do-index-range
  "`datomic.indexRange` -- every datom for one attribute whose value
  falls in `[start, end]` (inclusive; either bound optional), Datomic's
  AVET range scan. HONEST APPROXIMATION, two layers of it:

  1. Implemented as a full AVET scan for the attribute (`hot-datoms` with
     `:avet` + `:components [attr]`) followed by an in-process value-range
     filter -- correct, but O(all values for that attribute), not a true
     ordered range-seek (no ordered-range primitive exists in
     kotobase-peer for this yet).
  2. MORE IMPORTANTLY: this substrate stringifies every non-Link value at
     write time (kotobase-server's own `tx-edn->quads`, `(str v)`, before
     the quad ever reaches kotobase-peer) -- a transacted `:score 10`
     round-trips as the STRING `\"10\"`, never the number `10`. So
     `start_edn`/`end_edn`, compared against each row's own
     `edn/read-string`'d `v_edn` via `compare`, effectively do
     LEXICOGRAPHIC string comparison for anything that started life as a
     number (`\"9\" > \"10\"` lexicographically -- NOT the numeric range a
     caller sending numeric-looking bounds would expect). Passing
     type-mismatched bounds (e.g. a bare number against the stored
     string) throws outright (surfaced as `InternalError` by `handle`,
     not a silent empty/wrong result) -- callers MUST pass `start_edn`/
     `end_edn` shaped like what's actually stored (a pr-str'd STRING,
     e.g. `\"\\\"15\\\"\"`, not a bare `\"15\"`) to avoid the throw, and
     must additionally account for lexicographic-not-numeric ordering
     themselves (e.g. zero-padding) if the range needs to be numerically
     correct. body: {:graph :attr :start_edn :end_edn :limit}."
  [store {:keys [graph attr start_edn end_edn limit]}]
  (let [chain ((:head-get store) graph)
        start (some-> start_edn edn/read-string)
        end (some-> end_edn edn/read-string)]
    (then* (eng/hot-datoms (:get-fn store) chain {:index :avet :components [attr]}
                           (visible-of store) (:blind-fn store) (:decrypt-fn store))
           (fn [rows]
             (let [in-range? (fn [{:keys [v_edn]}]
                                (let [v (edn/read-string v_edn)]
                                  (and (or (nil? start) (>= (compare v start) 0))
                                       (or (nil? end) (<= (compare v end) 0)))))
                   filtered (filter in-range? rows)
                   filtered (cond->> filtered limit (take limit))]
               {:ok true :graph graph :attr attr :datoms (vec filtered)})))))

(defn do-tx
  "`datomic.tx` -- one commit's metadata (Datomic's own tx entity is
  mostly opaque anyway beyond `:db/txInstant`, which this substrate
  doesn't track by design -- see kotobase-peer's \"time-travel: as-of\"
  section comment on why: no wall-clock field on the commit envelope, so
  a chain stays stable across a future timestamp addition). body:
  {:graph :t}. Does NOT include per-tx datom content -- kotobase-peer has
  no public primitive to isolate one commit's own novelty out of the
  whole-chain replay `history-datoms` does; use `datomic.since` with
  `t-1` for that instead (a diff view across everything after a point,
  equivalent to \"what that one commit added\" for an exact single `t`).
  Pure/sync, same reasoning as `do-basis-t`."
  [store {:keys [graph t]}]
  (let [chain-cid ((:head-get store) graph)
        entries (when chain-cid (eng/chain (:get-fn store) chain-cid))
        entry (some #(when (= t (:seq %)) %) entries)]
    {:ok true :graph graph :t t :found (some? entry) :tx_cid (some-> entry :cid str)}))

(defn- assert-integer-bound!
  "Throws if `v` is present but not an integer. `>=`/`<`/`<=`/`>` compile
  to native JS relational operators in cljs (unlike JVM Clojure's numeric
  comparators, which throw a ClassCastException on a non-Number), and
  JS's loose coercion makes e.g. `(>= 5 \"abc\")` silently `false` --
  never a throw. Without this guard, a malformed :start/:end/:t here
  would silently read as \"nothing in range\" / \"not caught up\" instead
  of a clean rejection -- the same failure shape as the already-fixed
  do-q wire-literal bug, just on a different endpoint. `handle`'s
  existing try/catch turns this throw into a normal `{:ok false :error
  \"InternalError\"}` response, same as do-index-range's own `edn/read-
  string` throwing on a malformed start_edn/end_edn."
  [k v]
  (when (and (some? v) (not (integer? v)))
    (throw (ex-info (str (name k) " must be an integer, got " (pr-str v)) {k v}))))

(defn do-tx-range
  "`datomic.txRange` -- commit metadata for every `:seq` in `[start,
  end)` (Datomic's own txRange semantics: start inclusive, end exclusive;
  either bound omitted means unbounded on that side). body: {:graph
  :start :end :limit}. Same per-tx-datom-content limitation as
  `datomic.tx` above. Pure/sync, same reasoning as `do-basis-t`."
  [store {:keys [graph start end limit]}]
  (assert-integer-bound! :start start)
  (assert-integer-bound! :end end)
  (let [chain-cid ((:head-get store) graph)
        entries (when chain-cid (eng/chain (:get-fn store) chain-cid))
        in-range? (fn [{:keys [seq]}]
                    (and (or (nil? start) (>= seq start))
                         (or (nil? end) (< seq end))))
        filtered (filter in-range? entries)
        filtered (cond->> filtered limit (take limit))]
    {:ok true :graph graph
     :txs (mapv (fn [{:keys [seq cid]}] {:t seq :tx_cid (str cid)}) filtered)}))

(defn do-log
  "`datomic.log` -- the full transaction log (or a `[start, end)` slice
  if given) -- same shape as `datomic.txRange`, since Datomic's own
  `log`/`txRange` overlap in practice (a bounded log IS a tx-range); not
  a distinct implementation here either."
  [store body]
  (do-tx-range store body))

(defn do-sync
  "`datomic.sync` -- HONEST simplification: this substrate has no
  distributed replication lag to wait out (the chain-cid IS the current
  state the instant a write succeeds; there is no separate \"catching
  up\" process an HTTP request/response cycle could meaningfully block
  on). Reports whether the graph's current basis has already reached the
  requested `:t`, IMMEDIATELY, rather than either (a) blocking (not
  possible in a single request/response cycle) or (b) silently pretending
  to have waited. A caller that actually needs \"block until this write
  is visible\" should retry `datomic.sync` (or just re-read) rather than
  treat this as a real async wait primitive. body: {:graph :t}. Pure/
  sync, same reasoning as `do-basis-t`."
  [store {:keys [graph t]}]
  (assert-integer-bound! :t t)
  (let [chain-cid ((:head-get store) graph)
        entries (when chain-cid (eng/chain (:get-fn store) chain-cid))
        current-t (:seq (last entries))]
    {:ok true :graph graph :t t :current_t current-t
     :caught_up (boolean (and current-t (>= current-t t)))}))

(defn do-fold
  "`datomic.fold` -- compacts a graph's accumulated novelty into a fresh
  indexed snapshot (ADR-2607032430 D1 `fold!`). Not part of the datomic
  surface proper -- a maintenance operation a cron/ops caller invokes to
  keep `hot-datoms`/`do-q` reads fast as novelty grows. `:commit` is absent
  (no head write attempted) when there's nothing to fold, so a redundant/
  no-op call is cheap and doesn't perturb the head. Safe to call anytime,
  including concurrently with a transact or with another fold of the same
  graph -- `fold!` is deterministic/content-addressed, so races converge
  rather than corrupt (the CAS layer in the caller's shell resolves any
  actual head contention)."
  [store {:keys [graph]}]
  (let [get-fn (:get-fn store)
        chain ((:head-get store) graph)
        novelty-n (if chain (eng/novelty-size get-fn chain) 0)]
    (if (zero? novelty-n)
      {:ok true :graph graph :folded false}
      (then* (eng/fold! (:put! store) get-fn chain
                        (:blind-fn store) (:encrypt-fn store) (:decrypt-fn store))
             (fn [new-chain]
               ((:head-put! store) graph new-chain)
               {:ok true :graph graph :folded true :commit new-chain :novelty_folded novelty-n})))))

;; ── blob surface ─────────────────────────────────────────────────────────────
;; git-annex / DataLad の content-addressed 大容量バイナリ永続化面（ADR-2607175000）。
;; datom 面（JSON body、`handle`）とは分離し、**raw bytes** を扱う。store の
;; put!/get-fn/head をそのまま公開する薄いラッパ:
;;   blob-put/remove  は書き込みなので auth-did（CACAO 検証済み）を要求。
;;   blob-get/head    は読みで auth 不要。
;; block は content-addressed で **immutable**（delete が store 契約に無い）ため、
;; REMOVE は tombstone head で表現する（put! で再投入すると tombstone は解除）。
;; key は git-annex key（SHA256E-s<size>--<hash>）で、そのまま block cid に使う。

(defn- blob-tomb [key] (str "blob-tomb:" key))

(defn- blob-present?
  "key が present か（block があり、かつ tombstone が無い）。"
  [store key]
  (and (some? ((:get-fn store) key))
       (nil? ((:head-get store) (blob-tomb key)))))

(defn do-blob-put
  "raw bytes を key（=cid）で保存。tombstone があれば解除（再投入）。auth 必須。"
  [store key bytes _auth-did]
  ((:put! store) key bytes)
  ((:head-put! store) (blob-tomb key) nil)
  {:ok true :key key})

(defn do-blob-get
  "key の bytes を返す（present でなければ NotFound）。"
  [store key]
  (if (blob-present? store key)
    {:ok true :key key :data ((:get-fn store) key)}
    {:ok false :error "NotFound" :key key}))

(defn do-blob-head
  "key の present 判定（bytes を転送しない CHECKPRESENT 用）。"
  [store key]
  {:ok true :key key :present (blob-present? store key)})

(defn do-blob-remove
  "key を tombstone で不在化（immutable block は残すが present? は false に）。auth 必須。"
  [store key _auth-did]
  ((:head-put! store) (blob-tomb key) "removed")
  {:ok true :key key})

(defn handle-blob
  "blob 面の dispatch。`op` は put|get|head|remove、`bytes` は put 時のみ。
   auth-did は CACAO 検証済み issuer（put/remove は nil だと Unauthorized）。"
  [store op key bytes auth-did]
  (letfn [(need-auth [] (when-not auth-did {:ok false :error "Unauthorized" :key key}))]
    (case op
      "put"    (or (need-auth) (do-blob-put store key bytes auth-did))
      "get"    (do-blob-get store key)
      "head"   (do-blob-head store key)
      "remove" (or (need-auth) (do-blob-remove store key auth-did))
      {:ok false :error "MethodNotImplemented" :op op})))

;; ── dispatch ─────────────────────────────────────────────────────────────────

(defn handle
  "Dispatch a parsed XRPC call. `method` is the NSID suffix after
  `ai.gftd.apps.kotobase.datomic.`; `body` is the keywordized JSON body;
  `auth` is the CACAO-verified issuer DID (string, legacy) or an
  {:did :resources} map (ADR-2607174500 — resources carry the viewer's
  capability strings for read redaction). Returns a plain response map;
  never throws for a known method (errors become `{:ok false :error …}`)
  EXCEPT a storage-layer block-miss (`kotobase.server.trampoline`'s
  `:block-miss` marker), which re-throws so the caller's trampoline sees it
  and retries -- swallowing it here would surface as a spurious
  InternalError instead."
  [store method body auth]
  (letfn [(err [e]
            (if (:block-miss (ex-data e))
              (throw e)
              {:ok false :error "InternalError"
               :message #?(:clj (.getMessage ^Exception e)
                           :cljs (or (ex-message e) (.-message e)))}))
          (dispatch [store]
            (let [auth-did (if (map? auth) (:did auth) auth)]
              (case method
                "datoms"      (do-datoms store body)
                "transact"    (do-transact store body auth-did)
                "q"           (do-q store body)
                "pull"        (do-pull store body)
                "pullMany"    (do-pull-many store body)
                "indexPull"   (do-index-pull store body)
                "fold"        (do-fold store body)
                "entity"      (do-entity store body)
                "entid"       (do-entid store body)
                "ident"       (do-ident store body)
                "asOf"        (do-as-of store body)
                "since"       (do-since store body)
                "history"     (do-history store body)
                "basisT"      (do-basis-t store body)
                "dbStats"     (do-db-stats store body)
                "seekDatoms"  (do-seek-datoms store body)
                "indexRange"  (do-index-range store body)
                "tx"          (do-tx store body)
                "txRange"     (do-tx-range store body)
                "log"         (do-log store body)
                "sync"        (do-sync store body)
                {:ok false :error "MethodNotImplemented" :method method})))]
    (try
      ;; ADR-2607174500 Phase 3b: resolve (graph policy × viewer caps) into
      ;; the per-request visible? BEFORE dispatch for redaction-relevant
      ;; reads; each read op consumes it via (visible-of store). Writes and
      ;; metadata-only ops (tx/txRange/log/dbStats/basisT/sync/entid/ident —
      ;; commit metadata and id mappings, no datom content) skip the extra
      ;; narrow read entirely.
      (let [caps (when (map? auth) (:resources auth))
            visibility-methods #{"datoms" "q" "pull" "pullMany" "indexPull"
                                 "entity" "asOf" "since" "history"
                                 "seekDatoms" "indexRange"}
            resp (if (and (contains? visibility-methods method) (:graph body))
                   (then* ((request-visible? store caps) (:graph body))
                          (fn [visible?] (dispatch (assoc store :visible? visible?))))
                   (dispatch store))]
        #?(:clj resp
           :cljs (.catch (js/Promise.resolve resp) err)))
      (catch #?(:clj Exception :cljs :default) e
        ;; a do-* fn that throws SYNCHRONOUSLY lands here; Promise-wrap on
        ;; cljs so every caller's .then keeps working (same reasoning as
        ;; kotobase-cljc-worker's handle).
        #?(:clj (err e)
           :cljs (js/Promise.resolve (err e)))))))
