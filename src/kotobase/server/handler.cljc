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
            [kotobase-peer.core :as eng]))

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
  `[{:db/id \"e\" :ns/attr v …} …]` -- into `{:s :p :o}` quads. Datafication
  goes through the engine's canonical datom model (`eng/entities->datoms` =
  `datom.core/eavt`), the SAME `[e a v]` representation kgraph (kotoba's
  in-mem view) speaks -- ONE shared datom model across transport, DB, and
  language (ADR-2607032500). Reads the whole string as EDN (no
  brace-splitting) so map/vector values with literal braces are safe.
  `(str :ns/attr)` keeps the leading ':' -- PDS datom consumers key on
  \":ns/attr\".

  Retraction forms ride the same tx_edn vector: `[:db/retract e a v]` -> an
  :op :retract quad, `[:db/retractEntity e]` -> an :op :retract-entity quad
  -- dispatched BEFORE eavt (which is the pure entity-map->[e a v] datafier
  and stays map-only)."
  [tx-edn]
  (mapcat (fn [item]
            (cond
              (and (vector? item) (= 4 (count item)) (= :db/retract (first item)))
              (let [[_ e a v] item]
                [{:s (str e) :p (str a) :o (str v) :op :retract}])

              (and (vector? item) (= 2 (count item)) (= :db/retractEntity (first item)))
              [{:s (str (second item)) :op :retract-entity}]

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
                           (constantly true) (:blind-fn store) (:decrypt-fn store))
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
  "The full hot db as of `chain` (snapshot + novelty merged) -- for `do-q`,
  which needs an actual db value to route a multi-attribute pattern through
  arrangement.query. Composed entirely from kotobase-peer's public API
  (hot-datoms + transact), so it stays correct against novelty without
  kotobase-peer needing its own db-shaped 'hot-db' primitive."
  [store chain]
  (then* (eng/hot-datoms (:get-fn store) chain (constantly true)
                         (:blind-fn store) (:decrypt-fn store))
         (fn [rows]
           (eng/transact (eng/empty-db)
                         (map (fn [{:keys [e a v_edn]}] {:s e :p a :o (edn/read-string v_edn)})
                              rows)))))

(defn do-q
  "`datomic.q` -- triple-pattern query. Rebuilds a hot db from
  snapshot+novelty then routes through arrangement.query. body:
  {:graph :query_edn} where query_edn is a `[s p o]` pattern (nil =
  wildcard).

  `eng/q`'s `visible?` is required (ADR-2607050500) -- passes
  `(constantly true)` explicitly, same reasoning as `do-datoms`."
  [store {:keys [graph query_edn]}]
  (let [chain ((:head-get store) graph)
        pat   (edn/read-string query_edn)]
    (then* (hot-db store chain)
           (fn [db] {:ok true :graph graph :rows (vec (eng/q db pat (constantly true)))}))))

(defn do-pull
  "`datomic.pull` -- all attrs of one entity, via hot-datoms (snapshot +
  novelty merge). body: {:graph :entity}."
  [store {:keys [graph entity]}]
  (let [chain ((:head-get store) graph)]
    (then* (eng/hot-datoms (:get-fn store) chain {:index :eavt :components [entity]}
                           (constantly true) (:blind-fn store) (:decrypt-fn store))
           (fn [rows]
             {:ok true :graph graph :entity entity
              :attrs (reduce (fn [m {:keys [a v_edn]}] (update m a (fnil conj []) v_edn)) {} rows)}))))

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

;; ── dispatch ─────────────────────────────────────────────────────────────────

(defn handle
  "Dispatch a parsed XRPC call. `method` is the NSID suffix after
  `ai.gftd.apps.kotobase.datomic.`; `body` is the keywordized JSON body;
  `auth-did` is the CACAO-verified issuer or nil. Returns a plain response
  map; never throws for a known method (errors become `{:ok false :error
  …}`) EXCEPT a storage-layer block-miss (`kotobase.server.trampoline`'s
  `:block-miss` marker), which re-throws so the caller's trampoline sees it
  and retries -- swallowing it here would surface as a spurious
  InternalError instead."
  [store method body auth-did]
  (letfn [(err [e]
            (if (:block-miss (ex-data e))
              (throw e)
              {:ok false :error "InternalError"
               :message #?(:clj (.getMessage ^Exception e)
                           :cljs (or (ex-message e) (.-message e)))}))]
    (try
      (let [resp (case method
                   "datoms"   (do-datoms store body)
                   "transact" (do-transact store body auth-did)
                   "q"        (do-q store body)
                   "pull"     (do-pull store body)
                   "fold"     (do-fold store body)
                   {:ok false :error "MethodNotImplemented" :method method})]
        #?(:clj resp
           :cljs (.catch (js/Promise.resolve resp) err)))
      (catch #?(:clj Exception :cljs :default) e
        (err e)))))
