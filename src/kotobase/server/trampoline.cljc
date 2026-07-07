(ns kotobase.server.trampoline
  "Bridge an ASYNC block store (any `fetch1: cid -> Promise<bytes>`) to
  kotobase-peer's SYNCHRONOUS block reader contract (`(get-fn cid) -> bytes`).
  Storage-agnostic by construction -- works identically over Cloudflare R2,
  Backblaze B2, IndexedDB, or anything else that can answer \"give me the
  bytes at this CID\" asynchronously; the concrete `fetch1` is entirely the
  caller's concern.

  `cold-datoms`/`hydrate-db`/`commit!` walk a prolly-tree (or, for `commit!`,
  just the chain head) through that sync get-fn, but any real storage
  backend is async. `with-blocks` runs the sync computation against an
  in-memory block cache and, on a cache miss, fetches the block via `fetch1`
  and retries -- a block-miss trampoline. Correct for any tree shape;
  efficient once the read touches few blocks (prolly-tree prefix pruning
  bounds this for a filtered read, so this stays cheap even as a graph
  grows -- unlike a full-prefix warm)."
  (:require [clojure.string :as str]))

(defn missing-block
  "Signal thrown by the sync get-fn on a cache miss; caught by `with-blocks`.
  The `:block-miss` marker lets a caller's error handler re-throw it (rather
  than swallow it as a generic failure) so the trampoline actually sees the
  miss -- see `block-miss?`."
  [cid]
  (ex-info "block-miss" {:block-miss true :cid cid}))

(defn block-miss?
  "True if `e` is the trampoline's cache-miss signal. A caller wrapping a
  `with-blocks`-driven computation in its OWN try/catch (e.g. to turn engine
  errors into an API error response) must re-throw when this is true,
  instead of swallowing it -- otherwise the trampoline never sees the miss
  and the retry never happens."
  [e]
  (boolean (:block-miss (ex-data e))))

(defn with-blocks
  "Run `(f sync-get)` where `sync-get` reads from an in-memory cache,
  fetching absent blocks via `(fetch1 cid) -> Promise<bytes>` and retrying.
  Returns a Promise of `f`'s result. `f` must be pure/idempotent (it is
  re-run per miss, from scratch).

  `f` may return a plain value OR a js/Promise (kotobase-peer's crypto seam,
  ADR-2607051000, makes engine calls Promise-returning on cljs) -- a
  block-miss can therefore surface either as a SYNC throw from `f` or as a
  REJECTION of `f`'s promise (sync-get called inside a `.then`
  continuation), and both trampoline the same way. Non-miss failures reject
  through unchanged."
  [fetch1 f]
  (let [cache (atom {})
        sync-get (fn [cid]
                   (if (contains? @cache cid)
                     (get @cache cid)
                     (throw (missing-block cid))))]
    (letfn [(fetch-and-retry [e]
              (if (:block-miss (ex-data e))
                (-> (fetch1 (:cid (ex-data e)))
                    (.then (fn [bytes]
                             (swap! cache assoc (:cid (ex-data e)) bytes)
                             (step))))
                (js/Promise.reject e)))
            (step []
              (try
                (-> (js/Promise.resolve (f sync-get))
                    (.catch fetch-and-retry))
                (catch :default e (fetch-and-retry e))))]
      (step))))
