(ns kotobase.server.handler-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing async] :include-macros true])
            #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [kotobase.server.handler :as h]))

(defn- passthrough
  "kotobase-peer's crypto seam is synchronous on JVM but Promise-returning
  on cljs (ADR-2607051000) -- EVEN a plaintext-passthrough profile must
  respect that platform split, or kotobase-peer's own `.then` calls on the
  result throw on cljs. `identity` alone is wrong there."
  [x] #?(:clj x :cljs (js/Promise.resolve x)))

(defn- mem-store
  "An in-memory store with a plaintext-passthrough crypto profile -- the
  simplest possible `store` satisfying `kotobase.server.handler`'s
  contract, for node-testability without any real storage backend."
  []
  (let [blocks (atom {})
        heads (atom {})]
    {:get-fn (fn [cid] (get @blocks cid))
     :put! (fn [cid bytes] (swap! blocks assoc cid bytes))
     :head-get (fn [graph] (get @heads graph))
     :head-put! (fn [graph chain] (swap! heads assoc graph chain))
     :blind-fn passthrough
     :encrypt-fn passthrough
     :decrypt-fn passthrough}))

;; `handle` returns a plain value on :clj, a js/Promise on :cljs
;; (ADR-2607051000's platform split, inherited unchanged). `run-steps`
;; threads a sequence of (fn [] ...) steps through that split uniformly:
;; on :clj each step runs synchronously in order; on :cljs each step's
;; Promise is awaited before the next runs, and `cljs.test/async`'s `done`
;; is called only once the whole chain settles -- otherwise cljs.test moves
;; on before any assertion inside a `.then` callback ever runs, silently
;; reporting 0 assertions instead of failing loudly.
(defn- run-steps [steps]
  #?(:clj (doseq [step steps] (step))
     :cljs (reduce (fn [p step] (.then p (fn [_] (step)))) (js/Promise.resolve nil) steps)))

(deftest transact-datoms-q-pull-fold-roundtrip
  (let [store (mem-store)]
    #?(:clj
       (run-steps
        [(fn []
           (let [tx (h/handle store "transact"
                               {:graph "g1" :tx_edn "[{:db/id \"e1\" :yoro.post/text \"hello\"}]"} "did:key:ztest")]
             (is (:ok tx))
             (is (= 1 (:datom_count tx)))))
         (fn []
           (let [datoms (h/handle store "datoms" {:graph "g1"} nil)]
             (is (:ok datoms))
             (is (= 1 (count (:datoms datoms))))
             (is (= ":yoro.post/text" (:a (first (:datoms datoms)))))))
         (fn []
           (let [q (h/handle store "q" {:graph "g1" :query_edn "[nil \":yoro.post/text\" nil]"} nil)]
             (is (:ok q))
             (is (= 1 (count (:rows q))))
             (is (= "hello" (:o (first (:rows q)))))))
         (fn []
           (let [pull (h/handle store "pull" {:graph "g1" :entity "e1"} nil)]
             (is (:ok pull))
             (is (contains? (:attrs pull) ":yoro.post/text"))))
         (fn []
           (let [fold (h/handle store "fold" {:graph "g1"} nil)]
             (is (:ok fold))
             (is (:folded fold))))])
       :cljs
       (async
        done
        (-> (run-steps
             [(fn [] (.then (h/handle store "transact"
                                     {:graph "g1" :tx_edn "[{:db/id \"e1\" :yoro.post/text \"hello\"}]"} "did:key:ztest")
                            (fn [tx]
                              (is (:ok tx))
                              (is (= 1 (:datom_count tx))))))
              (fn [] (.then (h/handle store "datoms" {:graph "g1"} nil)
                            (fn [datoms]
                              (is (:ok datoms))
                              (is (= 1 (count (:datoms datoms))))
                              (is (= ":yoro.post/text" (:a (first (:datoms datoms))))))))
              (fn [] (.then (h/handle store "q" {:graph "g1" :query_edn "[nil \":yoro.post/text\" nil]"} nil)
                            (fn [q]
                              (is (:ok q))
                              (is (= 1 (count (:rows q))))
                              (is (= "hello" (:o (first (:rows q))))))))
              (fn [] (.then (h/handle store "pull" {:graph "g1" :entity "e1"} nil)
                            (fn [pull]
                              (is (:ok pull))
                              (is (contains? (:attrs pull) ":yoro.post/text")))))
              (fn [] (.then (h/handle store "fold" {:graph "g1"} nil)
                            (fn [fold]
                              (is (:ok fold))
                              (is (:folded fold)))))])
            (.then (fn [_] (done)))
            (.catch (fn [e] (is false (str "unexpected rejection: " e)) (done))))))))

(deftest unknown-method-is-clean-error
  (let [store (mem-store)]
    #?(:clj
       (let [resp (h/handle store "bogus" {} nil)]
         (is (not (:ok resp)))
         (is (= "MethodNotImplemented" (:error resp))))
       :cljs
       (async
        done
        (-> (h/handle store "bogus" {} nil)
            (.then (fn [resp]
                     (is (not (:ok resp)))
                     (is (= "MethodNotImplemented" (:error resp)))
                     (done))))))))

;; ── datomic.* extension coverage ──────────────────────────────────────────
;; `step` collapses the platform-split boilerplate `run-steps` (above)
;; already threads through: :clj calls `f` synchronously on `handle`'s
;; plain return; :cljs chains `f` via `.then` on `handle`'s Promise, and
;; the resulting Promise is itself what `run-steps`'s own `.then`-reduce
;; chains on -- composes correctly either way, `f` only ever does `is`
;; assertions and its return value is discarded.

(defn- step [store method body auth-did f]
  (fn []
    #?(:clj (f (h/handle store method body auth-did))
       :cljs (.then (h/handle store method body auth-did) f))))

(defn- run-async [steps]
  #?(:clj (run-steps steps)
     :cljs (async done
             (-> (run-steps steps)
                 (.then (fn [_] (done)))
                 (.catch (fn [e] (is false (str "unexpected rejection: " e)) (done)))))))

(deftest entity-entid-ident-and-pattern-pull-roundtrip
  (let [store (mem-store)]
    (run-async
     [(step store "transact"
            {:graph "g2" :tx_edn "[{:db/id \"alice\" :name \"Alice\" :db/ident :person/alice} {:db/id \"bob\" :name \"Bob\"}]"}
            "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "entity" {:graph "g2" :entity "alice"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= {":name" #{"Alice"} ":db/ident" #{":person/alice"}} (edn/read-string (:entity_edn resp))))))
      (step store "entid" {:graph "g2" :ident_edn ":person/alice"} nil
            (fn [resp] (is (:ok resp)) (is (= "alice" (:entity_id resp)))))
      (step store "entid" {:graph "g2" :ident_edn "\"bob\""} nil
            (fn [resp] (is (:ok resp)) (is (= "bob" (:entity_id resp)))
              "a plain (non-keyword) id passes through unchanged"))
      (step store "ident" {:graph "g2" :entity "alice"} nil
            (fn [resp] (is (:ok resp)) (is (= :person/alice (edn/read-string (:ident_edn resp))))))
      (step store "ident" {:graph "g2" :entity "bob"} nil
            (fn [resp] (is (:ok resp)) (is (nil? (edn/read-string (:ident_edn resp))))))
      (step store "pull" {:graph "g2" :entity "alice" :pattern_edn "[\":name\"]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= {":name" #{"Alice"}} (edn/read-string (:result_edn resp))))
              "pattern path returns exactly the requested attrs, not the whole entity"))
      (step store "pullMany" {:graph "g2" :entities ["alice" "bob"] :pattern_edn "[*]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 2 (count (:results_edn resp))))
              (is (= #{"Alice"} (get (edn/read-string (first (:results_edn resp))) ":name")))))
      (step store "indexPull" {:graph "g2" :entities ["bob"] :pattern_edn "[*]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{"Bob"} (get (edn/read-string (first (:results_edn resp))) ":name")))
              "indexPull is a direct alias of pullMany"))])))

(deftest as-of-since-history-across-two-commits
  (let [store (mem-store)
        t0 (atom nil)]
    (run-async
     [(step store "transact" {:graph "g3" :tx_edn "[{:db/id \"e1\" :n 1}]"} "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "basisT" {:graph "g3"} nil
            (fn [resp] (is (:ok resp)) (reset! t0 (:t resp))))
      (step store "transact" {:graph "g3" :tx_edn "[{:db/id \"e2\" :n 2}]"} "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      ;; asOf/since's body depends on t0, only known after the first
      ;; `step` above actually runs -- `step`'s own `body` arg is
      ;; evaluated eagerly at list-construction time, before t0 is set, so
      ;; these two can't use `step` the way the other calls do; write the
      ;; platform split directly instead, same as `step` does internally.
      (fn []
        #?(:clj (let [resp (h/handle store "asOf" {:graph "g3" :t @t0} nil)]
                  (is (:ok resp))
                  (is (= 1 (count (:datoms resp))))
                  (is (= "e1" (:e (first (:datoms resp))))
                      "asOf t0 must not see e2, committed after t0"))
           :cljs (.then (h/handle store "asOf" {:graph "g3" :t @t0} nil)
                        (fn [resp]
                          (is (:ok resp))
                          (is (= 1 (count (:datoms resp))))
                          (is (= "e1" (:e (first (:datoms resp)))))))))
      (fn []
        #?(:clj (let [resp (h/handle store "since" {:graph "g3" :t @t0} nil)]
                  (is (:ok resp))
                  (is (= 1 (count (:datoms resp))))
                  (is (= "e2" (:e (first (:datoms resp))))
                      "since t0 must see ONLY e2, the commit after t0"))
           :cljs (.then (h/handle store "since" {:graph "g3" :t @t0} nil)
                        (fn [resp]
                          (is (:ok resp))
                          (is (= 1 (count (:datoms resp))))
                          (is (= "e2" (:e (first (:datoms resp)))))))))
      (step store "history" {:graph "g3"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 2 (count (:datoms resp))) "history sees both commits, unfiltered")))
      (step store "history" {:graph "g3" :entity "e1"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 1 (count (:datoms resp))))
              (is (= "e1" (:e (first (:datoms resp)))) "entity filter narrows correctly")))])))

(deftest basis-t-and-db-stats
  (let [store (mem-store)]
    (run-async
     [(step store "basisT" {:graph "fresh-graph"} nil
            (fn [resp] (is (:ok resp)) (is (nil? (:t resp))) "no commit yet -> nil t, not an error"))
      (step store "dbStats" {:graph "fresh-graph"} nil
            (fn [resp]
              (is (:ok resp))
              (is (false? (:has_snapshot resp)))
              (is (zero? (:novelty_size resp)))))
      (step store "transact" {:graph "g4" :tx_edn "[{:db/id \"e1\" :n 1}]"} "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "basisT" {:graph "g4"} nil
            (fn [resp] (is (:ok resp)) (is (= 0 (:t resp)))))
      (step store "dbStats" {:graph "g4"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 0 (:t resp)))
              (is (false? (:has_snapshot resp)) "no fold yet")
              (is (= 1 (:novelty_size resp)))))])))

(deftest seek-datoms-and-index-range
  (let [store (mem-store)]
    (run-async
     [(step store "transact"
            {:graph "g5" :tx_edn "[{:db/id \"e1\" :score 10} {:db/id \"e2\" :score 20} {:db/id \"e3\" :score 30}]"}
            "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "seekDatoms" {:graph "g5" :index ":eavt" :components_edn ["e2"]} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 1 (count (:datoms resp))))
              (is (= "e2" (:e (first (:datoms resp)))))))
      ;; start_edn/end_edn must be EDN-typed to match what's actually
      ;; stored -- this engine stringifies every non-Link value at write
      ;; time (kotobase-server's own tx-edn->quads, `(str v)`), so
      ;; `:score 10` is stored (and v_edn-decodes back) as the STRING
      ;; "10", never the number 10. A bare `"15"` here would edn/read-string
      ;; to the NUMBER 15, and `compare`ing a String against a Long throws
      ;; -- see do-index-range's own docstring for why this is a real,
      ;; documented engine limitation (lexicographic, not numeric, string
      ;; comparison), not something to silently paper over here.
      (step store "indexRange" {:graph "g5" :attr ":score" :start_edn "\"15\"" :end_edn "\"25\""} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 1 (count (:datoms resp))))
              (is (= "e2" (:e (first (:datoms resp)))) "only score=20 falls in [15,25]")))
      (step store "indexRange" {:graph "g5" :attr ":score" :start_edn "\"15\""} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 2 (count (:datoms resp))) "unbounded end -> e2,e3 (score>=15)")))])))

(deftest tx-tx-range-log-sync
  (let [store (mem-store)]
    (run-async
     [(step store "transact" {:graph "g6" :tx_edn "[{:db/id \"e1\" :n 1}]"} "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "transact" {:graph "g6" :tx_edn "[{:db/id \"e2\" :n 2}]"} "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "tx" {:graph "g6" :t 0} nil
            (fn [resp] (is (:ok resp)) (is (true? (:found resp))) (is (some? (:tx_cid resp)))))
      (step store "tx" {:graph "g6" :t 99} nil
            (fn [resp] (is (:ok resp)) (is (false? (:found resp))) (is (nil? (:tx_cid resp)))))
      (step store "txRange" {:graph "g6" :start 0 :end 1} nil
            (fn [resp] (is (:ok resp)) (is (= 1 (count (:txs resp)))) (is (= 0 (:t (first (:txs resp)))))))
      (step store "log" {:graph "g6"} nil
            (fn [resp] (is (:ok resp)) (is (= 2 (count (:txs resp))) "log with no bounds = the whole tx-range")))
      (step store "sync" {:graph "g6" :t 1} nil
            (fn [resp] (is (:ok resp)) (is (true? (:caught_up resp)))))
      (step store "sync" {:graph "g6" :t 99} nil
            (fn [resp] (is (:ok resp)) (is (false? (:caught_up resp)))
              "an unreached t is reported honestly, not blocked on"))])))

(deftest do-q-routes-datalog-map-queries-to-the-right-engine
  (let [store (mem-store)]
    (run-async
     [(step store "transact"
            {:graph "g7" :tx_edn "[{:db/id \"e1\" :name \"Alice\" :role \"admin\"} {:db/id \"e2\" :name \"Bob\" :role \"user\"}]"}
            "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "q" {:graph "g7" :query_edn "[nil \":role\" \"admin\"]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 1 (count (:rows resp))))
              (is (= "e1" (:s (first (:rows resp)))) "unchanged: triple-pattern still routes to eng/q")))
      (step store "q" {:graph "g7"
                        :query_edn "{:find [?n] :where [[?e \":role\" \"admin\"] [?e \":name\" ?n]]}"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["Alice"]} (set (:rows resp))))
              "map-shaped query_edn now actually routes to eng/query (the real
               Datalog engine) instead of being silently mistreated as an
               empty/malformed triple pattern"))])))

;; ── query-literal normalization (the live "q sees nothing datoms sees"
;; bug, confirmed against backend.kotobase.net 2026-07-09): the write path
;; stores every non-Link position stringified (tx-edn->quads, `(str v)`),
;; so a query written the natural Datomic way -- keyword attributes,
;; keyword/number value literals -- previously compared a keyword/number
;; against the stored string, matched nothing, and returned ok+empty rows.

(deftest do-q-normalizes-keyword-and-number-literals-to-the-stored-strings
  (let [store (mem-store)]
    (run-async
     [(step store "transact"
            {:graph "g8"
             :tx_edn "[{:db/id \"w1\" :gh.genko/title \"First\" :gh.genko/status :draft :gh.genko/rev 3} {:db/id \"w2\" :gh.genko/title \"Second\" :gh.genko/status :done :gh.genko/rev 7}]"}
            "did:key:ztest"
            (fn [tx] (is (:ok tx))))
      (step store "q" {:graph "g8" :query_edn "[nil :gh.genko/title nil]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 2 (count (:rows resp)))
                  "triple pattern with a KEYWORD attribute now matches the stored string attr")))
      (step store "q" {:graph "g8" :query_edn "{:find [?e ?t] :where [[?e :gh.genko/title ?t]]}"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["w1" "First"] ["w2" "Second"]} (set (:rows resp)))
                  "Datalog clause with a KEYWORD attribute now matches")))
      (step store "q" {:graph "g8"
                        :query_edn "{:find [?t] :where [[?e :gh.genko/status :draft] [?e :gh.genko/title ?t]]}"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["First"]} (set (:rows resp)))
                  "a KEYWORD value literal matches the stored \":draft\" string")))
      (step store "q" {:graph "g8"
                        :query_edn "{:find [?t] :where [[?e :gh.genko/rev 7] [?e :gh.genko/title ?t]]}"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["Second"]} (set (:rows resp)))
                  "a NUMBER value literal matches the stored \"7\" string")))
      (step store "q" {:graph "g8"
                        :query_edn "{:find [?t] :in [?s] :where [[?e :gh.genko/status ?s] [?e :gh.genko/title ?t]]}"
                        :inputs_edn "[:done]"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["Second"]} (set (:rows resp)))
                  "a KEYWORD input (:in binding) is coerced the same way")))
      (step store "q" {:graph "g8"
                        :query_edn "{:find [?t] :where [[?e :gh.genko/title ?t] (not [?e :gh.genko/status :done])]}"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["First"]} (set (:rows resp)))
                  "literals inside (not [...]) are normalized too")))
      (step store "q" {:graph "g8"
                        :query_edn "{:find [?t] :rules [[(drafted ?e) [?e :gh.genko/status :draft]]] :where [(drafted ?e) [?e :gh.genko/title ?t]]}"}
            nil
            (fn [resp]
              (is (:ok resp))
              (is (= #{["First"]} (set (:rows resp)))
                  "literals inside :rules bodies are normalized too")))
      ;; unchanged behavior: string-literal queries (every existing caller)
      (step store "q" {:graph "g8" :query_edn "[nil \":gh.genko/title\" nil]"} nil
            (fn [resp]
              (is (:ok resp))
              (is (= 2 (count (:rows resp))) "pre-existing string-attr form still works")))])))

(deftest normalize-query-literals-is-precise-about-what-it-touches
  (testing "pattern syntax and structured literals pass through untouched"
    (is (= [nil ":a/b" nil] (h/normalize-query-literals [nil :a/b nil])))
    (is (= '{:find [?e] :where [[?e ":x" "s"] [(> ?n 18)] [(str ?a ?b) ?c]]}
           (h/normalize-query-literals
            '{:find [?e] :where [[?e :x "s"] [(> ?n 18)] [(str ?a ?b) ?c]]}))
        "predicate/function clauses are NOT coerced -- they evaluate over
         bound values, they aren't matched against stored datoms")
    (is (= '{:where [[?e ":ref" ["ipld/link" "bafy..."]]]}
           (h/normalize-query-literals '{:where [[?e :ref ["ipld/link" "bafy..."]]]}))
        "collection literals (a Link's wire form) keep today's behavior")
    (is (= '{:where [[?e ":x" ?v] (or [?v ":k" "1"] [?v ":k" "2"])]}
           (h/normalize-query-literals
            '{:where [[?e :x ?v] (or [?v :k 1] [?v :k 2])]}))
        "or-branches are normalized; lvars stay lvars")
    (is (= '{:where [(or-join [?v] [?v ":k" ":kw"])]}
           (h/normalize-query-literals '{:where [(or-join [?v] [?v :k :kw])]}))
        "or-join keeps its shared-vars vector, normalizes its branches")))
