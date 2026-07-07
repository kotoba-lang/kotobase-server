(ns kotobase.server.handler-test
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing async] :include-macros true])
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
