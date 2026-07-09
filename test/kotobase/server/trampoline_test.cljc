(ns kotobase.server.trampoline-test
  "kotobase.server.trampoline tests -- ClojureScript only (js/Promise-based,
  no JVM path, matching the source's own unconditional cljs syntax)."
  (:require [cljs.test :refer-macros [deftest is testing async]]
            [kotobase.server.trampoline :as tr]))

(deftest block-miss?-distinguishes-the-trampoline-signal-from-other-errors
  (is (true? (tr/block-miss? (tr/missing-block "bafyreiX"))))
  (is (false? (tr/block-miss? (ex-info "boom" {}))))
  (is (false? (tr/block-miss? (js/Error. "plain js error")))))

(deftest with-blocks-resolves-immediately-when-f-never-misses
  (async done
    (let [fetch-calls (atom 0)
          fetch1 (fn [_cid] (swap! fetch-calls inc) (js/Promise.resolve #js []))]
      (-> (tr/with-blocks fetch1 (fn [_sync-get] 42))
          (.then (fn [v]
                   (is (= 42 v))
                   (is (= 0 @fetch-calls) "f never called sync-get, so fetch1 never runs")
                   (done)))
          (.catch (fn [e] (is false (str "unexpected: " e)) (done)))))))

(deftest with-blocks-fetches-a-missing-block-and-retries-from-scratch
  (async done
    (let [fetch-calls (atom [])
          bytes-for {"a" #js [1 2 3]}
          fetch1 (fn [cid] (swap! fetch-calls conj cid) (js/Promise.resolve (get bytes-for cid)))]
      (-> (tr/with-blocks fetch1 (fn [sync-get] (.-length (sync-get "a"))))
          (.then (fn [v]
                   (is (= 3 v) "f re-ran after the miss and read the now-cached block")
                   (is (= ["a"] @fetch-calls) "fetch1 called exactly once for the one cid f needed")
                   (done)))
          (.catch (fn [e] (is false (str "unexpected: " e)) (done)))))))

(deftest with-blocks-handles-multiple-sequential-misses-without-refetching-cached-blocks
  (async done
    (let [fetch-calls (atom [])
          bytes-for {"a" #js [1] "b" #js [2 2]}
          fetch1 (fn [cid] (swap! fetch-calls conj cid) (js/Promise.resolve (get bytes-for cid)))
          f (fn [sync-get] (+ (.-length (sync-get "a")) (.-length (sync-get "b"))))]
      (-> (tr/with-blocks fetch1 f)
          (.then (fn [v]
                   (is (= 3 v))
                   (is (= ["a" "b"] @fetch-calls)
                       "each distinct cid fetched exactly once, in miss order -- \"a\" is never refetched once cached")
                   (done)))
          (.catch (fn [e] (is false (str "unexpected: " e)) (done)))))))

(deftest with-blocks-trampolines-a-miss-thrown-inside-an-async-continuation
  (async done
    ;; Mirrors kotobase-peer's own crypto seam being Promise-returning on
    ;; cljs (ADR-2607051000): sync-get called inside a `.then`, so the miss
    ;; surfaces as a promise REJECTION, not a sync throw -- with-blocks'
    ;; docstring calls this out explicitly as a case it must also handle.
    (let [fetch-calls (atom 0)
          fetch1 (fn [_cid] (swap! fetch-calls inc) (js/Promise.resolve #js [9 9]))
          f (fn [sync-get]
              (-> (js/Promise.resolve nil)
                  (.then (fn [_] (.-length (sync-get "x"))))))]
      (-> (tr/with-blocks fetch1 f)
          (.then (fn [v]
                   (is (= 2 v))
                   (is (= 1 @fetch-calls))
                   (done)))
          (.catch (fn [e] (is false (str "unexpected: " e)) (done)))))))

(deftest with-blocks-propagates-a-non-miss-failure-without-retrying
  (async done
    (let [fetch-calls (atom 0)
          fetch1 (fn [_cid] (swap! fetch-calls inc) (js/Promise.resolve #js []))
          boom (js/Error. "not a block miss")]
      (-> (tr/with-blocks fetch1 (fn [_sync-get] (throw boom)))
          (.then (fn [_] (is false "should have rejected") (done)))
          (.catch (fn [e]
                    (is (= boom e) "the original error propagates unchanged")
                    (is (= 0 @fetch-calls) "no retry attempted for a non-miss failure")
                    (done)))))))
