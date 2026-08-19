(ns kotobase.server.security.biscuit-authority-test
  "The biscuit path into the same `:effective-caps` the handler reads."
  (:require [biscuit.token :as bt]
            [cljs.test :refer-macros [deftest is testing]]
            [kotobase.server.security.authority :as auth]
            ["@noble/curves/ed25519.js" :refer [ed25519]]))

(defn- keypair [seed]
  (let [priv (js/Uint8Array.from (clj->js (vec seed)))]
    {:private priv :public (vec (.getPublicKey ed25519 priv))}))

(def root (keypair (range 32)))
(def k1 (keypair (range 32 64)))
(def attacker (keypair (map #(+ 100 %) (range 32))))

(defn- sign-fn [priv payload]
  (vec (.sign ed25519 (.encode (js/TextEncoder.) payload) priv)))

(defn- token [facts & [{:keys [signer]}]]
  (bt/authority {:facts facts :next-public-key (:public k1)
                 :root-private-key (or signer (:private root)) :sign-fn sign-fn}))

(def opts {:graph "acme" :tenant-id "t1" :root-public-key (:public root)
           :now-ms (.parse js/Date "2026-08-19T00:00:00Z")})

(deftest a-biscuit-yields-the-same-shape-cacao-does
  (let [r (auth/verify-biscuit (token '[[scope "kotoba://graph/acme"]
                                        [scope "kotoba://can/read"]])
                               opts)]
    (is (true? (:delegated? r)))
    (is (= :biscuit (:wire r)))
    (is (contains? (:effective-caps r) "kotoba://graph/acme"))
    (is (contains? (:effective-caps r) "kotoba://can/read"))))

(deftest verification-uses-only-the-root-public-key
  (testing "the property that lets this run at an edge"
    (is (some? (auth/verify-biscuit (token '[[scope "kotoba://graph/acme"]]) opts)))
    (testing "and a token minted by anyone else is refused"
      (is (nil? (auth/verify-biscuit
                 (token '[[scope "kotoba://graph/acme"]] {:signer (:private attacker)})
                 opts))))))

(deftest a-later-block-can-only-narrow
  (let [t (-> (token '[[scope "kotoba://graph/acme"] [scope "kotoba://can/read"]])
              (bt/append {:facts '[[scope "kotoba://graph/acme"]
                                   [scope "kotoba://can/write"]]
                          :next-public-key (:public k1)
                          :private-key (:private k1) :sign-fn sign-fn}))
        r (auth/verify-biscuit t opts)]
    (testing "can/write was never conferred, so appending it confers nothing"
      (is (= #{"kotoba://graph/acme"} (:effective-caps r))))))

(deftest a-biscuit-cannot-name-a-resource-a-cacao-could-not
  (testing "same three shape rules verify-grant enforces"
    (is (nil? (auth/verify-biscuit (token '[[scope "kotoba://graph/acme"]
                                            [scope "kotoba://evil/anything"]])
                                   opts)))))

(deftest the-graph-must-be-in-the-token
  (is (nil? (auth/verify-biscuit (token '[[scope "kotoba://can/read"]]) opts))))

(deftest tenant-binding-is-enforced-when-required
  (let [t (token '[[scope "kotoba://graph/acme"]])]
    (is (some? (auth/verify-biscuit t opts)))
    (is (nil? (auth/verify-biscuit t (assoc opts :require-tenant-binding? true))))
    (is (some? (auth/verify-biscuit (token '[[scope "kotoba://graph/acme"]
                                             [scope "kotoba://tenant/t1"]])
                                    (assoc opts :require-tenant-binding? true))))))

(deftest an-expired-token-is-refused
  (let [t (token '[[scope "kotoba://graph/acme"] [before "2026-08-01T00:00:00Z"]])]
    (is (nil? (auth/verify-biscuit t opts)))
    (is (some? (auth/verify-biscuit t (assoc opts :now-ms (.parse js/Date "2026-07-01T00:00:00Z")))))))

(deftest a-tampered-block-is-refused
  (let [t (token '[[scope "kotoba://graph/acme"]])
        forged (assoc-in t [:biscuit/blocks 0 :block/facts]
                         '[[scope "kotoba://graph/acme"] [scope "kotoba://can/admin"]])]
    (is (nil? (auth/verify-biscuit forged opts)))))
