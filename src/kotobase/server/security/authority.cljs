(ns kotobase.server.security.authority
  "Verification of explicit CACAO delegation chains. Grants are ordered from
  trusted root to leaf and monotonically attenuated."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [ipld.core :as ipld]
            [kotobase.cacao :as cacao]
            [kotobase.cid :as cid]
            ["@ipld/dag-cbor" :as dag-cbor]
            ["@noble/curves/ed25519.js" :refer [ed25519]]))

(defn- finite-time [s]
  (when (string? s)
    (let [n (js/Date.parse s)] (when (js/Number.isFinite n) n))))

(defn verify-grant
  [encoded {:keys [graph tenant-id require-tenant-binding? now-ms
                   revoked-credential-cids max-clock-skew-ms]
            :or {now-ms (.now js/Date) max-clock-skew-ms 60000}}]
  (try
    (let [bytes (cacao/base64->bytes encoded)
          credential-cid (ipld/cid bytes)
          ^js envelope (.decode dag-cbor bytes)
          ^js h (.-h envelope) ^js p (.-p envelope) ^js s (.-s envelope)
          issuer (.-iss p) audience (.-aud p)
          resources (set (vec (.-resources p)))
          graph-resource (str "kotoba://graph/" graph)
          tenant-resource (str "kotoba://tenant/" tenant-id)
          iat (finite-time (.-iat p)) exp (finite-time (.-exp p))
          payload {:domain (.-domain p) :iss issuer :aud audience
                   :version (.-version p) :nonce (.-nonce p)
                   :iat (.-iat p) :exp (.-exp p) :statement (.-statement p)
                   :resources (vec (.-resources p))}
          pub (cid/did-key->ed25519-pub issuer)
          sig (cacao/base64url->bytes (.-s s))]
      (when (and (= "caip122" (.-t h)) (= "EdDSA" (.-t s))
                 (= "kotobase.net" (.-domain p)) (= "1" (.-version p))
                 pub (string? audience) (seq audience)
                 iat exp (<= iat (+ now-ms max-clock-skew-ms)) (> exp now-ms)
                 (contains? resources graph-resource)
                 (or (not require-tenant-binding?) (contains? resources tenant-resource))
                 (every? #(or (= graph-resource %)
                              (= tenant-resource %)
                              (str/starts-with? % "kotoba://can/")) resources)
                 (not (contains? (set revoked-credential-cids) credential-cid))
                 (.verify ed25519 sig (cid/text->bytes (cacao/cacao-siwe-message payload)) pub))
        {:issuer issuer :audience audience :resources resources
         :issued-at iat :expires-at exp :credential-cid credential-cid}))
    (catch :default _ nil)))

(defn verify-chain
  "Return effective capabilities and credential CIDs, or nil."
  [encoded-chain {:keys [principal trusted-root-dids] :as opts}]
  (let [grants (mapv #(verify-grant % opts) encoded-chain)]
    (when (and (seq grants) (every? some? grants)
               (contains? (set trusted-root-dids) (:issuer (first grants)))
               (= principal (:audience (peek grants)))
               (every? true?
                       (map (fn [parent child]
                              (and (= (:audience parent) (:issuer child))
                                   (set/subset? (:resources child) (:resources parent))
                                   (<= (:expires-at child) (:expires-at parent))))
                            grants (rest grants))))
      {:effective-caps (:resources (peek grants))
       :credential-cids (mapv :credential-cid grants)
       :root-did (:issuer (first grants))
       :delegated? true})))
