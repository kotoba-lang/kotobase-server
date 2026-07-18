(ns kotobase.server.security.keywrap
  "RFC 9180 HPKE wrapping for serialized DEK keyrings."
  (:require [kotobase.cacao :as cacao]
            ["hpke-js" :refer [AeadId CipherSuite KdfId KemId]]))

(defn- suite []
  (CipherSuite. #js {:kem (.-DhkemX25519HkdfSha256 KemId)
                     :kdf (.-HkdfSha256 KdfId)
                     :aead (.-Chacha20Poly1305 AeadId)}))
(defn- utf8 [s] (.encode (js/TextEncoder.) s))

(defn generate-recipient-keypair []
  (let [s (suite)]
    (-> (.generateKeyPair (.-kem s))
        (.then (fn [^js pair]
                 (js/Promise.all
                  #js [(.serializePublicKey (.-kem s) (.-publicKey pair))
                       (.serializePrivateKey (.-kem s) (.-privateKey pair))])))
        (.then (fn [parts]
                 {:public-key-b64 (cacao/bytes->base64 (js/Uint8Array. (aget parts 0)))
                  :private-key-b64 (cacao/bytes->base64 (js/Uint8Array. (aget parts 1)))})))))

(defn wrap-keyring [public-key-b64 keyring context]
  (let [s (suite) info (utf8 (str "kotobase/keyring/v1\u0000" context))]
    (-> (.deserializePublicKey (.-kem s) (cacao/base64->bytes public-key-b64))
        (.then (fn [public-key]
                 (.createSenderContext s #js {:recipientPublicKey public-key :info info})))
        (.then (fn [^js sender]
                 (-> (.seal sender (utf8 (js/JSON.stringify (clj->js keyring))) info)
                     (.then (fn [ciphertext]
                              {:version 1 :suite "X25519-HKDF-SHA256-CHACHA20POLY1305"
                               :enc (cacao/bytes->base64 (js/Uint8Array. (.-enc sender)))
                               :ciphertext (cacao/bytes->base64
                                            (js/Uint8Array. ciphertext))}))))))))

(defn unwrap-keyring [private-key-b64 envelope context]
  (when-not (and (= 1 (:version envelope))
                 (= "X25519-HKDF-SHA256-CHACHA20POLY1305" (:suite envelope)))
    (throw (ex-info "unsupported wrapped keyring" {:type :keywrap-envelope-invalid})))
  (let [s (suite) info (utf8 (str "kotobase/keyring/v1\u0000" context))]
    (-> (.deserializePrivateKey (.-kem s) (cacao/base64->bytes private-key-b64))
        (.then (fn [private-key]
                 (.createRecipientContext
                  s #js {:recipientKey private-key
                         :enc (cacao/base64->bytes (:enc envelope)) :info info})))
        (.then (fn [recipient]
                 (.open recipient (cacao/base64->bytes (:ciphertext envelope)) info)))
        (.then (fn [plain]
                 (js->clj (js/JSON.parse (.decode (js/TextDecoder.) plain))))))))
