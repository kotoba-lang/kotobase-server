(ns kotobase.server.ipns-test
  "kotobase.server.ipns tests -- ClojureScript only, mirroring kotobase.ipns's
  own documented exception (npm Ed25519 crypto via @noble/curves, no JVM
  path). Uses kotobase.ipns/sign-head (kotoba-lang/kotobase-client) to
  produce genuinely valid signed records rather than hand-rolled fixtures,
  so a signature-verification regression in either namespace would be
  caught here too."
  (:require [cljs.test :refer-macros [deftest is testing]]
            [kotobase.ipns :as ipns]
            [kotobase.server.ipns :as sipns]))

(def seed (js/Uint8Array.from (into-array (range 32))))

(defn- signed [sequence]
  (ipns/sign-head seed {:name "k51test" :value "bafyreicid-example"
                         :sequence sequence :valid_until "2027-01-01T00:00:00Z"}))

(deftest head-response-formats-a-found-and-a-missing-record
  (testing "nil record -> 404 NotFound"
    (is (= {:ok false :error "NotFound" :status 404} (sipns/head-response nil))))
  (testing "a present record -> merged with :ok true :status 200"
    (let [record {:name "k51test" :value "bafyreicid-example" :sequence 3}]
      (is (= (assoc record :ok true :status 200) (sipns/head-response record))))))

(deftest verify-and-decide-publish-accepts-a-first-publish
  (is (= {:ok true} (sipns/verify-and-decide-publish (signed 1) nil))
      "no existing record -- any valid sequence is accepted"))

(deftest verify-and-decide-publish-accepts-a-sequence-advance
  (is (= {:ok true} (sipns/verify-and-decide-publish (signed 5) (signed 4)))
      "5 > 4 -- a genuine advance is accepted"))

(deftest verify-and-decide-publish-rejects-a-sequence-rollback
  (is (= {:ok false :error "SequenceRollback" :status 409}
         (sipns/verify-and-decide-publish (signed 3) (signed 4)))
      "3 <= 4 -- strictly lower sequence is rejected"))

(deftest verify-and-decide-publish-rejects-a-repeated-sequence
  (is (= {:ok false :error "SequenceRollback" :status 409}
         (sipns/verify-and-decide-publish (signed 4) (signed 4)))
      "4 <= 4 -- replaying the SAME sequence is also rejected (CAS boundary, not just strict rollback)"))

(deftest verify-and-decide-publish-rejects-an-invalid-signature
  (let [tampered (assoc (signed 1) :sequence 999)]
    (is (= {:ok false :error "InvalidSignature" :status 401}
           (sipns/verify-and-decide-publish tampered nil))
        "the signed payload no longer matches the tampered :sequence field")))

(deftest verify-and-decide-publish-checks-signature-before-sequence
  (testing "an invalid signature is rejected even when the sequence WOULD also
            be a rollback -- signature validity is checked first"
    (let [tampered (assoc (signed 1) :sequence 999)]
      (is (= {:ok false :error "InvalidSignature" :status 401}
             (sipns/verify-and-decide-publish tampered (signed 4)))))))

(deftest verify-and-decide-publish-rejects-a-non-numeric-body-sequence
  (testing "a validly-signed record with a non-numeric :sequence (e.g. a
            string) must NOT reach the <= rollback comparison -- under
            cljs, a non-numeric operand coerces to NaN, and NaN <= anything
            is false, so a naive comparison would silently let this
            through as 'not a rollback' instead of rejecting it"
    (is (= {:ok false :error "InvalidSequence" :status 400}
           (sipns/verify-and-decide-publish (signed "abc") nil))
        "no existing record -- still rejected on the body's own bad :sequence")
    (is (= {:ok false :error "InvalidSequence" :status 400}
           (sipns/verify-and-decide-publish (signed "abc") (signed 999)))
        "existing record has a much higher sequence -- would have silently
         passed as 'no rollback' under NaN <= 999 without the guard")))

(deftest verify-and-decide-publish-rejects-when-current-has-a-corrupted-sequence
  (testing "if a previously-accepted record already has a non-numeric
            :sequence (e.g. from before this guard existed), any further
            publish is rejected rather than silently comparing against
            NaN forever -- fails closed instead of permanently disabling
            the rollback guard for this name"
    (is (= {:ok false :error "InvalidSequence" :status 400}
           (sipns/verify-and-decide-publish (signed 1) (signed "abc"))))))
