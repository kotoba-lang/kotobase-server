(ns kotobase.server.sparql-equivalence-test
  "The only evidence that would justify switching `graph.sparql` over: the
  same query text, through both implementations, answering the same thing.

  Not a unit test of either. Each has its own tests and both pass; what no
  test of either alone can catch is the two DISAGREEING — and every failure
  mode found so far in this migration has been of that kind. A bare
  `<:sp/name>` that means one attribute here and a wildcard there. A literal
  that is `30` on one side and `\"30\"` on the other. A `visible?` predicate
  of the wrong shape that filters nothing while looking like it filters.
  None of those raise; they answer differently."
  (:require #?(:clj  [clojure.test :refer [deftest is testing]]
               :cljs [cljs.test :refer [deftest is testing async] :include-macros true])
            [kotobase.server.handler :as h]
            [kotobase.server.sparql-protocol :as spp]))

(defn- passthrough [x] #?(:clj x :cljs (js/Promise.resolve x)))

(defn- mem-store []
  (let [blocks (atom {}) heads (atom {})]
    {:get-fn (fn [cid] (get @blocks cid))
     :put! (fn [cid bytes] (swap! blocks assoc cid bytes))
     :head-get (fn [graph] (get @heads graph))
     :head-put! (fn [graph chain] (swap! heads assoc graph chain))
     :blind-fn passthrough
     :encrypt-fn passthrough
     :decrypt-fn passthrough}))

(defn- then* [x f] #?(:clj (f x) :cljs (.then (js/Promise.resolve x) f)))

(defn- run [steps]
  #?(:clj (doseq [s steps] (s))
     :cljs (async done
             (-> (reduce (fn [p s] (.then p (fn [_] (s)))) (js/Promise.resolve nil) steps)
                 (.then (fn [_] (done)))
                 (.catch (fn [e] (is false (str "unexpected rejection: " e)) (done)))))))

(def ^:private tx
  (str "[{:db/id \"e1\" :sp/name \"alice\" :sp/age 30}"
       " {:db/id \"e2\" :sp/name \"bob\"}]"))

(def ^:private both-should-answer
  "Queries inside the OLD subset's grammar — the ones a swap must not change.
  Written in the `<:attr>` spelling every existing caller uses, which is the
  whole point (org-w3-sparql-protocol#8)."
  ["SELECT ?e WHERE { ?e <:sp/name> \"alice\" }"
   "SELECT ?e ?n WHERE { ?e <:sp/name> ?n }"
   "SELECT * WHERE { ?e <:sp/age> ?v }"
   "SELECT ?n WHERE { ?e <:sp/name> ?n . ?e <:sp/age> 30 }"
   ;; OPTIONAL is IN the old grammar (its own grammar-help lists it); the
   ;; existing handler test rejects `SELECT ?e WHERE { OPTIONAL {...} }` for
   ;; an unbound projection var, not for OPTIONAL itself. Assuming otherwise
   ;; is how a \"the new one does more\" claim gets written without checking.
   "SELECT ?e ?a WHERE { ?e <:sp/name> ?n . OPTIONAL { ?e <:sp/age> ?a } }"

   ;; ── the rest of the old grammar. Its own `grammar-help` names UNION,
   ;; GROUP BY, ORDER BY, LIMIT and COUNT/SUM/MIN/MAX/AVG, and a swap has to
   ;; hold for all of it, not just the shapes that were easy to check first.
   "SELECT ?e WHERE { { ?e <:sp/name> \"alice\" } UNION { ?e <:sp/name> \"bob\" } }"
   "SELECT ?e ?n WHERE { ?e <:sp/name> ?n } ORDER BY ?n"
   "SELECT ?e ?n WHERE { ?e <:sp/name> ?n } ORDER BY DESC(?n)"
   "SELECT ?e ?n WHERE { ?e <:sp/name> ?n } LIMIT 1"
   ;; aggregates — the gap that blocked the swap, closed by
   ;; kotoba-lang/sparql#4 (the :group node) and
   ;; org-w3-sparql-protocol#9 (parsing it). They are equivalence cases now,
   ;; which is the whole point: the coverage is nested at last.
   "SELECT (COUNT(?e) AS ?c) WHERE { ?e <:sp/name> ?n }"
   "SELECT ?n (COUNT(?e) AS ?c) WHERE { ?e <:sp/name> ?n } GROUP BY ?n"])

(defn- ordered?
  "Does this query pin its own row order?"
  [q] (boolean (re-find #"(?i)ORDER\s+BY" q)))

(defn- normalize
  "Rows keyed by var name. A SET unless the query said ORDER BY — neither
  implementation promises order without one, so comparing vectors there
  would fail on a difference SPARQL itself says is not one. WITH an ORDER
  BY, order is the thing under test and a set would hide a disagreement
  about it."
  [ordered? {:keys [vars rows]}]
  (let [as-maps (map (fn [row] (zipmap vars row)) rows)]
    {:vars (set vars)
     :rows (if ordered? (vec as-maps) (set as-maps))}))

(deftest both-implementations-answer-the-same-rows
  (let [store (mem-store)]
    (run
     (into [(fn [] (then* (h/handle store "transact" {:graph "eq" :tx_edn tx} "did:key:ztest")
                          (fn [r] (is (:ok r)))))]
           (map (fn [q]
                  (fn []
                    (then* (h/handle store "sparql" {:graph "eq" :sparql q} nil)
                           (fn [old]
                             (then* (spp/do-sparql store {:graph "eq" :sparql q} nil)
                                    (fn [new']
                                      (testing q
                                        (is (:ok old) "the subset answers this query")
                                        (is (:ok new') "so does the protocol path")
                                        (is (= (normalize (ordered? q) old)
                                               (normalize (ordered? q) new'))
                                            "same vars, same rows"))))))))
                both-should-answer)))))

(deftest a-query-naming-an-unknown-attribute-answers-nothing
  (testing "not everything. A nil pattern position is a WILDCARD, and this is
            where that bug hid before org-w3-sparql-protocol#8"
    (let [store (mem-store)]
      (run
       [(fn [] (then* (h/handle store "transact" {:graph "eq3" :tx_edn tx} "did:key:ztest")
                      (fn [r] (is (:ok r)))))
        (fn []
          (then* (spp/do-sparql store {:graph "eq3" :sparql "SELECT ?e WHERE { ?e <http://example.org/nope> ?v }"} nil)
                 (fn [r]
                   (is (:ok r))
                   (is (= [] (:rows r))))))]))))

(deftest a-parse-failure-is-refused-the-same-way-the-subset-refuses
  (let [store (mem-store)]
    (run
     [(fn []
        (then* (spp/do-sparql store {:graph "eq4" :sparql "SELEC ?e WHERE"} nil)
               (fn [r]
                 (is (false? (:ok r)))
                 (is (= "UnsupportedSparql" (:error r)))
                 (is (string? (:message r))))))])))

(deftest ask-is-refused-rather-than-flattened
  (testing "graph.sparql returns {:vars :rows}; a boolean is not that shape,
            and answering one anyway is how a surface starts lying"
    (let [store (mem-store)]
      (run
       [(fn []
          (then* (spp/do-sparql store {:graph "eq5" :sparql "ASK WHERE { ?e <:sp/name> ?n }"} nil)
                 (fn [r]
                   (is (false? (:ok r)))
                   (is (= "UnsupportedSparql" (:error r)))
                   (is (re-find #"SELECT only" (:message r))))))]))))
