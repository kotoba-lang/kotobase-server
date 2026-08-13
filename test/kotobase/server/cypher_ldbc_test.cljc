(ns kotobase.server.cypher-ldbc-test
  "End-to-end for the 2026-08-13 Cypher surface work: labels, parameters,
  property projection, AS, ORDER BY on a projection, DISTINCT, undirected and
  right-to-left relationships, relationship variables, and variable-length
  paths.

  Every test parses real Cypher and EXECUTES it against a tiny datom set, so a
  construct that parses but is dropped on the way to the answer fails here. The
  parse-only probe that preceded this work could not have caught that."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [kotobase.server.cypher :as cypher]
            [kotobase.server.query-exec :as qe]))

;; person-1 --knows--> person-2 --knows--> person-3
;; post-10 <--replyOf-- comment-11 <--replyOf-- comment-12
(def datoms
  [["person-1" ":label" "Person"] ["person-1" ":id" "1"] ["person-1" ":firstName" "Ada"]
   ["person-2" ":label" "Person"] ["person-2" ":id" "2"] ["person-2" ":firstName" "Alan"]
   ["person-3" ":label" "Person"] ["person-3" ":id" "3"] ["person-3" ":firstName" "Grace"]
   ["city-9"   ":label" "City"]   ["city-9"   ":id" "9"]
   ["person-1" ":isLocatedIn" "city-9"]
   ["person-1" ":knows" "person-2"] ["person-2" ":knows" "person-3"]
   ["post-10"    ":label" "Post"]    ["post-10" ":id" "10"] ["post-10" ":content" "root"]
   ["comment-11" ":label" "Comment"] ["comment-11" ":id" "11"] ["comment-11" ":imageFile" "x.jpg"]
   ["comment-12" ":label" "Comment"] ["comment-12" ":id" "12"]
   ["comment-11" ":replyOf" "post-10"] ["comment-12" ":replyOf" "comment-11"]
   ;; forum-40 contains post-10 and is moderated by person-2 -- the shape LDBC
   ;; Interactive Short 6 walks to from a comment.
   ["forum-40" ":label" "Forum"] ["forum-40" ":id" "40"] ["forum-40" ":title" "Forum A"]
   ["forum-40" ":containerOf" "post-10"] ["forum-40" ":hasModerator" "person-2"]])

;; Minimal nested-loop BGP evaluator: enough to execute a compiled query, small
;; enough to be obviously correct.
(defn engine-query [{:keys [find where]}]
  (let [solve (fn solve [binds patterns]
                (if (empty? patterns)
                  [binds]
                  (let [[s p o] (first patterns)
                        resolve* (fn [t] (if (symbol? t) (get binds t ::unbound) t))
                        [sv pv ov] [(resolve* s) (resolve* p) (resolve* o)]]
                    (mapcat (fn [[ds dp do*]]
                              (when (and (or (= sv ::unbound) (= sv ds))
                                         (or (= pv ::unbound) (= pv dp))
                                         (or (= ov ::unbound) (= ov do*)))
                                (let [binds (cond-> binds
                                              (and (symbol? s) (= sv ::unbound)) (assoc s ds)
                                              (and (symbol? o) (= ov ::unbound)) (assoc o do*))]
                                  (solve binds (rest patterns)))))
                            datoms))))]
    (mapv (fn [b] (mapv #(get b %) find)) (solve {} where))))

(defn adjacency [attr node both?]
  (concat (keep (fn [[s p o]] (when (and (= s node) (= p attr)) o)) datoms)
          (when both? (keep (fn [[s p o]] (when (and (= o node) (= p attr)) s)) datoms))))

(defn run
  ([q] (run q nil))
  ([q params] (qe/execute engine-query (cypher/parse q params) adjacency)))

(defn rows-of [q & [params]] (set (:rows (run q params))))

(deftest labels-are-matched-not-skipped
  (testing "a label narrows the match"
    (is (= #{["Ada"]} (rows-of "MATCH (n:Person {id: \"1\"}) RETURN n.firstName"))))
  (testing "a label that no node carries returns nothing, rather than everything"
    (is (= #{} (rows-of "MATCH (n:Forum {id: \"1\"}) RETURN n.firstName")))))

(deftest parameters-are-values-and-a-missing-one-is-loud
  (is (= #{["Ada"]} (rows-of "MATCH (n:Person {id: $pid}) RETURN n.firstName" {"pid" "1"})))
  (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                        #"parameter has no value"
                        (cypher/parse "MATCH (n {id: $pid}) RETURN n" {}))))

(deftest projection-and-alias
  (is (= ["?firstName" "?cid"]
         (:vars (run "MATCH (n:Person {id: \"1\"})-[:isLocatedIn]->(c:City) RETURN n.firstName AS firstName, c.id AS cid"))))
  (is (= #{["Ada" "9"]}
         (rows-of "MATCH (n:Person {id: \"1\"})-[:isLocatedIn]->(c:City) RETURN n.firstName AS firstName, c.id AS cid"))))

;; Cypher returns null for an absent property; an inner join would drop the row.
(deftest a-missing-property-is-null-not-a-dropped-row
  (let [{:keys [rows]} (run "MATCH (n:Comment {id: \"12\"}) RETURN n.id, n.imageFile")]
    (is (= [["12" nil]] rows))))

(deftest order-by-a-projection-both-directions
  (is (= [["3"] ["2"] ["1"]]
         (:rows (run "MATCH (n:Person) RETURN n.id ORDER BY n.id DESC"))))
  (is (= [["1"] ["2"] ["3"]]
         (:rows (run "MATCH (n:Person) RETURN n.id ORDER BY n.id ASC")))))

(deftest distinct-removes-duplicate-rows
  (let [q "MATCH (a:Person)-[:knows]->(b:Person) RETURN a.label"]
    (is (< (count (:rows (run (str "MATCH (a:Person)-[:knows]->(b:Person) RETURN DISTINCT a.label"))))
           (inc (count (:rows (run q))))))
    (is (= [["Person"]]
           (:rows (run "MATCH (a:Person)-[:knows]->(b:Person) RETURN DISTINCT a.label"))))))

(deftest undirected-matches-both-ends
  (testing "directed sees one neighbour"
    (is (= #{["3"]} (rows-of "MATCH (n:Person {id: \"2\"})-[:knows]->(f) RETURN f.id"))))
  (testing "undirected sees both"
    (is (= #{["1"] ["3"]} (rows-of "MATCH (n:Person {id: \"2\"})-[:knows]-(f) RETURN f.id")))))

(deftest right-to-left-is-the-directed-inverse
  (is (= #{["1"]} (rows-of "MATCH (n:Person {id: \"2\"})<-[:knows]-(f) RETURN f.id"))))

(deftest variable-length-paths
  (testing "*0.. includes the start node itself"
    (is (= #{["12"] ["11"] ["10"]}
           (rows-of "MATCH (c:Comment {id: \"12\"})-[:replyOf*0..]->(m) RETURN m.id"))))
  (testing "*1.. excludes it"
    (is (= #{["11"] ["10"]}
           (rows-of "MATCH (c:Comment {id: \"12\"})-[:replyOf*1..]->(m) RETURN m.id"))))
  (testing "an explicit bound stops early"
    (is (= #{["11"]}
           (rows-of "MATCH (c:Comment {id: \"12\"})-[:replyOf*1..1]->(m) RETURN m.id"))))
  (testing "an unbounded walk over a cyclic relationship still terminates"
    (is (= #{["1"] ["2"] ["3"]}
           (rows-of "MATCH (n:Person {id: \"1\"})-[:knows*0..]-(f) RETURN f.id")))))

;; The guard that makes the whole change safe: a compiled query naming something
;; the executor cannot do must fail rather than be quietly approximated.
(deftest the-executor-refuses-what-it-cannot-do
  (testing "an unknown compiled key is refused"
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                          #"does not implement"
                          (qe/execute engine-query {:find '[?a] :where [] :some-future-thing true} adjacency))))
  (testing "a path query with no adjacency fn is refused, not answered zero-hop"
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                          #"no adjacency fn"
                          (qe/execute engine-query
                                      (cypher/parse "MATCH (n:Person {id: \"1\"})-[:knows*0..]->(f) RETURN f.id"))))))

(deftest ldbc-is1-runs-verbatim
  ;; The official LDBC IS1 text, only the label names mapped to this fixture.
  (let [q (str "MATCH (n:Person {id: $personId})-[:isLocatedIn]->(p:City)\n"
               "RETURN n.firstName AS firstName, n.id AS pid, p.id AS cityId")]
    (is (= #{["Ada" "1" "9"]} (rows-of q {"personId" "1"})))))

(deftest still-rejects-what-is-still-outside-the-subset
  ;; 2026-08-13 second pass: coalesce is now implemented, so it moved out of this
  ;; list and into its own execution test. WITH is still outside the subset.
  (doseq [[label q] [["WITH"            "MATCH (a {id: \"1\"})-[:knows]->(b) WITH b RETURN b"]
                     ["unknown function" "MATCH (a {id: \"1\"}) RETURN upper(a.x)"]
                     ["SKIP"            "MATCH (a {id: \"1\"}) RETURN a SKIP 2"]]]
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                 (cypher/parse q {}))
        (str label " must still be rejected loudly, not approximated"))))

;; ---------------------------------------------------------------- 2026-08-13, second pass

(deftest anonymous-nodes-still-constrain-the-match
  (testing "an anonymous labelled node narrows, it is not skipped"
    (is (= #{["2"]} (rows-of "MATCH (:Person {id: \"1\"})-[:knows]->(b) RETURN b.id")))
    ;; If the anonymous node were dropped, this would match every :knows edge.
    (is (= #{} (rows-of "MATCH (:Forum {id: \"1\"})-[:knows]->(b) RETURN b.id")))))

(deftest optional-match-is-a-left-join-not-a-filter
  (testing "a person with no city still comes back, with nil"
    (is (= #{["1" "9"] ["2" nil] ["3" nil]}
           (rows-of (str "MATCH (n:Person) OPTIONAL MATCH (n)-[:isLocatedIn]->(c:City) "
                         "RETURN n.id, c.id")))))
  (testing "an undirected or variable-length hop inside OPTIONAL is refused, not silently directed"
    (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
                          #"OPTIONAL MATCH cannot contain"
                          (cypher/parse "MATCH (n:Person) OPTIONAL MATCH (n)-[:knows]-(f) RETURN n.id")))))

(deftest coalesce-returns-the-first-non-null
  (testing "falls through to the second argument when the first is absent"
    (is (= #{["x.jpg"]}
           (rows-of "MATCH (m:Comment {id: \"11\"}) RETURN coalesce(m.content, m.imageFile) AS body"))))
  (testing "takes the first when it is present"
    (is (= #{["root"]}
           (rows-of "MATCH (m:Post {id: \"10\"}) RETURN coalesce(m.content, m.imageFile) AS body"))))
  (testing "both absent is null, not an error"
    (is (= [[nil]]
           (:rows (run "MATCH (m:Comment {id: \"12\"}) RETURN coalesce(m.content, m.imageFile) AS body"))))))

(deftest tointeger-orders-numerically-not-lexically
  ;; The reason the cast is recorded rather than dropped: as text, "10" sorts
  ;; before "9". LDBC IS 3 orders person ids numerically.
  (let [q "MATCH (n:Person) RETURN n.id AS pid ORDER BY toInteger(pid) ASC"]
    (is (= [["1"] ["2"] ["3"]] (:rows (run q)))))
  (testing "toInteger in RETURN yields a number"
    (is (= [[1]] (:rows (run "MATCH (n:Person {id: \"1\"}) RETURN toInteger(n.id) AS i")))))
  (testing "a non-numeric value casts to null rather than throwing"
    (is (= [[nil]] (:rows (run "MATCH (n:Person {id: \"1\"}) RETURN toInteger(n.firstName) AS i"))))))

(deftest an-unknown-function-is-rejected-at-parse-time
  (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs cljs.core.ExceptionInfo)
               (cypher/parse "MATCH (n:Person {id: \"1\"}) RETURN upper(n.firstName)" {}))))

;; ---------------------------------------------------------------- IS6 shape

(deftest a-path-that-connects-two-components-is-not-a-cross-product
  ;; LDBC Interactive Short 6's shape: the start node is pinned by id, and the
  ;; ONLY thing linking it to the forum/moderator half of the pattern is a
  ;; variable-length path. Solving the whole BGP before expanding the path makes
  ;; those halves a cross product.
  ;;
  ;; Measured 2026-08-13 on LDBC SF-0.1 before this was interleaved: IS6 returned
  ;; 135,701 rows for a query whose correct answer is one row, and spent 8.2 s
  ;; doing it. A wrong answer with a plausible latency next to it is worse than
  ;; a slow one.
  (let [q (str "MATCH (c:Comment {id: \"12\"})-[:replyOf*0..]->(m:Post), "
               "(f:Forum)-[:containerOf]->(m) "
               "RETURN m.id, f.id")]
    (is (= 1 (count (:rows (run q))))
        "one comment, one root post, one forum -- one row")
    (is (= #{["10" "40"]} (rows-of q)))))

(deftest a-genuinely-disconnected-pattern-is-still-a-cross-product
  ;; The interleaving must not silently drop rows from a query that really is
  ;; disconnected: 3 persons x 2 messages is 6, and that is the right answer.
  (let [rows (:rows (run "MATCH (p:Person), (m:Comment) RETURN p.id, m.id"))]
    (is (= 3 (count (distinct (map first rows)))))
    (is (= 2 (count (distinct (map second rows)))))
    (is (= 6 (count rows)))))
