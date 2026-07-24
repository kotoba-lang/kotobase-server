(ns kotobase.server.cypher
  "CYPHER BASIC SUBSET -> the same map-form Datalog `do-q` executes
  (ADR-2607172500's `graph.query` surface; ADR-2607250100 axis 4).
  Sibling of `kotobase.server.sparql`, same doctrine: every unsupported
  form is rejected loudly with the supported grammar in the error, never
  silently misread.

  Supported, and ONLY this:

    MATCH p1, p2, ...
    [WHERE v.attr = lit AND v.attr = lit ...]
    RETURN v1, v2, ...
    [LIMIT n]

    pattern := (v) | (v {attr: lit, ...}) | (v ...)-[:attr]->(v2 ...) chains
    lit     := \"string\" | 'string' | integer | decimal

  Attribute names are used as stored: a rel type `[:sp/knows]` matches the
  stored attribute string \":sp/knows\"; a property key `sp/name:` is
  prefixed to \":sp/name\" (the write path stores every attribute as its
  keyword-string form). A bare `(v)` with no properties and no
  relationship binds nothing and is rejected (never an implicit scan).

  Not supported (rejected): OPTIONAL MATCH, WHERE operators other than
  `=` joined by AND, OR/NOT, undirected or right-to-left relationships,
  variable-named relationships, anonymous nodes, node labels, property
  access in RETURN, ORDER BY/SKIP/DISTINCT, multi-hop `*`, and every
  write form (CREATE/MERGE/DELETE/SET -- this surface is read-only)."
  (:require [clojure.string :as str]))

(def grammar-help
  "supported: MATCH (a)-[:attr]->(b), (c {attr: \"v\"}) [WHERE a.attr = lit AND ...] RETURN a, b [LIMIT n]")

(defn- fail [msg near]
  (throw (ex-info (str "cypher-subset: " msg " (near: " (pr-str near) "). " grammar-help)
                  {:cypher-subset true})))

(def ^:private token-re
  #"\"[^\"]*\"|'[^']*'|-?[0-9]+(?:\.[0-9]+)?|<-|->|<>|<=|>=|<|>|\(|\)|\[|\]|\{|\}|,|:|=|\.|-|[A-Za-z_][A-Za-z0-9_/\-]*")

(defn- tokenize [s] (vec (re-seq token-re s)))

(defn- literal? [t]
  (or (str/starts-with? t "\"") (str/starts-with? t "'")
      (re-matches #"-?[0-9]+(?:\.[0-9]+)?" t)))

(defn- literal->value [t]
  (cond
    (str/starts-with? t "\"") (subs t 1 (dec (count t)))
    (str/starts-with? t "'") (subs t 1 (dec (count t)))
    :else t))

(defn- attr-name [t] (if (str/starts-with? t ":") t (str ":" t)))

(defn- ident? [t] (boolean (and t (re-matches #"[A-Za-z_][A-Za-z0-9_/\-]*" t))))

(defn- kw? [t kw] (and (ident? t) (= (str/upper-case t) kw)))

(def ^:private where-ops {"=" := "<>" :not= "<" :< "<=" :<= ">" :> ">=" :>=})
(def ^:private return-aggs #{"count" "sum" "min" "max" "avg"})

(defn- parse-props
  "After '{': attr : lit [, attr : lit]* '}' -> [rest-tokens clauses]."
  [ts v clauses]
  (loop [ts ts clauses clauses]
    (let [[k colon lit & more] ts]
      (when-not (ident? k) (fail "property key expected" k))
      (when-not (= ":" colon) (fail "property key needs ':'" k))
      (when-not (and lit (literal? lit)) (fail "property value must be a literal" lit))
      (let [clauses (conj clauses [v (attr-name k) (literal->value lit)])]
        (cond
          (= "," (first more)) (recur (rest more) clauses)
          (= "}" (first more)) [(rest more) clauses]
          :else (fail "property map needs ',' or '}'" (first more)))))))

(defn- parse-node
  "'(' var [{props}] ')' -> [rest-tokens var-sym clauses props?]."
  [ts clauses]
  (let [[open v & more] ts]
    (when-not (= "(" open) (fail "node pattern must open with (" open))
    (when-not (ident? v) (fail "node needs a variable name (anonymous nodes unsupported)" v))
    (let [sym (symbol (str "?" v))]
      (cond
        (= ")" (first more)) [(rest more) sym clauses false]
        (= "{" (first more))
        (let [[ts clauses] (parse-props (rest more) sym clauses)]
          (if (= ")" (first ts))
            [(rest ts) sym clauses true]
            (fail "node must close with )" (first ts))))
        :else (fail "node must close with ) or contain {props}" (first more))))))

(defn- parse-rel-chain
  "subj (-[:attr]-> node)* -> [rest-tokens clauses]."
  [ts subj clauses]
  (loop [ts ts subj subj clauses clauses]
    (cond
      (= "<-" (first ts)) (fail "right-to-left relationships unsupported" (first ts))
      (and (= "-" (first ts)) (= "[" (second ts)))
      (let [[_ _ colon attr close arrow & more] ts]
        (when-not (= ":" colon) (fail "relationship needs [:attr]" colon))
        (when-not (ident? attr) (fail "relationship type expected after [:" attr))
        (when-not (= "]" close) (fail "relationship must close with ]" close))
        (when-not (= "->" arrow) (fail "only left-to-right -[:attr]-> supported" arrow))
        (let [[ts obj clauses _] (parse-node (vec more) clauses)]
          (recur ts obj (conj clauses [subj (attr-name attr) obj]))))
      :else [ts clauses])))

(defn- parse-one-pattern [ts clauses]
  (let [[ts sym clauses props?] (parse-node ts clauses)
        [ts clauses] (parse-rel-chain ts sym clauses)]
    (when (and (not props?)
               (not (some (fn [[s _ o]] (or (= s sym) (= o sym))) clauses)))
      (fail "a bare (var) with no properties and no relationship binds nothing" (str sym)))
    [ts clauses]))

(defn parse
  "Cypher subset text -> {:find [?v ...] :where [[s p o] ...] :limit n?}.
  Throws ex-info {:cypher-subset true} on anything outside the subset."
  [text]
  (let [ts (tokenize (str text))]
    (when (empty? ts) (fail "empty query" ""))
    (when-not (kw? (first ts) "MATCH") (fail "must start with MATCH" (first ts)))
    (let [[ts clauses]
          (loop [ts (vec (rest ts)) clauses []]
            (let [[ts clauses] (parse-one-pattern ts clauses)]
              (if (= "," (first ts))
                (recur (vec (rest ts)) clauses)
                [ts clauses])))
          [ts clauses filters]
          (if (kw? (first ts) "WHERE")
            (loop [ts (rest ts) clauses clauses filters []]
              (let [[v dot k op lit & more] ts]
                (when-not (ident? v) (fail "WHERE needs var.attr op literal" v))
                (when-not (= "." dot) (fail "WHERE needs var.attr" dot))
                (when-not (ident? k) (fail "WHERE attribute expected" k))
                (when-not (contains? where-ops op) (fail "unsupported WHERE operator (=, <>, <, <=, >, >= )" op))
                (when-not (and lit (literal? lit)) (fail "WHERE value must be a literal" lit))
                (let [[clauses filters]
                      (if (= "=" op)
                        [(conj clauses [(symbol (str "?" v)) (attr-name k) (literal->value lit)]) filters]
                        ;; non-= comparisons: bind the attr to a derived var,
                        ;; filter as a post-pass (query-exec).
                        (let [fv (symbol (str "?" v "__" (str/replace k "/" "_")))]
                          [(conj clauses [(symbol (str "?" v)) (attr-name k) fv])
                           (conj filters {:var fv :op (get where-ops op) :value (literal->value lit)})]))]
                  (if (kw? (first more) "AND")
                    (recur (rest more) clauses filters)
                    [more clauses filters]))))
            [ts clauses []])
          _ (when-not (kw? (first ts) "RETURN") (fail "RETURN required" (first ts)))
          [ts find-vars]
          (loop [ts (rest ts) acc []]
            (cond
              (and (ident? (first ts)) (contains? return-aggs (str/lower-case (first ts)))
                   (= "(" (second ts)))
              (let [[agg _ v close & more] ts]
                (when-not (ident? v) (fail "aggregate needs a variable" v))
                (when-not (= ")" close) (fail "aggregate must close with )" close))
                (let [[alias more] (if (kw? (first more) "AS")
                                     (do (when-not (ident? (second more)) (fail "AS needs an alias name" (second more)))
                                         [(symbol (str "?" (second more))) (drop 2 more)])
                                     [(symbol (str "?" (str/lower-case agg) "_" v)) more])
                      acc (conj acc {:agg (keyword (str/lower-case agg)) :var (symbol (str "?" v)) :as alias})]
                  (if (= "," (first more))
                    (recur (vec (rest more)) acc)
                    [(vec more) acc])))
              (ident? (first ts))
              (let [acc (conj acc (symbol (str "?" (first ts))))]
                (if (= "," (second ts))
                  (recur (vec (drop 2 ts)) acc)
                  [(vec (rest ts)) acc]))
              :else (fail "RETURN needs variable names or agg(var) (property access unsupported)" (first ts))))
          limit
          (cond
            (empty? ts) nil
            (and (kw? (first ts) "LIMIT") (= 2 (count ts)) (re-matches #"[0-9]+" (second ts)))
            #?(:clj (Long/parseLong (second ts))
               :cljs (js/parseInt (second ts) 10))
            :else (fail "only LIMIT n allowed after RETURN" (first ts)))
          bound (set (mapcat (fn [[s _ o]] (filter symbol? [s o])) clauses))
          bare-vars (filterv symbol? find-vars)
          agg-items (filterv map? find-vars)
          ;; Cypher semantics: aggregates group implicitly by the bare
          ;; RETURN items.
          group-by' (when (seq agg-items) (vec bare-vars))]
      (when (empty? clauses) (fail "MATCH produced no bindable clauses" "MATCH"))
      (doseq [v bare-vars]
        (when-not (bound v) (fail "RETURN variable not bound in MATCH/WHERE" (str v))))
      (doseq [{:keys [var]} agg-items]
        (when-not (bound var) (fail "aggregate variable not bound in MATCH/WHERE" (str var))))
      (cond-> {:find (vec find-vars) :where (mapv vec clauses)}
        (seq filters) (assoc :filters filters)
        (seq group-by') (assoc :group-by group-by')
        limit (assoc :limit limit)))))
