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
  #"\"[^\"]*\"|'[^']*'|-?[0-9]+(?:\.[0-9]+)?|<-|->|\(|\)|\[|\]|\{|\}|,|:|=|\.|-|[A-Za-z_][A-Za-z0-9_/\-]*")

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
          [ts clauses]
          (if (kw? (first ts) "WHERE")
            (loop [ts (rest ts) clauses clauses]
              (let [[v dot k eq lit & more] ts]
                (when-not (ident? v) (fail "WHERE needs var.attr = literal" v))
                (when-not (= "." dot) (fail "WHERE needs var.attr" dot))
                (when-not (ident? k) (fail "WHERE attribute expected" k))
                (when-not (= "=" eq) (fail "only = comparisons supported in WHERE" eq))
                (when-not (and lit (literal? lit)) (fail "WHERE value must be a literal" lit))
                (let [clauses (conj clauses [(symbol (str "?" v)) (attr-name k) (literal->value lit)])]
                  (if (kw? (first more) "AND")
                    (recur (rest more) clauses)
                    [more clauses]))))
            [ts clauses])
          _ (when-not (kw? (first ts) "RETURN") (fail "RETURN required" (first ts)))
          [ts find-vars]
          (loop [ts (rest ts) acc []]
            (let [[v & more] ts]
              (when-not (ident? v) (fail "RETURN needs variable names (property access unsupported)" v))
              (let [acc (conj acc (symbol (str "?" v)))]
                (if (= "," (first more))
                  (recur (rest more) acc)
                  [more acc]))))
          limit
          (cond
            (empty? ts) nil
            (and (kw? (first ts) "LIMIT") (= 2 (count ts)) (re-matches #"[0-9]+" (second ts)))
            #?(:clj (Long/parseLong (second ts))
               :cljs (js/parseInt (second ts) 10))
            :else (fail "only LIMIT n allowed after RETURN" (first ts)))
          bound (set (mapcat (fn [[s _ o]] (filter symbol? [s o])) clauses))]
      (when (empty? clauses) (fail "MATCH produced no bindable clauses" "MATCH"))
      (doseq [v find-vars]
        (when-not (bound v) (fail "RETURN variable not bound in MATCH/WHERE" (str v))))
      (cond-> {:find (vec find-vars) :where (mapv vec clauses)}
        limit (assoc :limit limit)))))
