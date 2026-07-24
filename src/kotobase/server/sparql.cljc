(ns kotobase.server.sparql
  "SPARQL BASIC SUBSET -> the same map-form Datalog `do-q` executes
  (ADR-2607172500's `graph.sparql` surface; ADR-2607250100 axis 4).

  Supported, and ONLY this -- an HONEST SUBSET, same doctrine as the
  handler's other Datomic-cloud approximations (each unsupported form is
  rejected loudly with the supported grammar in the error, never silently
  misread):

    SELECT ?v1 ?v2 ... | SELECT *
    WHERE { t1 t2 t3 . t4 t5 t6 . ... }   (basic graph patterns only)
    LIMIT n                                (optional)

    term := ?var | <iri> | \"string\" | 'string' | integer | decimal

  Not supported (rejected): PREFIX/BASE, OPTIONAL, FILTER, UNION, GRAPH,
  ORDER/GROUP BY, DISTINCT, property paths, blank nodes, language tags,
  datatyped literals (^^), CONSTRUCT/ASK/DESCRIBE, ; and , abbreviations.

  Literal representation matches the write path's own stringification
  (`tx-edn->quads` stores every non-Link position as a string): <iri> and
  quoted strings compile to their string content, numbers to their decimal
  string -- the exact coercion `normalize-query-literals` applies to
  Datalog literals, applied here at compile time."
  (:require [clojure.string :as str]))

(def grammar-help
  "supported: SELECT ?v ... | SELECT * WHERE { s p o . s p o } [LIMIT n]; terms: ?var, <iri>, \"string\", 'string', number")

(defn- fail [msg near]
  (throw (ex-info (str "sparql-subset: " msg " (near: " (pr-str near) "). " grammar-help)
                  {:sparql-subset true})))

(def ^:private token-re
  ;; longest-match-first: iri, quoted strings, vars, numbers, braces, dot,
  ;; bare words (kept only to produce a good error message).
  #"<[^>\s]*>|\"[^\"]*\"|'[^']*'|\?[A-Za-z_][A-Za-z0-9_]*|-?[0-9]+(?:\.[0-9]+)?|\{|\}|\.|[^\s{}]+")

(defn- tokenize [s] (vec (re-seq token-re s)))

(defn- variable? [t] (str/starts-with? t "?"))

(defn- term->value
  "Compile one non-variable term to its stored-string representation."
  [t]
  (cond
    (and (str/starts-with? t "<") (str/ends-with? t ">")) (subs t 1 (dec (count t)))
    (and (str/starts-with? t "\"") (str/ends-with? t "\"")) (subs t 1 (dec (count t)))
    (and (str/starts-with? t "'") (str/ends-with? t "'")) (subs t 1 (dec (count t)))
    (re-matches #"-?[0-9]+(?:\.[0-9]+)?" t) t
    :else (fail "unsupported term" t)))

(defn- term->pattern [t]
  (if (variable? t) (symbol t) (term->value t)))

(defn parse
  "SPARQL subset text -> {:find [?v ...] :where [[s p o] ...] :limit n?}.
  Throws ex-info {:sparql-subset true} with the supported grammar on
  anything outside the subset."
  [text]
  (let [tokens (tokenize (str text))
        upper (fn [t] (str/upper-case t))]
    (when (empty? tokens) (fail "empty query" ""))
    (when-not (= "SELECT" (upper (first tokens))) (fail "must start with SELECT" (first tokens)))
    (let [[vars rest-tokens]
          (loop [ts (rest tokens) acc []]
            (cond
              (empty? ts) (fail "missing WHERE" "end of query")
              (= "WHERE" (upper (first ts))) [acc (rest ts)]
              (= "*" (first ts)) (recur (rest ts) (conj acc :*))
              (variable? (first ts)) (recur (rest ts) (conj acc (symbol (first ts))))
              :else (fail "only ?vars or * allowed in SELECT" (first ts))))
          _ (when (empty? vars) (fail "SELECT needs at least one ?var or *" "WHERE"))
          _ (when-not (= "{" (first rest-tokens)) (fail "WHERE must open with {" (first rest-tokens)))
          [patterns after]
          (loop [ts (rest rest-tokens) current [] acc []]
            (cond
              (empty? ts) (fail "WHERE never closed with }" "end of query")
              (= "}" (first ts))
              (let [acc (if (seq current)
                          (if (= 3 (count current)) (conj acc current) (fail "pattern must have exactly 3 terms" current))
                          acc)]
                [acc (rest ts)])
              (= "." (first ts))
              (if (= 3 (count current))
                (recur (rest ts) [] (conj acc current))
                (fail "pattern must have exactly 3 terms" current))
              :else
              (if (= 3 (count current))
                (fail "pattern must have exactly 3 terms (missing '.'?)" (first ts))
                (recur (rest ts) (conj current (term->pattern (first ts))) acc))))
          _ (when (empty? patterns) (fail "WHERE needs at least one triple pattern" "{}"))
          limit (cond
                  (empty? after) nil
                  (and (= "LIMIT" (upper (first after)))
                       (= 2 (count after))
                       (re-matches #"[0-9]+" (second after)))
                  #?(:clj (Long/parseLong (second after))
                     :cljs (js/parseInt (second after) 10))
                  :else (fail "only LIMIT n allowed after }" (first after)))
          all-vars (vec (distinct (filter symbol? (mapcat identity patterns))))
          find-vars (if (some #{:*} vars) all-vars (vec vars))]
      (doseq [v find-vars]
        (when-not (some #{v} all-vars)
          (fail "SELECT variable not bound in WHERE" (str v))))
      (cond-> {:find find-vars
               :where (mapv vec patterns)}
        limit (assoc :limit limit)))))
