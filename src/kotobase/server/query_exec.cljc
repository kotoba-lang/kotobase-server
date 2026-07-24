(ns kotobase.server.query-exec
  "Shared execution layer for the SPARQL/Cypher compiled-query shape
  (ADR-2607250100 axis 4 depth pass). A compiled query is:

    {:find      [?v ... | {:agg :count :var ?x :as ?c} ...]
     :where     [[s p o] ...]          ;; base BGP -> engine query
     :unions    [[[s p o] ...] ...]    ;; alternative BGPs -> solution union
     :optionals [[[s p o] ...] ...]    ;; OPTIONAL blocks -> LEFT JOIN
     :filters   [{:var ?v :op :< :value \"10\"} ...]
     :group-by  [?v ...]
     :order-by  [{:var ?v :dir :asc|:desc} ...]
     :limit     n}

  Either :where or :unions supplies the base solutions -- :unions is the
  bag union (SPARQL default semantics: duplicates kept) of each branch's
  solutions, with variables absent from a branch nil-filled.

  OPTIONAL, FILTER and aggregation run HERE, above the engine, as a
  correct-by-construction post-pass: OPTIONAL is a left outer join of a
  second engine query indexed by the shared variables (unmatched rows
  nil-fill); FILTER is a row predicate (numeric comparison when both
  sides parse as numbers, lexicographic otherwise -- values are stored
  as wire strings); aggregation is group-by + fold over rows. Engines
  like Neo4j/Datomic push these into their executors for efficiency --
  this layer trades that for correctness-first simplicity, and the cost
  is O(rows) per construct over the base result set, which callers
  should know when writing unbounded queries."
  (:require [clojure.string :as str]))

(defn- pattern-vars [patterns]
  (vec (distinct (filter symbol? (mapcat identity patterns)))))

(defn- rows->maps [vars rows]
  (mapv #(zipmap vars %) rows))

(defn- left-join
  "bind-maps LEFT JOIN the rows of one OPTIONAL block."
  [bind-maps opt-vars opt-rows]
  (let [bound-vars (when (seq bind-maps) (set (keys (first bind-maps))))
        join-vars (vec (filter (or bound-vars #{}) opt-vars))
        new-vars (vec (remove (set join-vars) opt-vars))
        opt-maps (rows->maps opt-vars opt-rows)
        idx (group-by #(mapv % join-vars) opt-maps)
        nil-fill (zipmap new-vars (repeat nil))]
    (vec (mapcat (fn [bm]
                   (let [matches (get idx (mapv bm join-vars))]
                     (if (seq matches)
                       (map #(merge bm (select-keys % new-vars)) matches)
                       [(merge bm nil-fill)])))
                 bind-maps))))

(defn- comparable-value [s]
  (if (nil? s)
    nil
    (let [n #?(:clj (try (Double/parseDouble s) (catch Exception _ nil))
               :cljs (let [x (js/parseFloat s)]
                       (when (and (not (js/isNaN x))
                                  (re-matches #"-?[0-9]+(?:\.[0-9]+)?" s))
                         x)))]
      (or n s))))

(defn- filter-pred [{:keys [var op value]}]
  (fn [bm]
    (let [l (comparable-value (get bm var))
          r (comparable-value value)]
      (cond
        (nil? l) false
        (and (number? l) (number? r))
        (case op := (== l r) :not= (not (== l r))
              :< (< l r) :<= (<= l r) :> (> l r) :>= (>= l r))
        :else
        (let [c (compare (str (get bm var)) (str value))]
          (case op := (zero? c) :not= (not (zero? c))
                :< (neg? c) :<= (not (pos? c)) :> (pos? c) :>= (not (neg? c))))))))

(defn- agg-fold [{:keys [agg var]} bms]
  (let [vals (keep #(get % var) bms)
        nums (keep #(let [v (comparable-value %)] (when (number? v) v)) vals)]
    (case agg
      :count (count vals)
      :sum (reduce + 0 nums)
      :min (when (seq nums) (reduce min nums))
      :max (when (seq nums) (reduce max nums))
      :avg (when (seq nums) (/ (reduce + 0.0 nums) (count nums))))))

(defn execute
  "engine-query: (fn [{:find [...] :where [...]}]) -> seq of tuples.
  Returns {:vars [...] :rows [[...]...]} after UNION/OPTIONAL/FILTER/
  aggregation/ORDER BY."
  [engine-query {:keys [find where unions optionals filters group-by order-by limit]}]
  (let [bind-maps
        (if (seq unions)
          (let [all-union-vars (vec (distinct (mapcat pattern-vars unions)))]
            (vec (mapcat (fn [branch]
                           (let [bvars (pattern-vars branch)
                                 nil-fill (zipmap (remove (set bvars) all-union-vars) (repeat nil))]
                             (map #(merge nil-fill %) (rows->maps bvars (engine-query {:find bvars :where branch})))))
                         unions)))
          (let [base-vars (pattern-vars where)]
            (rows->maps base-vars (engine-query {:find base-vars :where where}))))
        bind-maps (reduce (fn [bms opt-patterns]
                            (let [opt-vars (pattern-vars opt-patterns)
                                  opt-rows (engine-query {:find opt-vars :where opt-patterns})]
                              (left-join bms opt-vars opt-rows)))
                          bind-maps optionals)
        bind-maps (reduce (fn [bms f] (filterv (filter-pred f) bms)) bind-maps filters)
        aggregate? (some map? find)
        rows
        (cond
          aggregate?
          (let [groups (if (seq group-by)
                         (vals (clojure.core/group-by #(mapv % group-by) bind-maps))
                         [bind-maps])]
            (mapv (fn [g]
                    (mapv (fn [item]
                            (if (map? item)
                              (agg-fold item g)
                              (get (first g) item)))
                          find))
                  groups))
          :else (mapv (fn [bm] (mapv #(get bm %) find)) bind-maps))
        find-keys (mapv (fn [item] (if (map? item) (:as item) item)) find)
        rows (if (seq order-by)
               (let [idx-of (fn [v] (let [i (.indexOf find-keys v)]
                                      (when (neg? i) (throw (ex-info "ORDER BY var not projected" {:var v})))
                                      i))
                     specs (mapv (fn [{:keys [var dir]}] [(idx-of var) (or dir :asc)]) order-by)
                     keyfn (fn [row] (mapv (fn [[i _]] (comparable-value (nth row i))) specs))
                     cmp (fn [ka kb]
                           (loop [n 0]
                             (if (= n (count specs))
                               0
                               (let [[_ dir] (nth specs n)
                                     a (nth ka n) b (nth kb n)
                                     c (cond (and (number? a) (number? b)) (compare a b)
                                             :else (compare (str a) (str b)))
                                     c (if (= dir :desc) (- c) c)]
                                 (if (zero? c) (recur (inc n)) c)))))]
                 (vec (sort-by keyfn cmp rows)))
               rows)
        rows (if limit (vec (take limit rows)) (vec rows))]
    {:vars (mapv str find-keys)
     :rows rows}))
