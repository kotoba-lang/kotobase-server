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

  PUSH-DOWN (2026-07-25): constructs the engine (arrangement.datalog)
  can evaluate NATIVELY are compiled into the single engine query instead
  of run as a post-pass -- closing part of the maturity-bar axis-1
  \"engine push-down\" gap. Pushable, and only when NO OPTIONAL is
  present (OPTIONAL adds bindings the engine query doesn't see):
    - COUNT / COUNT-DISTINCT aggregates -> engine `:find (count ?v)` with
      GROUP BY = the bare find vars (engine computes the count during the
      join; the handler never materializes the full row set);
    - `=` / `!=` FILTER -> engine `[(= ?v lit)]` / `[(not= ?v lit)]`
      predicate clauses (string equality, safe on wire-string values).
  What STAYS a post-pass, with reason: OPTIONAL (engine has no left join),
  numeric FILTER `< <= > >=` and SUM/AVG/MIN/MAX (engine's `<`/`+` need
  numbers; values are wire strings the engine can't coerce), UNION
  (multi-clause or-join branch support not relied on here), ORDER BY
  (engine returns a set). The pushable path and the post-pass path are
  asserted to agree by the same test suite (identical results either way)."
  (:require [clojure.string :as str]))

(declare execute-post-pass execute-pushed)

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

(defn- order-and-limit [rows find-keys order-by limit]
  (let [rows
        (if (seq order-by)
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
                            (if (zero? c) (recur (inc n) ) c)))))]
            (vec (sort-by keyfn cmp rows)))
          (vec rows))]
    (if limit (vec (take limit rows)) rows)))

(defn- pushable?
  "True iff the whole query can be answered by ONE engine query: no
  OPTIONAL, every filter =/!=, every aggregate count/count-distinct, and
  (when aggregating) GROUP BY exactly the bare find vars -- the engine's
  implicit group-by is by the non-aggregate find columns, so a GROUP BY
  that differs must stay a post-pass."
  [{:keys [optionals filters find group-by]}]
  (and (empty? optionals)
       (every? #(#{:= :not=} (:op %)) filters)
       (every? (fn [item] (or (symbol? item) (#{:count :count-distinct} (:agg item)))) find)
       (let [aggregating? (some map? find)
             bare (set (filter symbol? find))]
         (or (not aggregating?)
             (= (set group-by) bare)
             (and (empty? group-by) (empty? bare))))))

(defn- filter->clause [{:keys [var op value]}]
  ;; engine query-fns whitelist: '= and 'not= compare with clojure =/not=,
  ;; correct for wire-string values.
  [(list (if (= op :=) '= 'not=) var value)])

(defn- find->engine-find [item]
  (if (map? item)
    (list (if (= (:agg item) :count) 'count 'count-distinct) (:var item))
    item))

(defn- execute-pushed
  "Single-engine-query path (see `pushable?`). Aggregates + =/!= filters go
  INTO the engine query; only ORDER BY / LIMIT remain as a post-pass over
  the (already grouped/counted) result."
  [engine-query {:keys [find where filters order-by limit]}]
  (let [engine-find (mapv find->engine-find find)
        clauses (into (vec where) (map filter->clause filters))
        rows (engine-query {:find engine-find :where clauses})
        find-keys (mapv (fn [item] (if (map? item) (:as item) item)) find)
        rows (order-and-limit rows find-keys order-by limit)]
    {:vars (mapv str find-keys) :rows rows}))

(defn execute
  "engine-query: (fn [{:find [...] :where [...]}]) -> seq of tuples.
  Returns {:vars [...] :rows [[...]...]} after UNION/OPTIONAL/FILTER/
  aggregation/ORDER BY."
  [engine-query {:keys [find where unions optionals filters group-by order-by limit] :as compiled}]
  (if (and (pushable? compiled) (empty? unions))
    (execute-pushed engine-query compiled)
    (execute-post-pass engine-query compiled)))

(defn- execute-post-pass
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
        rows (order-and-limit rows find-keys order-by limit)]
    {:vars (mapv str find-keys)
     :rows rows}))
