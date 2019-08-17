(ns db.q
  (:require [helper.core :as helper]
            [clojure.string :as string]
            [db.pull :as pull]))


(defn op? [val]
  (let [ops #{"select" "from" "update" "set" "insert"
              "delete" "pull" "join" "left-join" "right-join"
              "left-outer-join" "right-outer-join" "outer-join"
              "full-outer-join" "full-join" "cross-join"
              "where" "order" "order-by" "limit" "offset" "group" "group-by" "values"
              "on-conflict" "do-update-set"
              "returning"}]
    (and (ident? val)
      (contains? ops (name val)))))


(defn sql-map [v]
  (when (some? v)
    (let [parts (partition-by op? v)
          ops (take-nth 2 parts)
          args (filter #(not (contains? (set ops) %)) parts)]
      (zipmap (map first ops) (map vec args)))))


(defn select-fn [coll]
  (format "%s(%s)" (helper/sqlize
                    (first coll))
    (->> (rest coll)
         (map helper/sqlize)
         (string/join ", "))))


(defn select-val [val]
  (cond
    (ident? val) (helper/sqlize val)
    (list? val) (select-fn val)
    :else val))


(defn select [v]
  {:select
   (string/replace
     (->> (map select-val v)
          (string/join ", ")
          (format "select %s"))
     #",\sas,\s" " as ")})


(defn from [v]
  {:from (->> (map helper/sqlize v)
              (string/join ", ")
              (format "from %s"))})


(defn join-statement [join-name left right]
    (format "%s %s on %s.%s = %s.%s" join-name (name left) (name left) (str (name right) "_id") (name right) "id"))


(defn join [ctx join-name v]
  (let [from (helper/sqlize (get-in ctx [:query :from]))]
    {(keyword (string/replace join-name " " "-")) (->> (map #(join-statement join-name (helper/sqlize %) from) v)
                                                       (string/join " "))}))


(defn q-ident? [val]
  (and (ident? val)
    (-> val name (string/starts-with? "?"))))


(defn name-or-value [val]
  (cond
    (q-ident? val) (-> val name (string/replace #"\?" "") keyword)
    (ident? val) nil
    :else val))


(defn where-args [{:keys [params]} v]
  (filter #(and (some? %) (not (ident? %)))
    (flatten
      (map #(get params (name-or-value %) (name-or-value %))
        v))))


(defn where [ctx v]
  (if (vector? (first v))
    {:where (str "where " (ffirst v))
     :args (rest (first v))}
    (let [sql (string/join " "
                (map #(cond
                        (and (q-ident? %)
                          (sequential?
                           (get (:params ctx) (name-or-value %))))
                        (format "(%s)"
                          (string/join ", "
                           (map (fn [_] "?") (get (:params ctx) (name-or-value %)))))

                        (q-ident? %) "?"
                        (ident? %) (helper/sqlize %)
                        :else "?")
                  v))
          args (where-args ctx v)]
      {:where (str "where " sql)
       :args args})))


(defn limit [[i]]
  (when (pos-int? i)
    {:limit (str "limit " i)}))


(defn offset [[i]]
  (when (not (neg-int? i)) ; need to handle zero
    {:offset (str "offset " i)}))


(defn group [v]
  {:group-by (str "group by " (->> (map helper/sqlize v)
                                   (string/join ", ")))})


(defn order [v]
  {:order-by (str "order by " (->> (partition-all 2 v)
                                   (mapv vec)
                                   (mapv #(if (= 1 (count %))
                                            (conj % 'asc)
                                            %))
                                   (mapv #(str (helper/sqlize (first %)) " " (name (second %))))
                                   (string/join ", ")))})


(defn sql-part [ctx [k v]]
  (condp = k
    :select (select v)
    :from (from v)
    :pull {:pull v}
    :join (join ctx "join" v)
    :cross-join (join ctx "cross join" v)
    :left-join (join ctx "left join" v)
    :right-join (join ctx "right join" v)
    :outer-join (join ctx "outer join" v)
    :full-join (join ctx "full join" v)
    :left-outer-join (join ctx "left outer join" v)
    :right-outer-join (join ctx "right outer join" v)
    :where (where ctx v)
    :order (order v)
    :order-by (order v)
    :limit (limit v)
    :offset (offset v)
    :group (group v)
    :group-by (group v)
    nil))


(defn sql-vec
  "Generates a jdbc sql vector from an ident sql vector"
  [ctx v params]
  (let [ctx (assoc ctx :query v :params params)
        m (->> (sql-map v)
               (pull/fmt-pull)
               (pull/expand-pull-asterisk ctx {})
               (map #(sql-part ctx %))
               (apply merge)
               (pull/pull ctx))
        {:keys [select join left-join right-join
                left-outer-join right-outer-join full-join
                full-join cross-join full-outer-join
                where args order-by offset
                limit group-by from]} m
        sql (->> (filter some? [select from
                                join left-join right-join
                                left-outer-join right-outer-join
                                full-join cross-join full-outer-join
                                where group-by order-by offset limit])
                 (string/join " "))]
    (apply conj [sql] args)))
