(ns db.q
  (:require [helper.core :as helper]
            [clojure.string :as string]))


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
  {:select (->> (map select-val v)
                (string/join ", ")
                (format "select %s"))})


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


(defn sql-part [ctx [k v]]
  (condp = k
    :select (select v)
    :from (from v)
    ; :pull {:pull v}
    :join (join ctx "join" v)
    :cross-join (join ctx "cross join" v)
    :left-join (join ctx "left join" v)
    :right-join (join ctx "right join" v)
    :outer-join (join ctx "outer join" v)
    :full-join (join ctx "full join" v)
    :left-outer-join (join ctx "left outer join" v)
    :right-outer-join (join ctx "right outer join" v)
    ; :where (where v)
    ; :order (order v)
    ; :limit (limit v)
    ; :offset (offset v)
    ; :group (group v)
    ; :group-by (group v)
    ; :delete (delete v)
    ; :values (values v)
    ; :insert (insert v)
    ; :update (update v)
    ; :set (update-set v)
    ; :do-update-set (do-update-set v)
    ; :on-conflict (on-conflict v)
    nil))
