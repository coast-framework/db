(ns db.pull
  (:require [clojure.string :as string]
            [helper.core :as helper]))

(defn col [k]
  (-> k name helper/snake-case))


(defn table [k]
  (-> k namespace helper/snake-case))


(defn select-col [k]
  (let [table (table k)
        col (col k)]
    (format "%s.%s as %s$%s" table col table col)))


(defn op? [k]
  (contains? #{:select :pull :joins :where :order :order-by :limit :offset :group :group-by} k))

(defn sql-map [v]
  (let [parts (partition-by op? v)
        ops (take-nth 2 parts)
        args (filter #(not (contains? (set ops) %)) parts)]
    (zipmap (map first ops) (map vec args))))


(defn rel-opts [m]
  (let [k (-> m keys first)]
    (if (sequential? k)
      (sql-map (drop 1 k))
      {})))


(defn pull-limit [[i]]
  (when (pos-int? i)
    {:limit (str "where rn <= " i)}))


(defn order [v]
  {:order (str "order by " (->> (partition-all 2 v)
                                (mapv vec)
                                (mapv #(if (= 1 (count %))
                                         (conj % 'asc)
                                         %))
                                (mapv #(str (helper/sqlize (first %)) " " (name (second %))))
                                (string/join ", ")))})


(defn pull-sql-part [[k v]]
  (condp = k
    :order-by (order v)
    :order (order v)
    :limit (pull-limit v)
    :else {}))


(defn join-statement [{:keys [table left right]}]
  (str (helper/sqlize table)
       " on "
       (helper/sqlize left)
       " = "
       (helper/sqlize right)))


(defn pull-from [pf-joins o table]
  (let [[first-join second-join] pf-joins
        order-by-id (or (:left second-join) "id")
        order (or o (str "order by " order-by-id))
        select (->> (map :table pf-joins)
                    (map #(str % ".*,"))
                    (string/join "\n"))
        join (if (some? second-join)
               (str "\n          join " (join-statement (merge second-join {:table (:table first-join)})))
               "")]
    (format " (
          select
            %s
            row_number() over (%s) as rn
          from %s%s
        ) as %s"
      select order table join table)))


(defn rel-key [m]
  (let [k (-> m keys first)]
    (if (sequential? k)
      (first k)
      k)))


(defn pull-col [k]
  (str (-> k namespace helper/sqlize) "$" (-> k name helper/sqlize)))


(defn json-build-object [k]
  (str "'" (pull-col k) "', " (helper/sqlize k)))


(defn pull-op? [k]
  (or (contains? #{:limit :order} k)
      (contains? #{'limit 'order} k)))


(defn pull-sql-ops [v]
  (let [parts (partition-by pull-op? v)
        ops (take-nth 2 parts)
        args (filter #(not (contains? (set ops) %)) parts)]
    (zipmap (map first ops) (map vec args))))


(defn rel-col [k]
  (if (qualified-ident? k)
    (str "'" (pull-col k) "', " (pull-col k))
    (str "'" (-> k first pull-col) "', " (-> k first pull-col))))


(def pull-sql-map {"sqlite" {:json-agg "json_group_array"
                             :json-object "json_object"}
                   "postgres" {:json-agg "json_agg"
                               :json-object "json_build_object"}})


(defn pull-join [adapter associations m]
  (let [k (rel-key m)
        association (get associations k)
        {:keys [from col]} association
        pull-join-map (get-in association [:joins 0])
        pf-joins (get association :joins)
        {:keys [order limit]} (->> (rel-opts m)
                                   (map pull-sql-part)
                                   (apply merge))
        val (-> m vals first)
        v (filter qualified-ident? val)
        maps (filter map? val)
        child-cols (mapv #(-> % keys first rel-col) maps)
        json-cols (->> (map json-build-object v)
                       (concat child-cols)
                       (string/join ","))]
    (format "
      left outer join (
        select
          %s,
          %s(
            %s(
              %s
            )
          ) as %s
        from%s%s%s
        group by %s
      ) %s
    " col
      (get-in pull-sql-map [adapter :json-agg])
      (get-in pull-sql-map [adapter :json-object])
      json-cols
      (pull-col k)
      (pull-from pf-joins order from)
      (if (not (empty? maps))
        (str "\n        "
          (->> (map #(pull-join adapter associations %) maps)
               (string/join "\n        ")))
        "")
      (if (some? limit)
        (str "\n        " limit)
        "")
      col
      (join-statement pull-join-map))))


(defn pull-joins [adapter associations acc v]
  (let [maps (filter map? v)
        joins (mapv #(pull-join adapter associations %) maps)
        acc (concat acc joins)]
    (if (empty? maps)
     acc
     (pull-joins adapter associations acc (map #(-> % vals first) maps)))))


(defn pull
  "Converts the :pull key of a map into a sql map"
  [{:keys [adapter associations]} m]
  (if (not (contains? m :pull))
    m
    (let [v (:pull m)
          cols (filter qualified-ident? v)
          maps (filter map? v)
          rel-cols (->> (mapv rel-key maps)
                        (mapv pull-col))
          col-sql (->> (mapv select-col cols)
                       (concat rel-cols)
                       (string/join ", "))
          joins (pull-joins adapter associations [] v)]
      (assoc m :select (str "select " col-sql)
               :join (string/join "\n" joins)))))


(defn pull-vec [k p-vec cm am]
  (if (empty? cm)
    p-vec
    (let [next-p-vec (if (empty? p-vec)
                        (get cm k)
                        p-vec)
          has-many-ks (->> (keys am)
                           (filter #(= (namespace %) (name k))))
          has-many-cols (->> (mapv #(hash-map % (get cm (:has-many (get am %)))) has-many-ks)
                             (filter #(some? (first (vals %)))))
          next-p-vec (concat next-p-vec has-many-cols)
          cm (dissoc cm k)]
      (pull-vec (first (keys cm))
        next-p-vec
        cm
        (apply dissoc am has-many-ks)))))


(defn expand-pull-asterisk [associations col-map m]
  (if (and (contains? m :pull)
           (= "*" (name (first (:pull m))))
           (not (empty? associations)))
    (let [table (keyword (first (:from m)))
          pull (pull-vec table [] col-map associations)]
      (assoc m :pull pull))
    m))


(defn fmt-pull [m]
  (if (contains? m :pull)
    (if (and (vector? (:pull m))
             (vector? (first (:pull m))))
      (assoc m :pull (first (:pull m)))
      m)
    m))
