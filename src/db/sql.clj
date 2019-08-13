(ns db.sql
  (:require [helper.core :as helper]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [db.q])
  (:refer-clojure :exclude [update]))


(defn postgres? [ctx]
  (= (:adapter ctx) "postgres"))


(defn table-map? [m]
  (and (map? m)
    (let [first-val (first (vals m))]
      (coll? first-val))))


(defn single-table-map? [m]
  (and (map? m)
    (let [first-val (first (vals m))]
      (map? first-val))))


(defn table [val]
  (cond
    (keyword? val) (helper/sqlize val)

    (table-map? val) (-> val ffirst name helper/sqlize)

    (and (map? val)
      (every? qualified-ident? (keys val)))
    (-> val keys first namespace helper/sqlize)

    (sequential? val) (table (first val))

    :else nil))


(defn params [m]
  (cond
    (table-map? m) (-> m vals first)
    (map? m) m
    :else nil))


(defn columns [m]
  (cond
    (table-map? m) (->> m vals first keys (map helper/sqlize))

    (and (map? m)
      (every? qualified-ident? (keys m)))
    (->> m keys (map name) (map helper/sqlize))

    (map? m) (->> m keys (map helper/sqlize))

    :else nil))


(defn placeholders [m]
  (cond
    (table-map? m) (->> m vals first helper/vectorize (mapcat keys) (map (fn [_] "?")))
    (map? m) (->> m keys (map (fn [_] "?")))
    :else nil))


(defn values [m]
  (cond
    (table-map? m) (->> m vals first vals)
    (map? m) (vals m)
    :else nil))


(defn op [[key val]]
  (cond
    (nil? val) (format "%s is null" (helper/sqlize key))
    (sequential? val) (format "%s%s%s%s"
                        (if (not (empty? (filter nil? val)))
                          "("
                          "")
                        (if (empty? (filter some? val))
                          "1 = 1"
                          (helper/sqlize key))
                        (if (not (empty? (filter some? val)))
                          (->> (filter some? val)
                               (map (fn [_] "?"))
                               (string/join ", ")
                               (format " in (%s)"))
                          "")
                        (if (not (empty? (filter nil? val)))
                          (format " or %s is null)" (helper/sqlize key))
                          ""))
    :else (format "%s = ?" (helper/sqlize key))))


(defn qualify-table-map [m]
  (helper/map-keys #(keyword (-> m ffirst name) (name %)) (-> m vals first)))


(defn where
  "Takes a map or a vector and returns the where portion of a sql vec"
  [val]
  (cond
    (vector? val) val
    (single-table-map? val) (where (qualify-table-map val))
    (map? val) (let [pairs (map identity val)
                     columns (->> (map op pairs)
                                  (filter (comp not string/blank?)))
                     values (->> (map second pairs)
                                 (helper/flat)
                                 (filter some?))]
                  (apply conj
                    [(string/join " and " columns)]
                    (filter some? values)))
    :else nil))


(defn insert
  "Takes a db context and map and returns an insert java.jdbc sql vec"
  ([ctx m]
   (let [table (table m)
         columns (string/join ", " (columns m))
         placeholders (string/join ", " (placeholders m))
         values (values m)
         returning (if (postgres? ctx)
                     " returning *"
                     "")]
     (apply conj
      [(format "insert into %s (%s) values (%s)%s" table columns placeholders returning)]
      values))))


(defn insert-all
  "Takes a db context and a sequential? of maps and returns an insert java.jdbc sql vec"
  [ctx maps]
  (let [m (if (table-map? maps)
            (-> maps vals first)
            maps)
        table (table maps)
        columns (string/join ", " (columns (first m)))
        placeholders (string/join ", " (map #(format "(%s)" (->> (placeholders %) (string/join ", "))) m))
        values (mapcat values m)
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
      [(format "insert into %s (%s) values %s%s"
         table columns placeholders returning)]
      values)))


(defn update
  "Takes a db context, a map representing the row to be updated
   and a where clause in the form of a map or a sql vec"
  ([ctx m where-clause all?]
   (let [table (table m)
         columns (columns m)
         placeholders (placeholders m)
         values (values m)
         where-clause (where where-clause)
         where-sql (first where-clause)
         where-values (rest where-clause)
         returning (if (postgres? ctx)
                     " returning *"
                     "")
         set-columns (string/join ", "
                       (map
                         #(string/join " = " %)
                         (partition 2
                           (interleave columns placeholders))))
         where-in (if all?
                    where-sql
                    (format "id in (select id from %s where %s limit 1)" table where-sql))]
      (apply conj
        [(format "update %s set %s where %s%s"
           table set-columns where-in returning)]
        (concat values where-values))))
  ([ctx m where-clause]
   (update ctx m where-clause false)))


(defn update-all [ctx m where-clause]
  (update ctx m where-clause true))


(comment
  (update-all {} {:account {:a "a" :b "b"}} {:id [1 2 3] :name ["hello" nil]}))


(defn upsert-params [row {:keys [unique-by]}]
    (select-keys (params row) unique-by))


(defn upsert-all-params [rows {:keys [unique-by]}]
  (let [rows (if (table-map? rows) (-> rows vals first) rows)
        rows (map #(select-keys % unique-by) rows)]
    (apply (partial merge-with (comp distinct helper/flat vector)) rows)))


(defn upsert [ctx m options]
  (let [table (table m)
        columns (columns m)
        columns-sql (string/join ", " columns)
        placeholders (string/join ", " (placeholders m))
        values (values m)
        unique-keys (:unique-by options)
        where-clause (where (upsert-params m options))
        where-sql (first where-clause)
        where-values (rest where-clause)
        conflict (string/join "," (map helper/sqlize unique-keys))
        set-columns (string/join ", "
                      (map
                        #(string/join " = " %)
                        (partition 2
                          (interleave columns (map #(str "excluded." %) columns)))))
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
     [(format "insert into %s (%s) values (%s) on conflict(%s) do update set %s where id in (select id from %s where %s limit 1)%s"
        table columns-sql placeholders conflict set-columns
        table where-sql returning)]
     (concat values where-values))))


(defn upsert-all [ctx maps options]
  (let [rows (if (table-map? maps)
               (-> maps vals first)
               maps)
        table (table maps)
        columns (columns (first rows))
        columns-sql (string/join ", " columns)
        placeholders (string/join ", " (map #(format "(%s)" (->> (placeholders %) (string/join ", "))) rows))
        values (mapcat values rows)
        conflict (string/join "," (map helper/sqlize (:unique-by options)))
        set-columns (string/join ", "
                      (map
                        #(string/join " = " %)
                        (partition 2
                          (interleave columns (map #(str "excluded." %) columns)))))
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
     [(format "insert into %s (%s) values %s on conflict(%s) do update set %s%s"
        table columns-sql placeholders conflict set-columns returning)]
     values)))


(defn delete
  "Takes a db context, a table name, a where clause and a bool and returns a java.jdbc delete statement that deletes one or many rows depending on truthiness of one?"
  ([ctx where-clause one?]
   (let [table (table where-clause)
         where-clause (where (params where-clause))
         where-sql (first where-clause)
         params (rest where-clause)
         delete-where (if one?
                       (format "id in (select id from %s where %s limit 1)" table where-sql)
                       where-sql)
         delete-where (if (postgres? ctx)
                        (str delete-where " returning *")
                        delete-where)]

    (apply conj
     [(format "delete from %s where %s" table delete-where)]
     params)))
  ([ctx where-clause]
   (delete ctx where-clause true)))


(defn delete-all-params [rows]
  (let [rows (if (table-map? rows) (-> rows vals first) rows)]
    (apply (partial merge-with (comp distinct helper/flat vector)) rows)))


(defn delete-all
  "Takes a db context, a table name, and a where clause and returns a java.jdbc delete statement that deletes many rows"
  [ctx where-clause]
  (delete ctx {(table where-clause) (delete-all-params where-clause)} false))


(defn join [[left right]]
  (format "join %s on %s.%s = %s.%s" (name left) (name left) "id" (name right) (str (name left) "_id")))


(defn dup [num l]
  (let [frst (first l)
        lst (last l)]
    (mapcat #(if (and (not= frst %)
                      (not= lst %))
               (repeat num %)
               (list %))
      l)))


(defn fetch-joins [keywords]
  (->> keywords
       (dup 2)
       (partition 2)
       (map join)
       (reverse)
       (string/join " ")))


(defn fetch [ctx path-vec]
  (let [keywords (filter keyword? path-vec)
        from (-> keywords last helper/sqlize)
        joins (fetch-joins keywords)
        ids (filter #(not (keyword? %)) path-vec)
        where-clause (when (not (empty? ids))
                       (->> (partition 2 path-vec)
                            (mapv #(vector (keyword (-> % first name) "id") (second %)))
                            (into {})))
        where (when (not (empty? where-clause))
                (where where-clause))
        where-sql (first where)
        params (rest where)]
      (apply conj
        [(string/join " "
           (filter #(not (string/blank? %))
             [(format "select * from %s" from)
              joins
              (when (some? where) (format "where %s" where-sql))]))]
        params)))


(defn from [m]
  (let [where (where m)
        table (table m)
        where-sql (first where)
        where-params (rest where)]
    (apply conj
      [(format "select * from %s where %s" table where-sql)]
      where-params)))


(defn q
  ([ctx v m]
   (if (and
        (vector? v)
        (string? (first v)))
     v
     (db.q/sql-vec ctx v m)))
  ([ctx v]
   (q ctx v {})))
