(ns db.transactor
  (:require [db.sql :as sql]
            [db.defq]
            [db.pull]
            [clojure.java.jdbc :as jdbc])
  (:refer-clojure :exclude [update]))


(defn debug? [ctx]
  (true? (:debug ctx)))


(defn sqlite? [ctx]
  (= "sqlite" (:adapter ctx)))


(defn postgres? [ctx]
  (= "postgres" (:adapter ctx)))


(defn execute [ctx sql-vec]
  (when (debug? ctx)
    (println sql-vec))
  (jdbc/execute! ctx sql-vec))


(defn query [ctx sql-vec]
  (when (debug? ctx)
    (println sql-vec))
  (jdbc/query ctx sql-vec
    {:identifiers #(.replace % \_ \-)}))


(defn insert [ctx row]
  (if (postgres? ctx)
    (execute ctx
      (sql/insert ctx row))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/insert ctx row))
      (->> (sql/table row)
           (format "select * from %s where id in (select last_insert_rowid())")
           (query conn)
           (first)))))


(defn insert-all [ctx rows]
  (if (postgres? ctx)
    (execute ctx
      (sql/insert-all ctx rows))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/insert-all ctx rows))
      (let [{id :id} (first
                      (query conn ["select last_insert_rowid() as id"]))
            num-rows (if (sql/table-map? rows)
                       (-> rows vals first count)
                       (count rows))
            ids (take num-rows (range id 0 -1))
            table (sql/table rows)]
        (query conn
          (sql/from {table {:id ids}}))))))


(defn update [ctx row where]
  (if (postgres? ctx)
    (execute ctx
      (sql/update ctx row where))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/update ctx row where))
      (first
        (query conn (sql/from {(sql/table row) where}))))))


(defn update-all [ctx rows where]
  (if (postgres? ctx)
    (execute ctx
      (sql/update-all ctx rows where))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/update-all ctx rows where))
      (query conn (sql/from {(sql/table rows) where})))))


(defn upsert [ctx row & {:as unique-by}]
  (if (postgres? ctx)
    (execute ctx
      (sql/upsert ctx row unique-by))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/upsert ctx row unique-by))
      (first
        (query conn (sql/from {(sql/table row) (sql/upsert-params row unique-by)}))))))


(defn upsert-all [ctx rows & {:as unique-by}]
  (if (postgres? ctx)
    (execute ctx
      (sql/upsert-all ctx rows unique-by))
    (jdbc/with-db-transaction [conn ctx]
      (execute conn (sql/upsert-all ctx rows unique-by))
      (query conn (sql/from {(sql/table rows) (sql/upsert-all-params rows unique-by)})))))


(defn delete [ctx where]
  (if (postgres? ctx)
    (execute ctx
      (sql/delete ctx where))
    (jdbc/with-db-transaction [conn ctx]
      (let [deleted (first
                     (query conn (sql/from where)))]
        (execute conn (sql/delete ctx where))
        deleted))))


(defn delete-all [ctx where]
  (if (postgres? ctx)
    (execute ctx
      (sql/delete-all ctx where))
    (jdbc/with-db-transaction [conn ctx]
      (let [deleted (query conn (sql/from {(sql/table where) (sql/delete-all-params where)}))]
        (execute conn (sql/delete-all ctx where))
        deleted))))


(defn fetch
  [ctx path-vec & {:as options}]
  (let [rows (query ctx
               (sql/fetch ctx path-vec options))]
    (if (keyword? (last path-vec))
      rows
      (first rows))))


(defn from
  [ctx m & {:as options}]
  (query ctx
    (sql/from m options)))


(defn q
  ([ctx query-vec]
   (q ctx query-vec {}))
  ([ctx query-vec params]
   (query ctx
     (sql/q ctx query-vec params))))


(defn pull [ctx query-vec]
  (query ctx
    (db.pull/pull ctx query-vec)))


(defmacro defq
  ([ctx n filename]
   `(let [q-fn# (as-> (db.defq/query ~(str n) ~filename) %
                      (assoc % :ns *ns*)
                      (db.defq/query-fn ctx %))]
      (db.defq/create-root-var ~(str n) q-fn#)))
  ([ctx filename]
   `(db.defq/query-fns ~ctx ~filename)))
