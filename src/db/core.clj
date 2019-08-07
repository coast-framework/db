(ns db.core
  (:require [db.connector]
            [db.transactor]
            [db.migrator]
            [db.associator]
            [db.migrator.helper]
            [db.migrator.generator]
            [db.defq]
            [clojure.java.jdbc :as jdbc])
  (:refer-clojure :exclude [update drop]))

(def query db.transactor/query)
(def execute db.transactor/execute)
(def insert db.transactor/insert)
(def insert-all db.transactor/insert-all)
(def update db.transactor/update)
(def update-all db.transactor/update-all)
(def upsert db.transactor/upsert)
(def upsert-all db.transactor/upsert-all)
(def delete db.transactor/delete)
(def delete-all db.transactor/delete-all)
(def fetch db.transactor/fetch)
(def from db.transactor/from)
(def q db.transactor/q)
(def pull db.transactor/pull)

(defmacro with-transaction [binder context & body]
  `(jdbc/with-db-transaction [~binder ~context]
     ~@body))

(defmacro defq
  ([n filename]
   `(let [q-fn# (-> (db.defq/query ~(str n) ~filename)
                    (assoc :ns *ns*)
                    (db.defq/query-fn))]
      (db.defq/create-root-var ~(str n) q-fn#)))
  ([filename]
   `(db.defq/query-fns ~filename)))

(def migration db.migrator.generator/migration)

(def migrate db.migrator/migrate)
(def rollback db.migrator/rollback)
(def create db.migrator/create)
(def drop db.migrator/drop)

(def create-table db.migrator.helper/create-table)
(def drop-table db.migrator.helper/drop-table)
(def add-column db.migrator.helper/add-column)
(def add-index db.migrator.helper/add-index)
(def add-foreign-key db.migrator.helper/add-foreign-key)
(def add-reference db.migrator.helper/add-reference)
(def drop-foreign-key db.migrator.helper/drop-foreign-key)
(def drop-column db.migrator.helper/drop-column)
(def drop-index db.migrator.helper/drop-index)
(def drop-reference db.migrator.helper/drop-reference)
(def text db.migrator.helper/text)
(def integer db.migrator.helper/integer)
(def bool db.migrator.helper/bool)
(def decimal db.migrator.helper/decimal)
(def uuid db.migrator.helper/uuid)
(def reference db.migrator.helper/reference)

(def tables db.associator/tables)
(def table db.associator/table)
(def primary-key db.associator/primary-key)
(def has-many db.associator/has-many)
(def belongs-to db.associator/belongs-to)

(def context db.connector/context)
(def connect db.connector/connect)
(def disconnect db.connector/disconnect)
