(ns db.connector
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [env.core :as env])
  (:import (java.io FileNotFoundException)))


(defn db-edn []
  (let [filename "db.edn"
        resource (io/resource filename)]
    (slurp (or resource filename))))


(defn context
  ([db-env]
   (let [ctx (->> (db-edn)
                  (edn/read-string {:readers {'env env/env}})
                  (mapv (fn [[k v]] [k v]))
                  (into {}))]
      (get ctx (keyword (or db-env :dev)))))
  ([]
   (context :dev)))


(defn sqlite [ctx]
  (merge
    {:connection-uri (format "jdbc:sqlite:%s?foreign_keys=on;" (:database ctx))
     :classname "org.sqlite.SQLiteDataSource"}
    ctx))


(defn postgres [ctx]
  (merge
    {:dbtype "postgresql"
     :dbname (:database ctx)
     :host (:host ctx)
     :user (:username ctx)
     :password (:password ctx)
     :classname "org.postgresql.ds.PGSimpleDataSource"}
    ctx))


(defn connect [ctx]
  (case (:adapter ctx)
    "sqlite" (sqlite ctx)
    "postgres" (postgres ctx)
    (throw (Exception. "connect only allows sqlite and postgres adapters"))))


(defn disconnect [connection]
  (.close (:datasource connection)))
