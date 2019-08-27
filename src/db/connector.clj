(ns db.connector
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [env.core :as env])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.util Properties)
           (java.io FileNotFoundException)))


(defn db-file [filename]
  (let [db-filename (format "db/%s" filename)
        resource (io/resource filename)]
    (if (or resource
          (.exists (io/file db-filename)))
      (slurp
        (or (io/resource filename)
            db-filename))
      "(defn placeholder [])")))


(defn context
  ([db-env]
   (let [db-env (-> (or db-env :dev) keyword)
         ctx (->> (db-file "db.edn")
                  (edn/read-string {:readers {'env env/env}})
                  (mapv (fn [[k v]] [k v]))
                  (into {}))
         ctx (get ctx db-env)
         schema ((load-string
                  (db-file "schema.clj")))
         associations ((load-string
                         (db-file "associations.clj")))]
     (assoc ctx :schema schema :associations associations)))
  ([]
   (context :dev)))


(def opts {:auto-commit        true
           :read-only          false
           :connection-timeout 30000
           :validation-timeout 5000
           :idle-timeout       600000
           :max-lifetime       1800000
           :minimum-idle       10
           :maximum-pool-size  10
           :register-mbeans    false})


(defn pool
  "Shamelessly stolen from tomekw/hikari-cp. Makes a new hikaricp data source"
  [ctx]
  (let [ctx (merge opts ctx)
        {:keys [adapter database]} ctx
        connection-init-sql (when (= "sqlite" adapter)
                              "PRAGMA foreign_keys=ON")
        data-sources {"sqlite" "org.sqlite.SQLiteDataSource"
                      "postgres" "org.postgresql.ds.PGSimpleDataSource"}
        datasource-class-name (get data-sources adapter)
        _ (when (nil? datasource-class-name)
            (throw (Exception. "Unsupported connection string, only sqlite and postgres are supported currently")))
        c (doto (HikariConfig.)
            (.setDataSourceClassName datasource-class-name)
            (.addDataSourceProperty  "databaseName" database)
            (.setUsername            (:username ctx))
            (.setPassword            (:password ctx))
            (.setAutoCommit          (:auto-commit ctx))
            (.setReadOnly            (:read-only ctx))
            (.setConnectionTimeout   (:connection-timeout ctx))
            (.setValidationTimeout   (:validation-timeout ctx))
            (.setIdleTimeout         (:idle-timeout ctx))
            (.setMaxLifetime         (:max-lifetime ctx))
            (.setMinimumIdle         (:minimum-idle ctx))
            (.setMaximumPoolSize     (:maximum-pool-size ctx))
            (.setConnectionInitSql   connection-init-sql))]
       _ (when (= "sqlite" adapter)
           (.addDataSourceProperty c "url" (str "jdbc:sqlite:" database)))
       _ (when (some? (:port ctx))
           (.addDataSourceProperty c "portNumber" (:port ctx)))
       _ (when (some? (:host ctx))
           (.addDataSourceProperty c "serverName" (:host ctx)))
    (merge {:datasource (HikariDataSource. c)}
      ctx)))


(defn connect [ctx]
  (pool ctx))


(defn disconnect [connection]
  (.close (:datasource connection)))
