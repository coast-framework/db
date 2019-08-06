(ns db.migrator
  (:require [db.migrator.helper :as migrator.helper]
            [db.migrator.sql :as migrator.sql]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as string]
            [clojure.set :as set]
            [clojure.java.shell :as shell])
  (:import (java.io File))
  (:refer-clojure :exclude [drop]))


(defn migrations-dir []
  (.mkdirs (File. "db/migrations"))
  "db/migrations")


(defn migration-files []
  (->> (migrations-dir)
       (io/file)
       (file-seq)
       (filter #(.isFile %))
       (map #(.getName %))
       (filter #(or (.endsWith % ".sql")
                    (.endsWith % ".clj")))))


(defn create-table [conn]
  (jdbc/db-do-commands conn
    (jdbc/create-table-ddl :schema_migrations
                           [[:version "text" :primary :key]]
                           {:conditional? true})))


(defn completed-migrations [conn]
  (create-table conn)
  (->> (jdbc/query conn
        ["select version from schema_migrations order by version"]
        {:keywordize? true})
       (map :version)))


(defn version [filename]
  (first (string/split filename #"_")))


(defn migration-filename [version]
  (when (some? version)
    (let [filenames (migration-files)]
      (first
       (filter #(string/starts-with? % (str version)) filenames)))))


(defn pending [conn]
  (let [filenames (migration-files)
        all (set (map version filenames))
        completed (set (completed-migrations conn))
        versions (sort (set/difference all completed))]
    (mapv migration-filename versions)))


(defn statements [filename]
  (let [migration-type (last
                        (string/split filename #"\."))
        filename-with-path (str (migrations-dir) "/" filename)
        contents (slurp filename-with-path)]
    (condp = migration-type
      "sql" {:sql (migrator.sql/up contents)
             :filename filename}
      (let [f (load-file filename-with-path)
            output (f)]
        {:sql (string/join "" output)
         :vec output
         :filename filename}))))


(defn migrate [conn]
  (let [migrations (pending conn)]
    (reset! migrator.helper/rollback? false)
    (doseq [migration migrations]
      (let [statements (statements migration)
            friendly-name (string/replace migration #"\.sql|\.clj" "")]
        (if (string/blank? (:sql statements))
          (throw (Exception. (format "%s is empty" migration)))
          (do
            (println "")
            (println "-- Migrating: " friendly-name "---------------------")
            (println "")
            (println (:sql statements))
            (println "")
            (println "--" friendly-name "---------------------")
            (println "")
            (jdbc/db-do-commands conn
              (or (:vec statements) (:sql statements)))
            (jdbc/insert! conn
              :schema_migrations {:version (version migration)})
            (println friendly-name "migrated successfully")))))))


(defn rollback-statements [filename]
  (let [migration-type (last
                        (string/split filename #"\."))
        filename-with-path (str (migrations-dir) "/" filename)
        contents (slurp filename-with-path)]
    (condp = migration-type
      "sql" {:sql (migrator.sql/down contents)
             :filename filename}
      (let [f (load-file filename-with-path)
            output (f)]
        {:sql (string/join "" output)
         :vec output
         :filename filename}))))


(defn rollback [conn]
  (let [migration (-> (completed-migrations conn) last migration-filename)
        _ (reset! migrator.helper/rollback? true)]
    (when (some? migration)
        (let [statements (rollback-statements migration)
              friendly-name (string/replace migration #"\.sql|\.clj" "")]
          (if (string/blank? (:sql statements))
            (throw (Exception. (format "%s is empty" migration)))
            (do
              (println "")
              (println "-- Rolling back:" friendly-name "---------------------")
              (println "")
              (println (:sql statements))
              (println "")
              (println "--" friendly-name "---------------------")
              (println "")
              (jdbc/db-do-commands conn
                (or (:vec statements) (:sql statements)))
              (jdbc/delete! conn
                :schema_migrations ["version = ?" (version migration)])
              (println friendly-name "rolled back successfully")))))))


(defn create
 "Creates a new database"
 [{:keys [database adapter]}]
 (let [cmd (condp = adapter
             "sqlite" "touch"
             "postgres" "createdb"
             :else "")
       m (shell/sh cmd database)]
   (if (= 0 (:exit m))
     (str database " created successfully")
     (:err m))))


(defn drop
  "Drops an existing database"
  [{:keys [database adapter]}]
  (let [cmd (condp = adapter
              "sqlite" "rm"
              "postgres" "dropdb"
              :else "")
        m (shell/sh cmd database)]
    (if (= 0 (:exit m))
      (str database " dropped successfully")
      (:err m))))
