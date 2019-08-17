(ns db.migrator.helper
  (:require [helper.core :as helper]
            [clojure.string :as string]
            [env.core :as env]
            [db.connector :as connector]))


(def rollback? (atom false))


(def sql {"sqlite" {:timestamp "integer"
                    :now "(strftime('%s', 'now'))"
                    :pk "integer primary key"}
          "postgres" {:timestamp "timestamptz"
                      :now "now()"
                      :pk "serial primary key"}})


(defn not-null [m]
  (when (false? (:null m))
    "not null"))


(defn col-default [m]
  (when (contains? m :default)
    (str "default " (get m :default))))


(defn unique [m]
  (when (true? (:unique m))
    (str "unique")))


(defn collate [m]
  (when (contains? m :collate)
    (str "collate " (get m :collate))))


(defn col-type [type {:keys [precision scale]}]
  (if (or (some? precision)
          (some? scale))
    (str (or (helper/sqlize type) "numeric")
         (when (or (some? precision)
                   (some? scale))
           (str "(" (string/join ","
                      (filter some? [(or precision 0) scale]))
                ")")))
    (helper/sqlize type)))


(defn on-delete [m]
  (when (contains? m :on-delete)
    (str "on delete " (helper/sqlize (:on-delete m)))))


(defn on-update [m]
  (when (contains? m :on-update)
    (str "on update " (helper/sqlize (:on-update m)))))


(defn reference [m]
  (when (contains? m :references)
    (str "references " (:references m))))


(defn col [type col-name m]
  "SQL fragment for adding a column in create or alter table"
  (->> [(helper/sqlize col-name)
        (col-type type m)
        (unique m)
        (collate m)
        (not-null m)
        (col-default m)
        (reference m)
        (on-delete m)
        (on-update m)]
       (filter some?)
       (string/join " ")
       (string/trim)))


(defn references [col-name & {:as m}]
  (col :integer (str (name col-name) "-id") (merge {:null false :references (str (helper/sqlize col-name) "(id)") :index true :on-delete "cascade"} m)))


(defn drop-column
  "SQL for dropping a column from a table"
  [table col]
  (str "alter table " (helper/sqlize table) " drop column " (helper/sqlize col)))


(defn add-column
  "SQL for adding a column to an existing table"
  [table col-name type & {:as m}]
  (if (true? @rollback?)
    (drop-column table col-name)
    (str "alter table " (helper/sqlize table) " add column " (col type col-name m))))


(defn add-foreign-key
  "SQL for adding a foreign key column to an existing table"
  [from to & {col :col pk :pk fk-name :name :as m}]
  (let [from (helper/sqlize from)
        to (helper/sqlize to)]
   (string/join " "
     (filter some?
       ["alter table"
        from
        "add constraint"
        (or (helper/sqlize fk-name) (str from "_" to "_fk"))
        "foreign key"
        (str "(" (or (helper/sqlize (str col "_id") to) ")"))
        "references"
        to
        (str "(" (or (helper/sqlize pk) "id") ")")
        (on-delete m)
        (on-update m)]))))


(defn where [m]
  (when (contains? m :where)
    (str "where " (:where m))))


(defn index-cols [cols {order :order}]
  (->> (map #(conj [%] (get order %)) cols)
       (map #(map helper/sqlize %))
       (map #(string/join " " %))
       (map string/trim)))


(defn add-index
  "SQL for adding an index to an existing table"
  [table-name cols & {:as m}]
  (let [table-name (helper/sqlize table-name)
        cols (if (sequential? cols)
               cols
               [cols])
        cols (index-cols cols m)
        col-name (string/join ", " cols)
        index-col-names (map #(string/replace % #" " "_") cols)
        index-name (or (:name m) (str table-name "_" (string/join "_" index-col-names) "_index"))]
    (string/join " "
      (filter some?
        ["create"
         (unique m)
         "index"
         index-name
         "on"
         table-name
         (str "(" col-name ")")
         (where m)]))))


(defn add-reference
  "SQL for adding a foreign key column to an existing table"
  [table-name ref-name & {:as m}]
  (string/join " "
    (filter some?
      ["alter table"
       (helper/sqlize table-name)
       "add column"
       (helper/sqlize (or (:column m) (str ref-name "_id")))
       (or (-> m :type helper/sqlize) "integer")
       "references"
       (helper/sqlize ref-name)
       (str "(id)")])))


(defn alter-column [table-name col-name type & {:as m}]
  (string/join " "
    (filter some?
      ["alter table"
       (helper/sqlize table-name)
       "alter column"
       (helper/sqlize col-name)
       "type"
       (helper/sqlize type)
       (when (contains? m :using)
        (str "using " (:using m)))])))


(defn text [col-name & {:as m}]
  (col :text col-name m))


(defn timestamp [col-name & {:as m}]
  (col :timestamp col-name m))


(defn datetime [col-name & {:as m}]
  (col :datetime col-name m))


(defn timestamptz [col-name & {:as m}]
  (col :timestamptz col-name m))


(defn integer [col-name & {:as m}]
  (col :integer col-name m))


(defn bool [col-name & {:as m}]
  (col :boolean col-name m))


(defn decimal [col-name & {:as m}]
  (col :decimal col-name m))


(defn json [col-name & {:as m}]
  (col :json col-name m))


(defn uuid [col-name & {:as m}]
  (col :uuid col-name m))


(defn drop-table [table]
  (str "drop table " (helper/sqlize table)))


(defn reference-col [s]
  (let [ref-col (-> (re-find #"references (\w+)" s)
                    (last))]
    (when (some? ref-col)
      (str ref-col "_id"))))


(defn create-table
  "SQL to create a table"
  [table & args]
  (let [ctx (connector/context (env/env :coast-env))]
    (if (true? @rollback?)
      (drop-table table)
      (let [args (if (sequential? args) args '())
            [opts args] (if (map? (first args))
                          [(first args) (rest args)]
                          [{} args])
            index-sql-strings (->> (map reference-col args)
                                   (filter some?)
                                   (map #(add-index table %)))
            pk-col (or (:primary-key opts) "id")
            not-exists (if (true? (:if-not-exists opts))
                         "if not exists "
                         "")]
        (concat
          [(string/join " "
             (filter some?
               [(str "create table " not-exists (helper/sqlize table) " (")
                (string/join ", "
                 (conj args (str pk-col " " (get-in sql [(:adapter ctx) :pk]))))
                ")"]))]
          index-sql-strings)))))


(defn create-extension [s]
  (str "create extension " s))


(defn drop-extension [s]
  (str "drop extension " s))


(defn drop-foreign-key [alter-table-name & {:as m}]
  (let [constraint (when (contains? m :table)
                     (helper/sqlize (:table m)) "_" (helper/sqlize alter-table-name) "_fkey")
        constraint (if (contains? m :name)
                     (helper/sqlize (:name m))
                     constraint)]
    (str "alter table " (helper/sqlize alter-table-name) " drop constraint " constraint)))


(defn drop-index [table-name & {cols :column :as m}]
  (let [cols (if (sequential? cols) cols [cols])
        cols (index-cols cols m)
        index-col-names (map #(string/replace % #" " "_") cols)
        index-name (or (-> m :name helper/sqlize) (str table-name "_" (string/join "_" index-col-names) "_index"))]
    (str "drop index " index-name)))


(defn drop-reference [table-name ref-name]
  (str "alter table "
       (helper/sqlize table-name)
       " drop constraint "
       (helper/sqlize ref-name) "_" (helper/sqlize table-name) "_fkey"))


(defn rename-column [table-name column-name new-column-name]
  (string/join " "
    ["alter table"
     (helper/sqlize table-name)
     "rename column"
     (helper/sqlize column-name)
     "to"
     (helper/sqlize new-column-name)]))


(defn rename-index [index-name new-index-name]
  (string/join " "
    ["alter index"
     (helper/sqlize index-name)
     "rename to"
     (helper/sqlize new-index-name)]))


(defn rename-table [table-name new-table-name]
  (string/join " "
    ["alter table"
     (helper/sqlize table-name)
     "rename to"
     (helper/sqlize new-table-name)]))


(defn timestamps []
  (let [{:keys [adapter]} (connector/context (env/env :coast-env))]
    (string/join " "
      [(str "updated_at " (get-in sql [adapter :timestamp]) ",")
       (str "created_at " (get-in sql [adapter :timestamp]) " not null default " (get-in sql [adapter :now]))])))
