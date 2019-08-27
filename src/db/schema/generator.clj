(ns db.schema.generator
  (:require [db.transactor :as transactor]
            [helper.core :as helper]
            [clojure.string :as string]
            [clojure.java.io :as io]))


(defn indexes-sql []
  {"sqlite" ["select
               index_info.name as index_column, index_list.name index_name,
               master.name as table_name
             from
               sqlite_master as master
             left outer join
               pragma_index_list(master.name) index_list
             left outer join
               pragma_index_info(index_list.name) index_info
             where
               master.name != 'schema_migrations'"]
   "postgres" []})



(defn indexes [ctx]
  (transactor/query ctx
    (get (indexes-sql) (:adapter ctx))))


(defn columns-sql []
  {"sqlite" ["select
               tables.name as table_name,
               table_info.cid as column_id,
               table_info.name as column_name,
               table_info.type as column_type,
               table_info.pk as primary_key,
               table_info.dflt_value as default_value,
               table_info.[notnull] as is_not_null
              from sqlite_master tables
              left outer join pragma_table_info((tables.name)) table_info on tables.name <> table_info.name
              where tables.type = 'table' and tables.name != 'schema_migrations'
              order by table_name, column_id"]
   "postgres" [""]})


(defn columns [ctx]
  (transactor/query ctx
    (get (columns-sql) (:adapter ctx))))


(defn foreign-keys-sql []
  {"sqlite" ["select
              master.name as from_table_name, fk_list.'table' as to_table_name, fk_list.'from' as from_column_name, fk_list.'to' as to_column_name
             from
               sqlite_master as master
             left outer join
               pragma_foreign_key_list(master.name) fk_list
             order by fk_list.seq"]
   "postgres" [""]})


(defn foreign-keys [ctx]
  (transactor/query ctx
    (get (foreign-keys-sql) (:adapter ctx))))


(defn column [{:keys [column-name column-type primary-key default-value is-not-null]}]
  (format "      (%s %s%s%s%s)"
    (-> column-type helper/kebab-case str)
    (-> column-name helper/kebab-case keyword)
    (if (= primary-key 1)
      " :primary-key true"
      "")
    (if (string/blank? default-value)
      ""
      (format " :default \"%s\"" default-value))
    (if (= is-not-null 1)
      " :null false"
      "")))


(defn index [{:keys [index-name index-column]}]
  (format "      (index :%s :name \"%s\")"
    (-> index-column helper/kebab-case symbol)
    index-name))


(defn foreign-key [{:keys [from-table-name to-table-name]}]
  (format "      (foreign-key :%s :%s)"
    (-> from-table-name helper/kebab-case)
    (-> to-table-name helper/kebab-case)))


(defn create-table [[table props]]
  (let [columns (filter #(contains? % :column-name) props)
        indexes (filter #(contains? % :index-name) props)
        foreign-keys (filter #(contains? % :from-table-name) props)]
    (format "(create-table %s%s%s%s)"
      (-> table helper/kebab-case keyword)
      (str "\n"
        (string/join "\n"
          (map column columns)))
      (if (empty? indexes)
        ""
        (str "\n"
          (string/join "\n"
            (map index indexes))))
      (if (empty? foreign-keys)
        ""
        (str "\n"
          (string/join "\n"
            (map foreign-key foreign-keys)))))))


(defn generate-string [version columns indexes foreign-keys]
  (let [cols (group-by :table-name columns)
        indices (group-by :table-name indexes)
        foreign-keys (group-by :from-table-name foreign-keys)
        schema (merge-with concat cols indices foreign-keys)
        create-tables (string/join "\n\n    "
                        (map create-table schema))
        template (slurp (io/resource "schema.clj.txt"))]
    (-> (helper/replace-pattern template [#"\{\{version\}\}" version])
        (helper/replace-pattern [#"\{\{create-tables\}\}" create-tables]))))


(defn generate [version columns indexes foreign-keys]
  (spit "schema.clj"
    (generate-string version columns indexes foreign-keys)))
