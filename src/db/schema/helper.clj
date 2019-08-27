(ns db.schema.helper
  (:require [helper.core :as helper]))


(defn version [_ & args]
  (apply merge args))


(defn create-table [table & fns]
  (let [columns (->> (filter #(contains? % :column) fns)
                     (mapv :column))
        foreign-keys (->> (filter #(contains? % :foreign-key) fns)
                          (mapv :foreign-key))
        indexes (->> (filter #(contains? % :index) fns)
                     (mapv :index))]
    {(helper/sqlize table) {:column-names (->> (map :name columns)
                                               (map helper/sqlize)
                                               (set))
                            :columns columns
                            :foreign-keys foreign-keys
                            :indexes indexes}}))


(defn column [type column-name & {:as options}]
  {:column (merge {:type type :name column-name}
             options)})


(def integer (partial column :integer))
(def text (partial column :text))
(def timestamp (partial column :timestamp))
(def datetime (partial column :datetime))
(def timestamptz (partial column :timestamptz))
(def bool (partial column :bool))
(def decimal (partial column :decimal))
(def json (partial column :json))
(def uuid (partial column :uuid))


(defn foreign-key [from to]
  {:foreign-key [from to]})


(defn index [column-name & {:as options}]
  {:index [column-name (:name options)]})
