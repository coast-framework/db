(ns db.schema.generator-test
  (:require [db.schema.generator :refer [generate-string]]
            [clojure.test :refer [deftest testing is]]))


(deftest generate-string-test
  (is (= (generate-string "20190821112134"
           [{:table-name "account" :column-name "id" :column-type "integer" :primary-key 1 :default-value ""
             :is-not-null 1}
            {:table-name "account" :column-name "name" :column-type "text" :primary-key 0 :default-value ""
             :is-not-null 1}
            {:table-name "account" :column-name "email" :column-type "text" :primary-key 0 :default-value ""
             :is-not-null 1}
            {:table-name "account" :column-name "password" :column-type "text" :primary-key 0 :default-value ""
             :is-not-null 1}
            {:table-name "todo" :column-name "id" :column-type "integer" :primary-key 1 :default-value ""
             :is-not-null 1}
            {:table-name "todo" :column-name "name" :column-type "text" :primary-key 0 :default-value ""
             :is-not-null 0}
            {:table-name "todo" :column-name "account_id" :column-type "integer" :primary-key 0 :default-value ""
             :is-not-null 0}]
           [{:index-name "index_account_id_on_todo" :index-column "account_id" :table-name "todo"}]
           [{:from-table-name "todo" :to-table-name "account"}])
         (slurp "test/db/schema/sample-schema.clj.txt"))))
