(ns db.schema.helper-test
  (:require [db.schema.helper :refer [version integer text create-table index foreign-key]]
            [clojure.test :refer [deftest testing is]]))


(deftest version-test
  (is (= {"account" {:column-names #{"id" "name" "email" "password"}
                     :columns [{:type :integer :name :id :primary-key true}
                               {:type :text :name :name :null false}
                               {:type :text :name :email :null false}
                               {:type :text :name :password :null false}]
                     :foreign-keys []
                     :indexes [[:email "index_email_on_account"]]}
          "todo" {:column-names #{"id" "name" "account_id"}
                  :columns [{:type :integer :name :id :primary-key true}
                            {:type :text :name :name :null false}
                            {:type :integer :name :account-id}]
                  :indexes [[:account-id "index_account_id_on_todo"]]
                  :foreign-keys [[:account :todo]]}}
         (version "123"
          (create-table :account
           (integer :id :primary-key true)
           (text :name :null false)
           (text :email :null false)
           (text :password :null false)
           (index :email :name "index_email_on_account"))

          (create-table :todo
           (integer :id :primary-key true)
           (text :name :null false)
           (integer :account-id)
           (foreign-key :account :todo)
           (index :account-id :name "index_account_id_on_todo"))))))
