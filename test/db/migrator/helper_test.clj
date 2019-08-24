(ns db.migrator.helper-test
  (:require [db.migrator.helper :refer [add-foreign-key]]
            [clojure.test :refer [deftest testing is]]))


(deftest add-foreign-key-test
  (testing "without options"
    (is (= (add-foreign-key :todo :account)
           "alter table todo add constraint todo_account_fk foreign key (account_id) references account (id)")))

  (testing "with options"
    (is (= (add-foreign-key :todo :account :col "account" :pk "account_pk")
           "alter table todo add constraint todo_account_fk foreign key (account) references account (account_pk)"))))
