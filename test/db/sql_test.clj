(ns db.sql-test
  (:require [db.sql :as sql]
            [clojure.test :refer [deftest testing is]]))


(deftest table-test
  (testing "table with nested map"
    (is (= "account" (sql/table {:account {}}))))

  (testing "table with qualified keword map"
    (is (= "account" (sql/table {:account/name "hello"}))))

  (testing "table with qualified keyword map"
    (is (= "organization" (sql/table #:organization{:id 1 :name [nil "name"]})))))


(deftest where-test
  (testing "where with unqualified keyword map"
    (is (= ["a = ? and b = ?" 1 2] (sql/where {:a 1 :b 2}))))

  (testing "where with qualified keyword map"
    (is (= ["account.a = ? and account.b = ?" 1 2] (sql/where #:account{:a 1 :b 2})))))


(deftest insert-test
  (testing "insert with qualified keyword map"
    (is (= ["insert into account (name) values (?)" "name"] (sql/insert {} {:account/name "name"}))))

  (testing "insert-all with qualified keyword maps"
    (is (= ["insert into account (name) values (?), (?)" "name1" "name2"]
           (sql/insert-all {} [#:account{:name "name1"}
                               #:account{:name "name2"}]))))

  (testing "insert with nested map"
    (is (= ["insert into account (name) values (?)" "name"] (sql/insert {} {:account {:name "name"}}))))

  (testing "insert-all with nested maps"
    (is (= ["insert into account (name) values (?), (?)" "name1" "name2"]
           (sql/insert-all {} {:account [{:name "name1"} {:name "name2"}]})))))


(deftest update-test
  (testing "update with qualified keyword map"
    (is (= ["update account set name = ? where id in (select id from account where id = ? limit 1)" "name" 1]
           (sql/update {} {:account/name "name"} {:id 1}))))

  (testing "update-all with a qualified keyword map"
    (is (= ["update account set a = ?, b = ? where id in (?, ?, ?)" "a" "b" 1 2 3]
           (sql/update-all {} #:account{:a "a" :b "b"} {:id [1 2 3]}))))

  (testing "update with a complex where clause"
    (is (= ["update account set name = ? where id in (select id from account where id = ? and name is null limit 1)" "name" 1]
           (sql/update {} {:account/name "name"} {:id 1 :name nil}))))

  (testing "update with a complex where clause"
    (is (= ["update account set name = ? where id = ? and name is null" "name" 1]
           (sql/update-all {} {:account/name "name"} {:id 1 :name nil})))))


(deftest delete-test
  (testing "delete with qualified keyword map"
    (is (= ["delete from account where id in (select id from account where account.name = ? limit 1)" "name"]
           (sql/delete {} {:account/name "name"}))))

  (testing "delete-all with a qualified keyword map"
    (is (= ["delete from account where account.a = ? and account.b = ?" "a" "b"]
           (sql/delete-all {} #:account{:a "a" :b "b"}))))

  (testing "delete with a nested map and complex where clause"
    (is (= ["delete from account where id in (select id from account where id = ? and name is null limit 1)" 1]
           (sql/delete {} {:account {:id 1 :name nil}}))))

  (testing "delete with a nested map and nil combo where clause"
    (is (= ["delete from account where id in (select id from account where id = ? and (name in (?) or name is null) limit 1)" 1 "name"]
           (sql/delete {} {:account {:id 1 :name ["name" nil]}}))))

  (testing "delete-all with a complex where clause"
    (is (= ["delete from account where account.id = ? and (account.name in (?) or account.name is null)" 1 "name"]
           (sql/delete-all {} #:account{:id 1 :name [nil "name"]})))))
