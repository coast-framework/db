(ns db.q-test
  (:require [db.q :as q]
            [clojure.test :refer [deftest testing is]]))

(deftest sql-map-test
  (testing "sql-map with subquery"
    (is (= (q/sql-map '[:select * :from accounts :order-by created-at desc])
           '{:select [*]
             :from [accounts]
             :order-by [created-at desc]})))

  (testing "sql-map with subquery"
    (is (= (q/sql-map '[:select id name :from accounts :where id in (:select (distinct account-id) :from test)])
           '{:select [id name]
             :from [accounts]
             :where [id in (:select (distinct account-id) :from test)]})))

  (testing "sql-map with nil"
    (is (= (q/sql-map nil) nil)))

  (testing "sql-map with empty vec"
    (is (= (q/sql-map []) {})))

  (testing "sql-map with select alias"
    (is (= (q/sql-map '[:select (count *) :as all])
           {:select '[(count *) :as all]}))))


(deftest select-test
  (testing "select with asterisk"
    (is (= (q/select '[*]) {:select "select *"})))

  (testing "select with two columns"
    (is (= (q/select '[id name]) {:select "select id, name"})))

  (testing "select with an aggregate function"
    (is (= (q/select '[(count *) name]) {:select "select count(*), name"})))

  (testing "select with an aggregate function with args"
    (is (= (q/select '[(group-concat id, name), name]) {:select "select group_concat(id, name), name"})))

  (testing "select with alias"
    (is (= (q/select '[(count id) :as todos account-id])
           {:select "select count(id) as todos, account_id"}))))


(deftest from-test
  (testing "from with one table"
    (is (= (q/from '[account]) {:from "from account"})))

  (testing "from with two tables"
    (is (= (q/from '[account todo]) {:from "from account, todo"}))))


(deftest join-test
  (testing "join with one table"
    (is (= (q/join '{:query {:from account :join [todo]}} "join" '[todo]) {:join "join todo on todo.account_id = account.id"})))

  (testing "left-outer-join with two tables"
    (is (= (q/join '{:query {:from account :join [todo tag]}} "left outer join" '[todo tag]) {:left-outer-join "left outer join todo on todo.account_id = account.id left outer join tag on tag.account_id = account.id"}))))


(deftest where-test
  (testing "where with ?idents"
    (is (= (q/where {:params {:id 1 :name "name"}}
             '[id = ?id :and name = ?name])
           {:where "where id = ? and name = ?"
            :args '(1 "name")})))

  (testing "where with values"
    (is (= (q/where {:params {:id 1 :name "name"}}
             '[id = 2 :or name = "name2"])
           {:where "where id = ? or name = ?"
            :args '(2 "name2")})))


  (testing "where with a mix of ?idents and values"
    (is (= (q/where {:params {:id 1 :name "name" :val3 "time@to.party"}}
             '[id = 2 :or name = "name2" :or email = ?val3])
           {:where "where id = ? or name = ? or email = ?"
            :args '(2 "name2" "time@to.party")})))

  (testing "where missing ?idents"
    (is (= (q/where {} '[id = ?id])
           {:where "where id = ?"
            :args '()})))

  (testing "where with sql-vec"
    (is (= (q/where {} [["(id = ? and name = ?) or name is null" 1 "name"]])
           {:where "where (id = ? and name = ?) or name is null"
            :args '(1 "name")})))

  (testing "where with a vector of values"
    (is (= (q/where {:params {:ids [1 2 3]}} '[id in ?ids])
           {:where "where id in (?, ?, ?)"
            :args '(1 2 3)}))))
