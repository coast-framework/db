(ns db.sql-test
  (:require [db.sql :as sql]
            [clojure.test :refer [deftest testing is]]
            [time.core :as time]))


(deftest table-test
  (testing "table with nested map"
    (is (= "account" (sql/table {:account {}}))))

  (testing "table with qualified keword map"
    (is (= "account" (sql/table {:account/name "hello"}))))

  (testing "table with qualified keyword map"
    (is (= "organization" (sql/table #:organization{:id 1 :name [nil "name"]}))))

  (testing "table with sequential? and qualified keyword maps"
    (is (= "memberships" (sql/table [{:memberships/name "name"}]))))

  (testing "table with nested map with vector"
    (is (= "account" (sql/table {:account [{:name "name"}]})))))


(deftest where-test
  (testing "where with unqualified keyword map"
    (is (= ["a = ? and b = ?" 1 2] (sql/where {:a 1 :b 2}))))

  (testing "where with qualified keyword map"
    (is (= ["account.a = ? and account.b = ?" 1 2] (sql/where #:account{:a 1 :b 2})))))


(deftest insert-test
  (let [ctx {:schema {"account" {:column-names #{"name"}}}}]
    (testing "insert with qualified keyword map"
      (is (= ["insert into account (name) values (?)" "name"] (sql/insert ctx {:account/name "name"}))))

    (testing "insert-all with qualified keyword maps"
      (is (= ["insert into account (name) values (?), (?)" "name1" "name2"]
             (sql/insert-all ctx [#:account{:name "name1"}
                                  #:account{:name "name2"}]))))

    (testing "insert with nested map"
      (is (= ["insert into account (name) values (?)" "name"] (sql/insert ctx {:account {:name "name"}}))))

    (testing "insert-all with nested maps"
      (is (= ["insert into account (name) values (?), (?)" "name1" "name2"]
             (sql/insert-all ctx {:account [{:name "name1"} {:name "name2"}]}))))))


(deftest update-test
  (let [ctx {:schema {"account" {:column-names #{"name" "id" "a" "b"}}}}]
    (testing "update with qualified keyword map"
      (is (= ["update account set name = ? where id in (select id from account where id = ? limit 1)" "name" 1]
             (sql/update ctx {:account/name "name"} {:id 1}))))

    (testing "update-all with a qualified keyword map"
      (is (= ["update account set a = ?, b = ? where id in (?, ?, ?)" "a" "b" 1 2 3]
             (sql/update-all ctx #:account{:a "a" :b "b"} {:id [1 2 3]}))))

    (testing "update with a complex where clause"
      (is (= ["update account set name = ? where id in (select id from account where id = ? and name is null limit 1)" "name" 1]
             (sql/update ctx {:account/name "name"} {:id 1 :name nil}))))

    (testing "update with a complex where clause"
      (is (= ["update account set name = ? where id = ? and name is null" "name" 1]
             (sql/update-all ctx {:account/name "name"} {:id 1 :name nil}))))

    (testing "update with a sql-vec where clause"
      (is (= ["update account set name = ? where name = ? or name = ?" "name1" "name2" "name3"]
             (sql/update-all ctx {:account/name "name1"} ["name = ? or name = ?" "name2" "name3"]))))

    (testing "update-all with a nested map"
      (is (= ["update account set a = ?, b = ? where id in (?, ?, ?)" "a" "b" 1 2 3]
             (sql/update-all ctx {:account {:a "a" :b "b"}} {:id [1 2 3]})))))

  (let [ctx {:schema {"account" {:column-names #{"name" "id" "a" "b"} :columns [{:type :integer :name :updated-at}]}}}]
    (with-redefs [time/now (fn [] 12345)]
      (testing "update with updated-at"
        (is (= ["update account set name = ?, updated_at = ? where id in (select id from account where id = ? limit 1)" "name" 12345 1]
               (sql/update ctx {:account {:name "name"}} {:id 1}))))

      (testing "update-all with updated-at"
        (is (= ["update account set name = ?, updated_at = ? where name = ?" "name" 12345 "name1"]
               (sql/update-all ctx {:account {:name "name"}} ["name = ?" "name1"])))))))


(deftest delete-test
  (testing "delete with qualified keyword map"
    (is (= ["delete from account where id in (select id from account where account.name = ? limit 1)" "name"]
           (sql/delete {} {:account/name "name"}))))

  (testing "delete with a nested map and complex where clause"
    (is (= ["delete from account where id in (select id from account where id = ? and name is null limit 1)" 1]
           (sql/delete {} {:account {:id 1 :name nil}}))))

  (testing "delete with a nested map and nil combo where clause"
    (is (= ["delete from account where id in (select id from account where id = ? and (name in (?) or name is null) limit 1)" 1 "name"]
           (sql/delete {} {:account {:id 1 :name ["name" nil]}})))))


(deftest delete-all-test
  (testing "delete-all with a qualified keyword map"
    (is (= ["delete from account where account.a = ? and account.b = ?" "a" "b"]
           (sql/delete-all {} [#:account{:a "a" :b "b"}]))))


  (testing "delete-all with a complex where clause"
    (is (= ["delete from account where account.id = ? and (account.name in (?) or account.name is null)" 1 "name"]
           (sql/delete-all {} [#:account{:id 1 :name nil} #:account{:name "name"}])))))


(deftest fetch-test
  (testing "select without id"
    (is (= ["select * from account"]
           (sql/fetch {} [:account]))))

  (testing "basic select"
    (is (= ["select * from account where account.id = ?" 1]
           (sql/fetch {} [:account 1]))))

  (testing "basic join with select"
    (is (= ["select * from todo join account on account.id = todo.account_id where account.id = ?" 1]
           (sql/fetch {} [:account 1 :todo]))))

  (testing "basic join with select and where clause"
    (is (= ["select * from todo join account on account.id = todo.account_id where account.id = ? and todo.id = ?" 1 2]
           (sql/fetch {} [:account 1 :todo 2]))))

  (testing "three table fetch"
    (is (= ["select * from tag join todo on todo.id = tag.todo_id join account on account.id = todo.account_id where account.id = ? and todo.id = ? and tag.id = ?" 1 2 3]
           (sql/fetch {} [:account 1 :todo 2 :tag 3])))))


(deftest fetch-joins-test
  (testing "one join"
    (is (= "join account on account.id = todo.account_id"
           (sql/fetch-joins [:account :todo]))))

  (testing "no joins"
    (is (= "" (sql/fetch-joins [:account]))))

  (testing "three joins"
    (is (= "join todo on todo.id = tag.todo_id join account on account.id = todo.account_id"
           (sql/fetch-joins [:account :todo :tag])))))


(deftest from-test
  (testing "from with nested map"
    (is (= ["select * from account where account.id = ?" 1]
           (sql/from {:account {:id 1}}))))

  (testing "from with qualified keyword map"
    (is (= ["select * from account where account.id = ?" 1]
           (sql/from {:account/id 1}))))

  (testing "from with nested map with vector where query"
    (is (= ["select * from account where account.id in (?, ?, ?)" 1 2 3]
           (sql/from {:account {:id [1 2 3]}})))))


(deftest upsert-test
  (let [ctx {:schema {"account" {:column-names #{"name" "email" "id"}}}}]
    (testing "upsert with nested map"
      (is (= ["insert into account (name, email, id) values (?, ?, ?) on conflict(id) do update set name = excluded.name, email = excluded.email, id = excluded.id where id in (select id from account where id = ? limit 1)" "name" "email" 1 1]
             (sql/upsert ctx {:account {:name "name" :email "email" :id 1}}
               {:unique-by [:id]}))))

    (testing "upsert with qualified keyword map"
      (is (= ["insert into account (name, email, id) values (?, ?, ?) on conflict(account.id) do update set name = excluded.name, email = excluded.email, id = excluded.id where id in (select id from account where account.id = ? limit 1)" "name" "email" 1 1]
             (sql/upsert ctx #:account{:name "name" :email "email" :id 1}
               {:unique-by [:account/id]})))))

  (let [ctx {:schema {"account" {:column-names #{"name" "email" "id"} :columns [{:type :integer :name :updated-at}]}}}]
    (with-redefs [time/now (fn [] 12345)]
      (testing "upsert with updated-at"
        (is (= ["insert into account (name, email, id) values (?, ?, ?) on conflict(id) do update set name = excluded.name, email = excluded.email, id = excluded.id, updated_at = ? where id in (select id from account where id = ? limit 1)" "name" "email" 1 12345 1]
               (sql/upsert ctx {:account {:name "name" :email "email" :id 1}}
                 {:unique-by [:id]})))))))

(deftest upsert-all-test
  (let [ctx {:schema {"account" {:column-names #{"name" "email" "id"}}}}]
    (testing "upsert-all with nested map"
      (is (= ["insert into account (name, email, id) values (?, ?, ?), (?, ?, ?) on conflict(id) do update set name = excluded.name, email = excluded.email, id = excluded.id" "name" "email" 1 "name2" "email2" 2]
             (sql/upsert-all ctx {:account [{:name "name" :email "email" :id 1}
                                            {:name "name2" :email "email2" :id 2}]}
               {:unique-by [:id]}))))

    (testing "upsert-all with qualified keyword map"
      (is (= ["insert into account (name, email, id) values (?, ?, ?), (?, ?, ?) on conflict(account.id) do update set name = excluded.name, email = excluded.email, id = excluded.id" "name" "email" 1 "name2" "email2" 2]
             (sql/upsert-all ctx [#:account{:name "name" :email "email" :id 1}
                                  #:account{:name "name2" :email "email2" :id 2}]
               {:unique-by [:account/id]}))))

    (testing "upsert-all with qualified keyword map with postgres"
      (is (= ["insert into account (name, email, id) values (?, ?, ?), (?, ?, ?) on conflict(account.id) do update set name = excluded.name, email = excluded.email, id = excluded.id returning *" "name" "email" 1 "name2" "email2" 2]
             (sql/upsert-all (assoc ctx :adapter "postgres")
               [#:account{:name "name" :email "email" :id 1}
                #:account{:name "name2" :email "email2" :id 2}]
               {:unique-by [:account/id]})))))

  (let [ctx {:schema {"account" {:column-names #{"name" "email" "id"} :columns [{:type :integer :name :updated-at}]}}}]
    (with-redefs [time/now (fn [] 12345)]
      (testing "upsert-all with nested map and updated-at"
        (is (= ["insert into account (name, email, id) values (?, ?, ?), (?, ?, ?) on conflict(id) do update set name = excluded.name, email = excluded.email, id = excluded.id, updated_at = ?" "name" "email" 1 "name2" "email2" 2 12345]
               (sql/upsert-all ctx {:account [{:name "name" :email "email" :id 1}
                                              {:name "name2" :email "email2" :id 2}]}
                 {:unique-by [:id]})))))))

(deftest upsert-all-params-test
  (testing "upsert-all-params with nested map"
    (is (= {:id [1 2]} (sql/upsert-all-params {:account [{:name "name" :email "email" :id 1}
                                                         {:name "name2" :email "email2" :id 2}]}
                         {:unique-by [:id]}))))

  (testing "upsert-all-params with qualified keyword maps"
    (is (= {:account/id [1 2]} (sql/upsert-all-params [#:account{:name "name" :email "email" :id 1}
                                                       #:account{:name "name2" :email "email2" :id 2}]
                                 {:unique-by [:account/id]})))))

(deftest upsert-params-test
  (testing "upsert-params with nested map"
    (is (= {:id 1} (sql/upsert-params {:account {:name "name" :email "email" :id 1}}
                     {:unique-by [:id]}))))

  (testing "upsert-params with qualified keyword map"
    (is (= {:account/id 1} (sql/upsert-params #:account{:name "name" :email "email" :id 1}
                             {:unique-by [:account/id]})))))


(deftest q-test
  (testing "q with a valid sql statement with no params"
    (is (= ["select * from accounts order by created_at desc"]
           (sql/q {} '[:select * :from accounts :order-by created-at desc]))))

  (testing "q with a valid sql statement with a param"
    (is (= ["select * from accounts where email like ? order by created_at desc" "%@aol.com"]
           (sql/q {} '[:select *
                       :from accounts
                       :where email like ?email
                       :order-by created-at desc]
              {:email "%@aol.com"}))))

  (testing "q with a qualified keyword"
    (is (= ["select * from accounts where accounts.email like ? order by created_at desc" "%@aol.com"]
           (sql/q {} '[:select *
                       :from accounts
                       :where accounts/email like ?email
                       :order-by created-at desc]
              {:email "%@aol.com"}))))

  (testing "q with two order by columns"
    (is (= ["select * from accounts order by created_at desc, updated_at desc"]
           (sql/q {} '[:select *
                       :from accounts
                       :order-by created-at desc updated-at desc]))))

  (testing "q with two select columns"
    (is (= ["select id, name from accounts order by created_at desc, updated_at asc"]
           (sql/q {} '[:select id name
                       :from accounts
                       :order-by created-at desc updated-at]))))

  (testing "q with a sql-vec in the where clause"
    (is (= ["select * from account where id in (?, ?, ?)" 1 2 3]
           (sql/q {} '[:select *
                       :from account
                       :where ["id in (?, ?, ?)" 1 2 3]]))))

  (testing "q with a vector as an ?ident value"
    (is (= ["select * from account where id in (?, ?, ?)" 1 2 3]
           (sql/q {} '[:select *
                       :from account
                       :where id in ?ids]
             {:ids [1 2 3]}))))

  (testing "q with limit and offset"
    (is (= ["select * from account order by created_at desc offset 0 limit 10"]
           (sql/q {} '[:select *
                       :from account
                       :order-by created-at desc
                       :limit 10
                       :offset 0]))))

  (testing "q with group by and count(*)"
    (is (= ["select count(todo.id) as todos, todo.account_id from todo group by todo.account_id"]
           (sql/q {} '[:select (count todo/id) :as todos todo/account_id
                       :from todo
                       :group-by todo/account_id]))))

  (testing "q with a sql vec"
    (is (= ["select * from account"]
           (sql/q {} '["select * from account"])))))
