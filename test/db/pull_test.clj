(ns db.pull-test
  (:require [db.pull :as pull]
            [clojure.test :refer [deftest testing is]]))


(deftest pull-test
  (let [associations '{:account/todos {:joins [{:table todo :left todo.account_id :right account.id}]
                                       :has-many :todo
                                       :from todo
                                       :col todo.account_id}
                       :todo/account {:joins [{:table account :left account.id :right todo.account_id}]
                                      :belongs-to :account
                                      :from account
                                      :col account.id}}]
    (testing "pull"
      (is (= {:pull '[account/name #:account{:todos [todo/id todo/name]}]
              :select "select account$todos, account.name as account$name"
              :join "
      left outer join (
        select
          todo.account_id,
          json_group_array(
            json_object(
              'todo$id', todo.id,'todo$name', todo.name
            )
          ) as account$todos
        from (
          select
            todo.*,
            row_number() over (order by id) as rn
          from todo
        ) as todo
        group by todo.account_id
      ) todo on todo.account_id = account.id\n    "}
             (pull/pull {:adapter "sqlite" :associations associations}
               {:pull '[account/name
                        {:account/todos [todo/id todo/name]}]}))))

    (testing "pull with order and limit"
      (is (= {:pull '[account/name
                      {(:account/todos :order-by todo/created-at :limit 777)
                       [todo/id todo/name todo/created-at]}]
              :select "select account$todos, account.name as account$name"
              :join "
      left outer join (
        select
          todo.account_id,
          json_group_array(
            json_object(
              'todo$id', todo.id,'todo$name', todo.name,'todo$created_at', todo.created_at
            )
          ) as account$todos
        from (
          select
            todo.*,
            row_number() over (order by todo.created_at asc) as rn
          from todo
        ) as todo
        where rn <= 777
        group by todo.account_id
      ) todo on todo.account_id = account.id\n    "}
             (pull/pull {:adapter "sqlite" :associations associations}
               {:pull '[account/name
                        {(:account/todos :order-by todo/created-at :limit 777)
                         [todo/id todo/name todo/created-at]}]}))))))
