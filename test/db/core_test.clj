(ns db.core-test
  (:require [db.core :as db]
            [clojure.test :refer [deftest use-fixtures is testing]]
            [clojure.java.io :as io]))

(def context {:adapter "sqlite" :database "db.sqlite3"})

(defn delete-dir [root]
  (when (.isDirectory (io/file root))
    (doseq [path (.listFiles (io/file root))]
      (delete-dir path)))
  (io/delete-file root))

(defn with-db [f]
  (db/drop context)
  (db/create context)
  (f)
  (Thread/sleep 100)
  (db/drop context))

(defn with-schema [f]
  (when (.exists (io/file "db/migrations"))
    (delete-dir "db/migrations"))
  (when (.exists (io/file "db/db.edn"))
    (io/delete-file "db/db.edn"))
  (when (.exists (io/file "db/schema.clj"))
    (io/delete-file "db/schema.clj"))
  (spit "db/db.edn" (str {:test context}))
  (let [conn (db/connect (db/context :test))]
   (db/migration ["create-table-account" "name:text" "email:text" "password:text"])
   (db/migrate conn)
   (f)
   (delete-dir "db/migrations")
   (io/delete-file "db/db.edn")
   (io/delete-file "db/schema.clj")))

(use-fixtures :once with-db with-schema)

(deftest insert-test
  (let [conn (db/connect (db/context :test))]
    (testing "insert with nested map"
      (is (= {:name "name" :email "email@email.com" :password "password"
              :id 1 :updated-at nil}
             (dissoc (db/insert conn {:account {:name "name" :email "email@email.com" :password "password"}})
               :created-at))))

    (testing "insert with qualified keyword map"
      (is (= {:name "name" :email "email@email.com" :password "password"
              :id 2 :updated-at nil}
             (dissoc (db/insert conn #:account{:name "name" :email "email@email.com" :password "password"})
               :created-at))))))

(deftest insert-all-test
  (let [conn (db/connect (db/context :test))]
    (testing "insert-all with nested map"
      (is (= '({:id 3 :name "name3" :email "email@email.com" :password "password"}
               {:id 4 :name "name4" :email "email@email.com" :password "password"})
             (->> (db/insert-all conn
                    {:account [{:name "name3" :email "email@email.com" :password "password"}
                               {:name "name4" :email "email@email.com" :password "password"}]})
                  (map #(dissoc % :created-at :updated-at))))))

    (testing "insert-all with qualified keyword map"
      (is (= '({:id 5 :name "name" :email "email@email.com" :password "password"}
               {:id 6 :name "name" :email "email@email.com" :password "password"})
             (->> (db/insert-all conn
                    [#:account{:name "name" :email "email@email.com" :password "password"}
                     #:account{:name "name" :email "email@email.com" :password "password"}])
                  (map #(dissoc % :created-at :updated-at))))))))

(deftest update-test
  (let [conn (db/connect (db/context :test))]
    (testing "update with nested map"
      (is (= {:name "name1" :email "name1@test.com" :password "password" :id 1}
             (dissoc (db/update conn
                        {:account {:name "name1" :email "name1@test.com"}}
                        {:id 1})
               :created-at :updated-at))))

    (testing "update with qualified keyword map"
      (is (= {:name "name2" :email "name2@test.com" :password "password" :id 2}
             (dissoc (db/update conn
                        #:account{:name "name2" :email "name2@test.com"}
                        {:id 2})
               :created-at :updated-at))))))

(deftest update-all-test
  (let [conn (db/connect (db/context :test))]
    (testing "update-all with nested map"
      (is (= '({:id 3 :name "new-name" :email "new@test.com" :password "password"}
               {:id 4 :name "new-name" :email "new@test.com" :password "password"})
             (->> (db/update-all conn
                    {:account {:name "new-name" :email "new@test.com"}}
                    {:id [3 4]})
                  (map #(dissoc % :created-at :updated-at))))))

    (testing "update-all with qualified keyword map"
      (is (= '({:id 5 :name "new-name" :email "email@email.com" :password "password"}
               {:id 6 :name "new-name" :email "email@email.com" :password "password"})
             (->> (db/update-all conn
                    {:account/name "new-name"}
                    {:id [5 6]})
                  (map #(dissoc % :created-at :updated-at))))))))

(deftest upsert-test
  (let [conn (db/connect (db/context :test))]
    (testing "upsert with nested map"
      (is (= {:name "upserted" :email "name1@test.com" :password "password" :id 1}
             (-> (db/upsert conn
                    {:account {:name "upserted" :id 1}}
                    :unique-by [:id])
                 (dissoc :created-at :updated-at)))))

    (testing "upsert with qualified keyword map"
      (is (= {:name "name2" :email "name2@test.com" :password "password" :id 2}
             (-> (db/upsert conn
                    #:account{:name "name2" :email "name2@test.com" :id 2}
                    :unique-by [:account/id])
                 (dissoc :created-at :updated-at)))))

    (testing "upsert that inserts with qualified keyword map"
      (is (= {:name "upsert1" :email "upsert1@test.com" :password "upsert1" :id 7}
             (-> (db/upsert conn
                    #:account{:name "upsert1" :email "upsert1@test.com" :password "upsert1" :id 7}
                    :unique-by [:account/id])
                 (dissoc :created-at :updated-at)))))))

(deftest upsert-all-test
  (let [conn (db/connect (db/context :test))]
    (testing "upsert-all with nested map"
      (is (= '({:id 3 :name "name3" :email "name3@test.com" :password "password"}
               {:id 4 :name "name4" :email "name4@test.com" :password "password"})
             (->> (db/upsert-all conn
                    {:account [{:name "name3" :email "name3@test.com" :id 3}
                               {:name "name4" :email "name4@test.com" :id 4}]}
                    :unique-by [:id])
                  (map #(dissoc % :created-at :updated-at))))))

    (testing "upsert-all with qualified keyword map"
      (is (= '({:id 5 :name "name5" :email "email@email.com" :password "password"}
               {:id 6 :name "name6" :email "email@email.com" :password "password"})
             (->> (db/upsert-all conn
                    [#:account{:name "name5" :id 5}
                     #:account{:name "name6" :id 6}]
                    :unique-by [:account/id])
                  (map #(dissoc % :created-at :updated-at))))))))


(deftest core-delete-test
  (let [conn (db/connect (db/context :test))]
    (testing "db.core/delete with nested map"
      (db/insert conn {:account {:name "delete-me"}})
      (is (= {:name "delete-me" :email nil :password nil}
             (dissoc
              (db/delete conn {:account {:name "delete-me"}})
              :created-at :updated-at :id))))

    (testing "db.core/delete with qualified keyword map"
      (db/insert conn {:account/name "delete-me-too"})
      (is (= {:name "delete-me-too" :email nil :password nil}
             (dissoc
              (db/delete conn {:account/name "delete-me-too"})
              :created-at :updated-at :id))))))


(deftest core-delete-all-test
  (let [conn (db/connect (db/context :test))]
    (testing "db.core/delete-all with nested map"
      (db/insert-all conn {:account [{:name "delete1"} {:name "delete2"}]})
      (is (= '({:name "delete1" :email nil :password nil}
               {:name "delete2" :email nil :password nil})
             (->> (db/delete-all conn {:account [{:name "delete1"}
                                                 {:name "delete2"}]})
                  (map #(dissoc % :created-at :updated-at :id))))))

    (testing "db.core/delete-all with qualified keyword map"
      (db/insert-all conn {:account [{:name "delete3"} {:name "delete4"}]})
      (is (= '({:name "delete3" :email nil :password nil}
               {:name "delete4" :email nil :password nil})
             (->> (db/delete-all conn [{:account/name "delete3"}
                                       {:account/name "delete4"}])
                  (map #(dissoc % :created-at :updated-at :id))))))))
