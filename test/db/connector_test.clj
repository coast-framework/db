(ns db.connector-test
  (:require [db.connector :as connector]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [db.core])
  (:import (java.io FileNotFoundException)))

(def context {:adapter "sqlite" :database "test.sqlite3"})

(defn with-db [f]
  (f)
  (Thread/sleep 100)
  (db.core/drop context)
  (when (.exists (io/file "db.edn"))
    (io/delete-file "db.edn")))

(use-fixtures :once with-db)

(deftest context-test
  (testing "db-edn without a db.edn"
    (when (.exists (io/file "db/db.edn"))
      (io/delete-file "db/db.edn"))

    (is (thrown? FileNotFoundException (connector/db-edn))))

  (testing "with db.edn"
    (spit "db/db.edn" "{:dev {:adapter \"sqlite\"}}")
    (is (= "sqlite" (:adapter (connector/context)))))

  (testing "with db.edn and :test"
    (spit "db/db.edn" "{:test {:adapter \"postgres\"}}")
    (is (= "postgres" (:adapter (connector/context :test))))))


(deftest pool-test
  (testing "pool with sqlite"
    (is (true? (contains? (connector/pool context)
                 :datasource)))))


(deftest connect-test
  (testing "connect with sqlite"
    (is (true? (contains?
                 (connector/connect context)
                 :datasource)))))


(deftest disconnect-test
  (testing "disconnecting with sqlite"
    (let [connection (connector/connect context)]
      (is (nil? (connector/disconnect connection))))))
