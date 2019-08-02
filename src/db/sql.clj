(ns db.sql
  (:require [helper.core :as helper]
            [clojure.string :as string])
  (:refer-clojure :exclude [update]))


(defn postgres? [ctx]
  (= (:adapter ctx) "postgres"))


(defn table-map? [m]
  (and (map? m)
    (let [first-val (first (vals m))]
      (coll? first-val))))


(defn table [val]
  (cond
    (keyword? val) (helper/sqlize val)

    (table-map? val) (-> val ffirst name helper/sqlize)

    (and (map? val)
      (every? qualified-ident? (keys val)))
    (-> val keys first namespace helper/sqlize)

    :else nil))


(defn params [m]
  (cond
    (table-map? m) (-> m vals first)
    (map? m) m
    :else nil))


(defn columns [m]
  (cond
    (table-map? m) (->> m vals first keys (map helper/sqlize))

    (and (map? m)
      (every? qualified-ident? (keys m)))
    (->> m keys (map name) (map helper/sqlize))

    (map? m) (->> m keys (map helper/sqlize))

    :else nil))


(defn placeholders [m]
  (cond
    (table-map? m) (->> m vals first helper/vectorize (mapcat keys) (map (fn [_] "?")))
    (map? m) (->> m keys (map (fn [_] "?")))
    :else nil))


(defn values [m]
  (cond
    (table-map? m) (->> m vals first vals)
    (map? m) (vals m)
    :else nil))


(defn op [[key val]]
  (cond
    (nil? val) (format "%s is null" (helper/sqlize key))
    (sequential? val) (format "%s%s%s%s"
                        (if (not (empty? (filter nil? val)))
                          "("
                          "")
                        (if (empty? (filter some? val))
                          "1 = 1"
                          (helper/sqlize key))
                        (if (not (empty? (filter some? val)))
                          (->> (filter some? val)
                               (map (fn [_] "?"))
                               (string/join ", ")
                               (format " in (%s)"))
                          "")
                        (if (not (empty? (filter nil? val)))
                          (format " or %s is null)" (helper/sqlize key))
                          ""))
    :else (format "%s = ?" (helper/sqlize key))))


(defn where
  "Takes a map or a vector and returns the where portion of a sql vec"
  [val]
  (cond
    (vector? val) val
    (map? val) (let [pairs (map identity val)
                     columns (->> (map op pairs)
                                  (filter (comp not string/blank?)))
                     values (->> (map second pairs)
                                 (helper/flat)
                                 (filter some?))]
                  (apply conj
                    [(string/join " and " columns)]
                    (filter some? values)))
    :else nil))


(defn insert
  "Takes a db context and map and returns an insert java.jdbc sql vec"
  ([ctx m]
   (let [table (table m)
         columns (string/join ", " (columns m))
         placeholders (string/join ", " (placeholders m))
         values (values m)
         returning (if (postgres? ctx)
                     " returning *"
                     "")]
     (apply conj
      [(format "insert into %s (%s) values (%s)%s" table columns placeholders returning)]
      values))))


(defn insert-all
  "Takes a db context and a sequential? of maps and returns an insert java.jdbc sql vec"
  [ctx maps]
  (let [m (if (table-map? maps)
            (-> maps vals first)
            maps)
        table (if (table-map? maps)
                (table maps)
                (table (first maps)))
        columns (string/join ", " (columns (first m)))
        placeholders (string/join ", " (map #(format "(%s)" (->> (placeholders %) (string/join ", "))) m))
        values (mapcat values m)
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
      [(format "insert into %s (%s) values %s%s"
         table columns placeholders returning)]
      values)))


(defn update
  "Takes a db context, a map representing the row to be updated
   and a where clause in the form of a map or a sql vec"
  ([ctx m where-clause all?]
   (let [table (table m)
          columns (columns m)
          placeholders (placeholders m)
          values (values m)
          where-clause (where where-clause)
          where-sql (first where-clause)
          where-values (rest where-clause)
          returning (if (postgres? ctx)
                      " returning *"
                      "")
          set-columns (string/join ", "
                        (map
                          #(string/join " = " %)
                          (partition 2
                            (interleave columns placeholders))))
          where-in (if all?
                     where-sql
                     (format "id in (select id from %s where %s limit 1)" table where-sql))]
      (apply conj
        [(format "update %s set %s where %s%s"
           table set-columns where-in returning)]
        (concat values where-values))))
  ([ctx m where-clause]
   (update ctx m where-clause false)))


(comment
  (update* {} {:account {:a "a" :b "b"}} ["id = ?" 1])
  (update* {} {:account {:a "a" :b "b"}} {:id 1})
  (update* {} #:account{:a "a" :b "b"} {:id 1}))


(defn update-all [ctx m where-clause]
  (update ctx m where-clause true))


(comment
  (update-all {} {:account {:a "a" :b "b"}} {:id [1 2 3] :name ["hello" nil]}))


(defn upsert [ctx m & {:as options}]
  (let [table (table m)
        columns (columns m)
        columns-sql (string/join ", " columns)
        placeholders (string/join ", " (placeholders m))
        values (values m)
        where-clause (where (select-keys (get m (keyword table) m) (helper/vectorize (:unique-by options))))
        where-sql (first where-clause)
        where-values (rest where-clause)
        conflict (string/join "," (map helper/sqlize (helper/vectorize (:unique-by options))))
        set-columns (string/join ", "
                      (map
                        #(string/join " = " %)
                        (partition 2
                          (interleave columns (map #(str "excluded." %) columns)))))
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
     [(format "insert into %s (%s) values (%s) on conflict(%s) do update set %s where id in (select id from %s where %s limit 1)%s"
        table columns-sql placeholders conflict set-columns
        table where-sql returning)]
     (concat values where-values))))


(comment

  (upsert {:adapter "sqlite"}
    {:account {:email "email" :password "password" :id 1}}
    :unique-by :id))


(defn upsert-all [ctx maps & {:as options}]
  (let [m (if (table-map? maps)
            (-> maps vals first)
            maps)
        table (if (table-map? maps)
                (table maps)
                (table (first maps)))
        columns (columns (first m))
        columns-sql (string/join ", " columns)
        placeholders (string/join ", " (map #(format "(%s)" (->> (placeholders %) (string/join ", "))) m))
        values (mapcat values m)
        conflict (string/join "," (map helper/sqlize (helper/vectorize (:unique-by options))))
        set-columns (string/join ", "
                      (map
                        #(string/join " = " %)
                        (partition 2
                          (interleave columns (map #(str "excluded." %) columns)))))
        returning (if (postgres? ctx)
                    " returning *"
                    "")]
    (apply conj
     [(format "insert into %s (%s) values %s on conflict(%s) do update set %s%s"
        table columns-sql placeholders conflict set-columns returning)]
     values)))

(comment
  (upsert-all {:adapter "sqlite"}
    {:account [{:email "email" :password "password" :id 1}
               {:email "email1" :password "password1" :id 2}
               {:email "email2" :password "password2" :id 3}]}
    :unique-by [:id])

  (upsert-all {:adapter "sqlite"}
    [#:account{:email "email" :password "password" :id 1}
     #:account{:email "email1" :password "password1" :id 2}
     #:account{:email "email2" :password "password2" :id 3}]
    :unique-by [:id]))

(defn delete
  "Takes a db context, a table name, a where clause and a bool and returns a java.jdbc delete statement that deletes one or many rows depending on truthiness of one?"
  ([ctx where-clause one?]
   (let [table (table where-clause)
         where-clause (where (params where-clause))
         where-sql (first where-clause)
         params (rest where-clause)
         delete-where (if one?
                       (format "id in (select id from %s where %s limit 1)" table where-sql)
                       where-sql)
         delete-where (if (postgres? ctx)
                        (str delete-where " returning *")
                        delete-where)]

    (apply conj
     [(format "delete from %s where %s" table delete-where)]
     params)))
  ([ctx where-clause]
   (delete ctx where-clause true)))


(defn delete-all
  "Takes a db context, a table name, and a where clause and returns a java.jdbc delete statement that deletes many rows"
  [ctx where-clause]
  (delete ctx where-clause false))
  ; ([ctx nested-map]
  ;  (delete-all ctx (ffirst nested-map) (apply (partial merge-with (comp distinct helper/flat vector)) (-> nested-map vals first)))))

;
; (comment
;   (delete nil :account ["id = ?" 1]) ; this only deletes one row with "delete from account where id (select id from account <clause> limit 1)"
;   (delete nil {:account {:id 1 :name nil}})
;
;   (delete nil :account ["id = ? or id is null" 1])
;   (delete nil {:account {:id 1 :name ["a" "b" nil nil] :a nil}})
;
;   ; this deletes multiple rows
;   (delete-all nil :account ["id in (?, ?, ?)" 1 2 3]) ; or
;   (delete-all nil {:account [{:id 1 :name "name 1"} {:id 2 :name nil}]})
;
;
;   ; updates one row
;   (coast/update db :account ["id = ?" 1])
;   (coast/update db {:account {:id 1 :name nil}})
;
;   ; updates multiple rows
;   (coast/update-all db :account ["id = ?" 1])
;   (coast/update-all db {:account {:id 1 :name nil}})
;
;   ; one row
;   (coast/insert db {:account {:id 1 :name nil}})
;
;   ; multi row
;   (coast/insert-all db {:account [{:id 1 :name nil}
;                                   {:id 2 :name nil}]})
;
;   ; query
;   (coast/q db
;     '[:select *
;       :from account
;       :where {:id ?id :name ?name :hello ?hello}
;       :order :id :desc :name
;       :limit 10
;       :offset 1]
;
;     {:name nil
;      :id 1
;      :hello "h"}) ; => {:account [{:name nil :id 1}]
;
;   (coast/q db
;     '[:select *
;       :from account
;       :join organizations tests
;       :where [id like ?id]
;              [name ?name]
;              [hello ?hello]
;       :order id desc name
;       :limit 10
;       :offset 1]
;
;     {:name nil
;      :id 1
;      :hello "h"})
;
;   (coast/q db
;     '[:select *
;       :from [:select id :from test :where test.name like ?test/name]
;       :join organizations tests
;       :where id like ?id :and
;             name ?name :and
;             hello ?hello
;       :order id desc name
;       :limit 10
;       :offset 1]
;
;     {:name nil
;      :id 1
;      :hello "h"})
;
;   (string/join ","
;     (map #(string/join " " %)(partition-all 2 [:id :desc :name])))
;
;   (helper/map-keys first
;     (into {}
;       (map vec
;        (partition 2
;         (partition-by keyword? [:id 'like "%hello%" :name '?name :hello '?hello])))))
;
;   ; Account.where(name: nil).delete_all
;   ; (coast/delete-all :account {:name nil})
;
;   ; Account.find(1).delete
;   ; (coast/fetch :account 1)
;   ; (coast/fetch :account)
;
;   (coast/delete
;    (coast/fetch :account 1))
;
;   (coast/fetch :account 1))
