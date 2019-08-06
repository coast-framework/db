# db
Clojure SQL Superpowers

## Install

Add this thing to your `deps.edn` file and your sql lib of choice

```clojure
{:deps {coast-framework/db {:git/url "" :sha ""}
        org.xerial/sqlite-jdbc {:mvn/version "3.25.2"}}}
```

## Docs

There are some pretty comprehensive docs available for this monster library

[Read the docs]()

## Quickstart

This library handles everything you need for database management in a web application.
Consider this section more of a crash course than an easy snippet to copy.

### Create a database

Create a sqlite database, the process is similar for a postgres database.
First create a `db.edn` file in the root of your project or in your `resources`
folder for you uberjar-ers out there.

```sh
cd your-project-folder && touch db.edn
```

Fill that `db.edn` in:

```clojure
{:dev {:database "usability_dev.sqlite3"
       :adapter "sqlite"
       :debug true}

 :test {:database "usability_test.sqlite3"
        :adapter "sqlite"
        :debug true}

 :prod {:database "usability.sqlite3"
        :adapter "sqlite"}}
```

Now we're ready to create the `:dev` database:

```clojure
(require '[db.core :as db])

(db/create (db/context :dev))
```

### Migrations

Unlike other clojure sql libraries, this one also does migrations!
Create a migration like this:

```clojure
(let [ctx (db/context :dev)]
  (db/migration "create-table-account" "name:text" "email:text" "password:text"))
```

This creates a new folder in your project, `db` and it also creates a `migrations` subfolder
in that folder with a file named something like this `20190725281234_create_table_account.clj` that looks like this:

```clojure
(ns 20190725281234-create-table-account
  (:require [db.migrator.helper :refer :all]))

(create-table :account
  (text :name :null false)
  (text :email :unique true :null false)
  (text :password :null false))
```

I took the liberty of adding the `:null false` and `:unique true` bits.

Connect to the database and run the connection:

```clojure
(def conn (db/connect (db/context :dev)))
```

Go ahead and run that migration

```clojure
(db/migrate conn)
```

It's just that easy. If you make a mistake don't forget to rollback

```clojure
(db/rollback conn)
```

### Insert rows

Inserts, updates and deletes are designed to be easy, not simple

```clojure
(db/insert conn {:account {:name "name" :email "a@b.com" :password "pw"}})
```

Insert two or more records

```clojure
(db/insert-all conn {:account [{:name "name1" :email "c@d.com" :password "pw"}
                               {:name "name2" :email "e@f.com" :password "pw"}]})
```

### Queries

There are a few ways to query things.
You could get a single row by id

```clojure
(db/fetch conn [:account 1]) ; => {:name "name" :email "a@b.com" :password "pw"}
```

or you could get rows by table name

```clojure
(db/fetch conn [:account]) ; => [{:name "name" ...} {:name "name1" ...} ...]
```

There's also rows by where clause

```clojure
(db/from conn {:account {:email "a@b.com" :name "name"}})
; => ["select * from account where email = ? and name = ?", "a@b.com", "name"]

; qualified keywords work too
(db/from conn {:account/email "a@b.com" :account/name "name"})
; => ["select * from account where email = ? and name = ?", "a@b.com" "name"]
```

Or a more complex query

```clojure
(db/q '[:select *
        :from account
        :where email = ?email :or name = ?name
        :order-by id
        :limit 10]
  {:email "a@b.com" :name "name1"})
```

### Update and deletes

Update that last one, oh and you don't need nested maps, qualified keywords work too

```clojure
(db/update conn {:account/name "name3"} {:email "e@f.com"})
; => ["update account set name = ? where email = ?", "name3", "e@f.com"]
```

Delete the last two, dotted keywords work too

```clojure
(db/delete-all conn {:account.email ["e@f.com" "c@d.com"]})
; => ["delete from account where email in (?, ?)", "e@f.com", "c@d.com"]
```

There's a lot more where that came from.

## WAT

What is this monster lib that doesn't follow clojure conventions of small libs?

This is coast's database library and it handles the following:

- Database management (Dropping/Creating new databases)
- Associations (Similar to rails' has-many/belongs-to model definitions)
- Migrations
- SQL Helpers
- Connection Pooling

You either die a small library or you live long enough to become a big one.
