(ns watch
  (:require [clojure-watch.core :as watch]
            [clojure.java.shell]))


(defn -main [& args]
  (watch/start-watch
   [{:path "test"
     :event-types [:create :modify]
     :bootstrap (fn [path] (println "Watching" path))
     :callback (fn [event filename]
                 (println
                  (:out
                   (clojure.java.shell/sh "clj" "-Atest"))))
     :options {:recursive true}}])

  (watch/start-watch
   [{:path "src"
     :event-types [:create :modify]
     :bootstrap (fn [path] (println "Watching" path))
     :callback (fn [event filename]
                 (println
                  (:out
                   (clojure.java.shell/sh "clj" "-Atest"))))
     :options {:recursive true}}]))
