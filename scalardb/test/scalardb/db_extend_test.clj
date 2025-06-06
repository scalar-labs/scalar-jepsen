(ns scalardb.db-extend-test
  (:require [cassandra.core :as cassandra]
            [clojure.test :refer [deftest is]]
            [scalardb.db-extend :as ext]))

(deftest create-properties-test
  (let [db (ext/extend-db (cassandra/db) :cassandra)
        properties (ext/create-properties db
                                          {:nodes ["n1" "n2" "n3"]
                                           :isolation-level :serializable})]
    (is (= "n1,n2,n3"
           (.getProperty properties "scalar.db.contact_points")))
    (is (= "cassandra"
           (.getProperty properties "scalar.db.username")))
    (is (= "cassandra"
           (.getProperty properties "scalar.db.password")))
    (is (= "SERIALIZABLE"
           (.getProperty
            properties
            "scalar.db.consensus_commit.isolation_level")))))
