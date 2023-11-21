(ns scalardb.db-extend-test
  (:require [cassandra.core :as cassandra]
            [clojure.test :refer [deftest is]]
            [scalardb.db-extend :as ext]
            [spy.core :as spy]))

(deftest create-properties-test
  (with-redefs [cassandra/live-nodes (spy/stub ["n1" "n2" "n3"])]
    (let [db (ext/extend-db (cassandra/db) :cassandra)
          properties (ext/create-properties db
                                            {:isolation-level :serializable
                                             :serializable-strategy :extra-write})]
      (is (= "n1,n2,n3"
             (.getProperty properties "scalar.db.contact_points")))
      (is (= "cassandra"
             (.getProperty properties "scalar.db.username")))
      (is (= "cassandra"
             (.getProperty properties "scalar.db.password")))
      (is (= "SERIALIZABLE"
             (.getProperty
              properties
              "scalar.db.consensus_commit.isolation_level")))
      (is (= "EXTRA_WRITE"
             (.getProperty
              properties
              "scalar.db.consensus_commit.serializable_strategy"))))))
