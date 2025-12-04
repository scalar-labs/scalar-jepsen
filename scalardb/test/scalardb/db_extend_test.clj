(ns scalardb.db-extend-test
  (:require [clojure.test :refer [deftest is]]
            [scalardb.db.postgres :as postgres]
            [scalardb.db-extend :as ext]))

(deftest create-properties-test
  (let [db (ext/extend-db postgres/db (postgres/->ExtPostgres))
        properties (ext/create-properties
                    db
                    {:nodes ["n1"]
                     :isolation-level :serializable
                     :jdbc-isolation-level :read-committed})]
    (is (= "jdbc:postgresql://n1:5432/"
           (.getProperty properties "scalar.db.contact_points")))
    (is (= "postgres"
           (.getProperty properties "scalar.db.username")))
    (is (= "postgres"
           (.getProperty properties "scalar.db.password")))
    (is (= "SERIALIZABLE"
           (.getProperty
            properties
            "scalar.db.consensus_commit.isolation_level")))
    (is (= "READ_COMMITTED"
           (.getProperty properties "scalar.db.jdbc.isolation_level")))))
