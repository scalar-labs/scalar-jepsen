(ns scalardb.db.cassandra
  (:require [cassandra.core :as cassandra]
            [cassandra.nemesis :as cn]
            [cassandra.runner :as cr]
            [clojure.string :as string]
            [scalardb.db-extend :as ext])
  (:import (com.scalar.db.storage.cassandra CassandraAdmin
                                            CassandraAdmin$ReplicationStrategy
                                            CassandraAdmin$CompactionStrategy)
           (java.util Properties)))

(defrecord ExtCassandra []
  ext/DbExtension
  (live-nodes [_ test] (cassandra/live-nodes test))
  (wait-for-recovery [_ test] (cassandra/wait-rf-nodes test))
  (create-table-opts
    [_ test]
    {(keyword CassandraAdmin/REPLICATION_STRATEGY)
     (str CassandraAdmin$ReplicationStrategy/SIMPLE_STRATEGY)
     (keyword CassandraAdmin/COMPACTION_STRATEGY)
     (str CassandraAdmin$CompactionStrategy/LCS)
     (keyword CassandraAdmin/REPLICATION_FACTOR) (:rf test)})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [nodes (:nodes test)]
          (when (nil? nodes)
            (throw (ex-info "No living node" {:test test})))
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.storage" "cassandra")
                 (.setProperty "scalar.db.contact_points"
                               (string/join "," nodes))
                 (.setProperty "scalar.db.username" "cassandra")
                 (.setProperty "scalar.db.password" "cassandra"))
               (ext/set-common-properties test)))))
  (create-storage-properties [this test] (ext/create-properties this test)))

(defn gen-db
  [faults admin]
  (let [db (ext/extend-db (cassandra/db) (->ExtCassandra))
        ;; replace :kill nemesis with :crash for Cassandra
        faults (mapv #(if (= % :kill) :crash %) faults)]
    (when-not (every? #(some? (get cr/nemeses (name %))) faults)
      (throw
       (ex-info
        (str "Invalid nemesis for Cassandra: " faults) {})))
    [db
     (cn/nemesis-package
      {:db db
       :faults faults
       :admin admin
       :partition {:targets [:one
                             :primaries
                             :majority
                             :majorities-ring
                             :minority-third]}})
     Integer/MAX_VALUE]))
