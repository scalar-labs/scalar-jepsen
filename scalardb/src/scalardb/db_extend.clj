(ns scalardb.db-extend
  (:require [cassandra.core :as cassandra]
            [clojure.string :as string]
            [jepsen.db :as db])
  (:import (com.scalar.db.storage.cassandra CassandraAdmin
                                            CassandraAdmin$ReplicationStrategy
                                            CassandraAdmin$CompactionStrategy)
           (java.util Properties)))

(def ^:private ISOLATION_LEVELS {:snapshot "SNAPSHOT"
                                 :serializable "SERIALIZABLE"})

(def ^:private SERIALIZABLE_STRATEGIES {:extra-read "EXTRA_READ"
                                        :extra-write "EXTRA_WRITE"})

(defprotocol DbExtension
  (live-nodes [this test])
  (create-table-opts [this test])
  (create-properties [this test]))

(def ^:private cassandra-functions
    {:live-nodes (fn [_ test] (cassandra/live-nodes test))
     :create-table-opts
     (fn [_ test]
       {(keyword CassandraAdmin/REPLICATION_STRATEGY)
        (str CassandraAdmin$ReplicationStrategy/SIMPLE_STRATEGY)
        (keyword CassandraAdmin/COMPACTION_STRATEGY)
        (str CassandraAdmin$CompactionStrategy/LCS)
        (keyword CassandraAdmin/REPLICATION_FACTOR) (:rf test)})
     :create-properties
     (fn [_ test]
       (let [nodes (cassandra/live-nodes test)]
         (when (nil? nodes)
           (throw (ex-info "No living node" {:test test})))
         (doto (Properties.)
           (.setProperty "scalar.db.contact_points" (string/join "," nodes))
           (.setProperty "scalar.db.username" "cassandra")
           (.setProperty "scalar.db.password" "cassandra")
           (.setProperty "scalar.db.isolation_level"
                         ((:isolation-level test) ISOLATION_LEVELS))
           (.setProperty "scalar.db.consensus_commit.serializable_strategy"
                         ((:serializable-strategy test) SERIALIZABLE_STRATEGIES)))))})

(def ext-db-functions
  {:cassandra cassandra-functions})

(defn extend-db
  [db db-type]
  (let [ext-fns (db-type ext-db-functions)]
  (reify
    db/DB
    (db/setup! [_ test node] (db/setup! db test node))
    (db/teardown! [_ test node] (db/teardown! db test node))
    db/Primary
    (primaries [_ test] (db/primaries db test))
    (setup-primary! [_ test node] (db/setup-primary! db test node))
    db/LogFiles
    (log-files [_ test node] (db/log-files db test node))
    DbExtension
    (live-nodes [_ test] ((:live-nodes ext-fns) db test))
    (create-table-opts [_ test] ((:create-table-opts ext-fns) db test))
    (create-properties [_ test] ((:create-properties ext-fns) db test)))))
