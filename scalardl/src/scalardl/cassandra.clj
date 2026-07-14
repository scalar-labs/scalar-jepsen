(ns scalardl.cassandra
  (:require [clojure.string :as string]
            [clojure.tools.logging :refer [info]]
            [cassandra.core :as cassandra]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :as clause]
            [qbits.hayt.dsl.statement :as st])
  (:import (com.scalar.db.schemaloader SchemaLoader)
           (com.scalar.db.storage.cassandra CassandraAdmin
                                            CassandraAdmin$ReplicationStrategy)
           (java.util Properties)))

(def ^:private ^:const SCHEMA_URL
  "https://raw.githubusercontent.com/scalar-labs/scalardl/master/schema-loader/ledger-schema.json")

(def ^:private ^:const TX_COMMITTED 3)

(defn cassandra-log
  [test]
  (cassandra/cassandra-log test))

(defn wait-cassandra
  [test]
  (cassandra/wait-rf-nodes test))

(defn committed?
  "Return true/false when the transaction has been committed or aborted"
  [txid {:keys [cass-nodes]}]
  (let [session (alia/session {:contact-points (mapv #(str %1 ":9042") cass-nodes)
                               :load-balancing-local-datacenter "datacenter1"})
        rows (try (alia/execute session
                                (st/select :coordinator.state
                                           (clause/where {:tx_id txid}))
                                {:consistency-level :serial})
                  (catch Exception e (throw e))
                  (finally (cassandra/close-cassandra session)))]
    (= (-> rows first :tx_state) TX_COMMITTED)))

(defn spinup-cassandra!
  [node test]
  (when (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
    (cassandra/wipe! test node))
  (doto node
    (cassandra/install! test)
    (cassandra/configure! test)
    (cassandra/wait-turn test)
    (cassandra/guarded-start! test)))

(defn teardown-cassandra!
  [node test]
  (when-not (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
    (cassandra/wipe! test node)))

(defn- create-properties
  [test]
  (doto (Properties.)
    (.setProperty "scalar.db.storage" "cassandra")
    (.setProperty "scalar.db.contact_points" (string/join "," (:cass-nodes test)))
    (.setProperty "scalar.db.username" "cassandra")
    (.setProperty "scalar.db.password" "cassandra")))

(defn- create-table-opts
  [test]
  {CassandraAdmin/REPLICATION_STRATEGY
   (str CassandraAdmin$ReplicationStrategy/SIMPLE_STRATEGY)
   CassandraAdmin/REPLICATION_FACTOR (str (:rf test))})

(defn create-tables
  [test]
  (info "creating tables")
  (let [schema (slurp SCHEMA_URL)]
    (SchemaLoader/load (create-properties test)
                       schema
                       (create-table-opts test)
                       true)))
