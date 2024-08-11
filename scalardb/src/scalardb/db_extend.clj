(ns scalardb.db-extend
  (:require [cassandra.core :as cassandra]
            [clojure.string :as string]
            [jepsen.db :as db]
            [scalardb.db.postgres :as postgres])
  (:import (com.scalar.db.storage.cassandra CassandraAdmin
                                            CassandraAdmin$ReplicationStrategy
                                            CassandraAdmin$CompactionStrategy)
           (java.io FileInputStream)
           (java.util Properties)))

(def ^:private ISOLATION_LEVELS {:snapshot "SNAPSHOT"
                                 :serializable "SERIALIZABLE"})

(def ^:private SERIALIZABLE_STRATEGIES {:extra-read "EXTRA_READ"
                                        :extra-write "EXTRA_WRITE"})

(defn- load-config
  [test]
  (when-let [path (and (seq (:config-file test)) (:config-file test))]
    (let [props (Properties.)]
      (with-open [stream (FileInputStream. path)]
        (.load props stream))
      props)))

(defn- set-common-properties
  [test properties]
  (doto properties
    (.setProperty "scalar.db.consensus_commit.isolation_level"
                  ((:isolation-level test) ISOLATION_LEVELS))
    (.setProperty "scalar.db.consensus_commit.serializable_strategy"
                  ((:serializable-strategy test) SERIALIZABLE_STRATEGIES))
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.enabled"
                  (str (:enable-group-commit test)))
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.slot_capacity" "4")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.old_group_abort_timeout_millis" "15000")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.delayed_slot_move_timeout_millis" "400")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.metrics_monitor_log_enabled" "true")))

(defprotocol DbExtension
  (live-nodes [this test])
  (wait-for-recovery [this test])
  (create-table-opts [this test])
  (create-properties [this test]))

(defrecord ExtCassandra []
  DbExtension
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
    (or (load-config test)
        (let [nodes (:nodes test)]
          (when (nil? nodes)
            (throw (ex-info "No living node" {:test test})))
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.storage" "cassandra")
                 (.setProperty "scalar.db.contact_points"
                               (string/join "," nodes))
                 (.setProperty "scalar.db.username" "cassandra")
                 (.setProperty "scalar.db.password" "cassandra"))
               (set-common-properties test))))))

(defrecord ExtPostgres []
  DbExtension
  (live-nodes [_ test] (postgres/live-node? test))
  (wait-for-recovery [_ test] (postgres/wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (load-config test)
        (let [node (-> test :nodes first)]
          ;; We have only one node in this test
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.storage" "jdbc")
                 (.setProperty "scalar.db.contact_points"
                               (str "jdbc:postgresql://" node ":5432/"))
                 (.setProperty "scalar.db.username" "postgres")
                 (.setProperty "scalar.db.password" "postgres"))
               (set-common-properties test))))))

(def ^:private ext-dbs
  {:cassandra (->ExtCassandra)
   :postgres (->ExtPostgres)})

(defn extend-db
  [db db-type]
  (let [ext-db (db-type ext-dbs)]
    (reify
      db/DB
      (setup! [_ test node] (db/setup! db test node))
      (teardown! [_ test node] (db/teardown! db test node))
      db/Primary
      (primaries [_ test] (db/primaries db test))
      (setup-primary! [_ test node] (db/setup-primary! db test node))
      db/Pause
      (pause! [_ test node] (db/pause! db test node))
      (resume! [_ test node] (db/resume! db test node))
      db/Kill
      (start! [_ test node] (db/start! db test node))
      (kill! [_ test node] (db/kill! db test node))
      db/LogFiles
      (log-files [_ test node] (db/log-files db test node))
      DbExtension
      (live-nodes [_ test] (live-nodes ext-db test))
      (wait-for-recovery [_ test] (wait-for-recovery ext-db test))
      (create-table-opts [_ test] (create-table-opts ext-db test))
      (create-properties [_ test] (create-properties ext-db test)))))
