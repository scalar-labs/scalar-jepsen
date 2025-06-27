(ns scalardb.db-extend
  (:require [cassandra.core :as cassandra]
            [clojure.string :as string]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [scalardb.db.cluster :as cluster]
            [scalardb.db.postgres :as postgres])
  (:import (com.scalar.db.storage.cassandra CassandraAdmin
                                            CassandraAdmin$ReplicationStrategy
                                            CassandraAdmin$CompactionStrategy)
           (java.io FileInputStream)
           (java.util Properties)))

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
                  (-> test :isolation-level name string/upper-case (string/replace #"-" "_")))
    (.setProperty "scalar.db.consensus_commit.one_phase_commit.enabled"
                  (str (:enable-one-phase-commit test)))
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.enabled"
                  (str (:enable-group-commit test)))
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.slot_capacity" "4")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.old_group_abort_timeout_millis" "15000")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.delayed_slot_move_timeout_millis" "400")
    (.setProperty "scalar.db.consensus_commit.coordinator.group_commit.metrics_monitor_log_enabled" "true")
    (.setProperty "scalar.db.consensus_commit.include_metadata.enabled" "true")))

(defprotocol DbExtension
  (get-db-type [this])
  (live-nodes [this test])
  (wait-for-recovery [this test])
  (create-table-opts [this test])
  (create-properties [this test])
  (create-storage-properties [this test]))

(defrecord ExtCassandra []
  DbExtension
  (get-db-type [_] :cassandra)
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
               (set-common-properties test)))))
  (create-storage-properties [this test] (create-properties this test)))

(defrecord ExtPostgres []
  DbExtension
  (get-db-type [_] :postgres)
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
               (set-common-properties test)))))
  (create-storage-properties [this test] (create-properties this test)))

(defrecord ExtCluster []
  DbExtension
  (get-db-type [_] :cluster)
  (live-nodes [_ test] (cluster/running-pods? test))
  (wait-for-recovery [_ test] (cluster/wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (load-config test)
        (let [node (-> test :nodes first)
              ip (c/on node (cluster/get-load-balancer-ip))]
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.transaction_manager" "cluster")
                 (.setProperty "scalar.db.contact_points"
                               (str "indirect:" ip))
                 (.setProperty "scalar.db.sql.connection_mode" "cluster")
                 (.setProperty "scalar.db.sql.cluster_mode.contact_points"
                               (str "indirect:" ip)))
               (set-common-properties test)))))
  (create-storage-properties [_ _]
    (let [node (-> test :nodes first)
          ip (c/on node (cluster/get-postgres-ip))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5432/postgres"))
        (.setProperty "scalar.db.username" "postgres")
        (.setProperty "scalar.db.password" "postgres")))))

(def ^:private ext-dbs
  {:cassandra (->ExtCassandra)
   :postgres (->ExtPostgres)
   :cluster (->ExtCluster)})

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
      (get-db-type [_] (get-db-type ext-db))
      (live-nodes [_ test] (live-nodes ext-db test))
      (wait-for-recovery [_ test] (wait-for-recovery ext-db test))
      (create-table-opts [_ test] (create-table-opts ext-db test))
      (create-properties [_ test] (create-properties ext-db test))
      (create-storage-properties
        [_ test]
        (create-storage-properties ext-db test)))))
