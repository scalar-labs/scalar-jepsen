(ns scalardb.db-extend
  (:require [clojure.string :as string]
            [jepsen.db :as db])
  (:import (java.io FileInputStream)
           (java.util Properties)))

(defn load-config
  [test]
  (when-let [path (and (seq (:config-file test)) (:config-file test))]
    (let [props (Properties.)]
      (with-open [stream (FileInputStream. path)]
        (.load props stream))
      props)))

(defn set-common-properties
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

(defn extend-db
  [db ext-db]
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
      (create-storage-properties ext-db test))))
