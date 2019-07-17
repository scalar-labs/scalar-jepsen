(ns scalardl.cassandra
  (:require [clojure.tools.logging :refer [debug info warn]]
            [cassandra.core :as cassandra]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]))

(def ^:private ^:const TX_COMMITTED 3)
(def ^:private ^:const TX_ABORTED 4)

(defn check-tx-state
  "Return true/false when the transaction has been committed or aborted.
  Return nil when it can't read the state from the coordinator table"
  [txid {:keys [cass-nodes]}]
  (let [cluster (alia/cluster {:contact-points cass-nodes})
        state (some-> (try (alia/execute (alia/connect cluster)
                                         (select :coordinator.state (where {:tx_id txid}))
                                         {:consistency :serial})
                           (catch Exception _
                             (warn "Failed to read the coordinator table"))
                           (finally (alia/shutdown cluster)))
                      first :tx_state)]
    (if state (= state TX_COMMITTED) nil)))

(defn spinup-cassandra!
  [node test]
  (when (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
    (cassandra/wipe! node))
  (doto node
    (cassandra/install! test)
    (cassandra/configure! test)
    (cassandra/wait-turn test)
    (cassandra/guarded-start! test)))

(defn teardown-cassandra!
  [node]
  (when-not (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
    (cassandra/wipe! node)))

(defn create-tables
  [{:keys [cass-nodes rf]}]
  (info "creating tables")
  (let [session (alia/connect (alia/cluster {:contact-points cass-nodes}))]
    (alia/execute session (create-keyspace :scalar
                                           (if-exists false)
                                           (with {:replication {"class"              "SimpleStrategy"
                                                                "replication_factor" rf}})))

    (alia/execute session (create-keyspace :coordinator
                                           (if-exists false)
                                           (with {:replication {"class"              "SimpleStrategy"
                                                                "replication_factor" rf}})))

    (alia/execute session (create-table :scalar.asset
                                        (if-exists false)
                                        (column-definitions {:id                     :text
                                                             :age                    :int
                                                             :argument               :text
                                                             :before_argument        :text
                                                             :before_contract_id     :text
                                                             :before_hash            :blob
                                                             :before_input           :text
                                                             :before_output          :text
                                                             :before_prev_hash       :blob
                                                             :before_signature       :blob
                                                             :before_tx_committed_at :bigint
                                                             :before_tx_id           :text
                                                             :before_tx_prepared_at  :bigint
                                                             :before_tx_state        :int
                                                             :before_tx_version      :int
                                                             :contract_id            :text
                                                             :hash                   :blob
                                                             :input                  :text
                                                             :output                 :text
                                                             :prev_hash              :blob
                                                             :signature              :blob
                                                             :tx_committed_at        :bigint
                                                             :tx_id                  :text
                                                             :tx_prepared_at         :bigint
                                                             :tx_state               :int
                                                             :tx_version             :int
                                                             :primary-key            [:id :age]})
                                        (with {:compaction {:class :LeveledCompactionStrategy}})))

    (alia/execute session (create-table :scalar.asset_metadata
                                        (if-exists false)
                                        (column-definitions {:asset_id    :text
                                                             :latest_age  :int
                                                             :primary-key [:asset_id]})
                                        (with {:compaction {:class :LeveledCompactionStrategy}})))

    (alia/execute session (create-table :scalar.contract
                                        (if-exists false)
                                        (column-definitions {:id             :text
                                                             :cert_holder_id :text
                                                             :cert_version   :int
                                                             :binary_name    :text
                                                             :properties     :text
                                                             :registered_at  :bigint
                                                             :signature      :blob
                                                             :primary-key    [:cert_holder_id :cert_version :id]})
                                        (with {:compaction {:class :LeveledCompactionStrategy}})))
    (alia/execute session (create-index :scalar.contract :id (if-exists false)))

    (alia/execute session (create-table :scalar.contract_class
                                        (if-exists false)
                                        (column-definitions {:binary_name :text
                                                             :byte_code   :blob
                                                             :primary-key [:binary_name]})))

    (alia/execute session (create-table :scalar.certificate
                                        (if-exists false)
                                        (column-definitions {:holder_id     :text
                                                             :version       :int
                                                             :pem           :text
                                                             :registered_at :bigint
                                                             :primary-key   [:holder_id :version]})))

    (alia/execute session (create-table :coordinator.state
                                        (if-exists false)
                                        (column-definitions {:tx_id         :text
                                                             :tx_state      :int
                                                             :tx_created_at :bigint
                                                             :primary-key   [:tx_id]})))
    (alia/shutdown session)))