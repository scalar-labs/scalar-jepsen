(ns scalardl.cassandra
  (:require [clojure.tools.logging :refer [debug info warn]]
            [cassandra.core :as cassandra]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]))

(def ^:private ^:const TX_COMMITTED 3)

(defn cassandra-log
  [test]
  (cassandra/cassandra-log test))

(defn committed?
  "Return true/false when the transaction has been committed or aborted"
  [txid {:keys [cass-nodes]}]
  (let [cluster (alia/cluster {:contact-points cass-nodes})
        rows (try (alia/execute (alia/connect cluster)
                                (select :coordinator.state
                                        (where {:tx_id txid}))
                                {:consistency :serial})
                  (catch Exception e (throw e))
                  (finally (alia/shutdown cluster)))]
    (and (not (empty? rows)) (= (-> rows first :tx_state) TX_COMMITTED))))

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

(defn create-tables
  [test]
  (info "creating tables")
  (let [cluster (alia/cluster {:contact-points (:cass-nodes test)})
        session (alia/connect cluster)]
    (doto session
      (cassandra/create-my-keyspace test {:keyspace "scalar"})
      (cassandra/create-my-table {:keyspace "scalar"
                                  :table "asset"
                                  :schema {:id                     :text
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
                                           :primary-key            [:id :age]}})
      (cassandra/create-my-table {:keyspace "scalar"
                                  :table "asset_metadata"
                                  :schema {:asset_id               :text
                                           :latest_age             :int
                                           :tx_committed_at        :bigint
                                           :tx_id                  :text
                                           :tx_prepared_at         :bigint
                                           :tx_state               :int
                                           :tx_version             :int
                                           :before_latest_age      :int
                                           :before_tx_committed_at :bigint
                                           :before_tx_id           :text
                                           :before_tx_prepared_at  :bigint
                                           :before_tx_state        :int
                                           :before_tx_version      :int
                                           :primary-key [:asset_id]}})

      (cassandra/create-my-table {:keyspace "scalar"
                                  :table "contract"
                                  :schema {:id             :text
                                           :cert_holder_id :text
                                           :cert_version   :int
                                           :binary_name    :text
                                           :properties     :text
                                           :registered_at  :bigint
                                           :signature      :blob
                                           :primary-key    [:cert_holder_id :cert_version :id]}})
      (alia/execute (create-index :scalar.contract :id (if-exists false)))
      (cassandra/create-my-table {:keyspace "scalar"
                                  :table "contract_class"
                                  :schema {:binary_name :text
                                           :byte_code   :blob
                                           :primary-key [:binary_name]}})

      (cassandra/create-my-table {:keyspace "scalar"
                                  :table "certificate"
                                  :schema {:holder_id     :text
                                           :version       :int
                                           :pem           :text
                                           :registered_at :bigint
                                           :primary-key   [:holder_id :version]}})

      (cassandra/create-my-keyspace test {:keyspace "coordinator"})
      (cassandra/create-my-table {:keyspace "coordinator"
                                  :table "state"
                                  :schema {:tx_id         :text
                                           :tx_state      :int
                                           :tx_created_at :bigint
                                           :primary-key   [:tx_id]}}))
    (cassandra/close-cassandra cluster session)))
