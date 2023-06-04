(ns scalardb.core-test
  (:require [clojure.test :refer [deftest is]]
            [qbits.alia :as alia]
            [cassandra.core :as c]
            [scalardb.core :as scalar]
            [spy.core :as spy])
  (:import (com.scalar.db.api DistributedStorage
                              DistributedTransaction
                              DistributedTransactionManager
                              TwoPhaseCommitTransaction
                              TwoPhaseCommitTransactionManager
                              Get
                              Result)
           (com.scalar.db.io BigIntValue
                             IntValue
                             TextValue)
           (java.util Optional)))

(deftest setup-transaction-tables-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/shutdown (spy/spy)
                c/create-my-keyspace (spy/spy)
                c/create-my-table (spy/spy)]
    (scalar/setup-transaction-tables {:nodes ["n1" "n2" "n3"]}
                                     [{:keyspace "test-keyspace1"
                                       :table "test-table2"
                                       :schema {:id :text
                                                :val :int
                                                :primary-key [:id]}}
                                      {:keyspace "test-keyspace2"
                                       :table "test-table2"
                                       :schema {:id :text
                                                :val :int
                                                :val2 :int
                                                :primary-key [:id]}}])
    (is (spy/called-once? alia/connect))
    (is (spy/called-n-times? alia/shutdown 2))
    (is (spy/called-n-times? c/create-my-keyspace 3))
    (is (spy/called-n-times? c/create-my-table 3))))

(deftest create-properties-test
  (let [nodes ["n1" "n2" "n3"]
        properties (#'scalar/create-properties
                    {:isolation-level :serializable
                     :serializable-strategy :extra-write} nodes)]
    (is (= "n1,n2,n3"
           (.getProperty properties "scalar.db.contact_points")))
    (is (= "cassandra"
           (.getProperty properties "scalar.db.username")))
    (is (= "cassandra"
           (.getProperty properties "scalar.db.password")))
    (is (= "SERIALIZABLE"
           (.getProperty properties "scalar.db.isolation_level")))
    (is (= "EXTRA_WRITE"
           (.getProperty properties "scalar.db.consensus_commit.serializable_strategy")))))

(defn- mock-result
  "This is only for Coordinator/get and this returns ID as `tx_state`"
  [id]
  (reify
    Result
    (getValue [_ column]
      (condp = column
        "tx_id" (Optional/of (TextValue. column id))
        "tx_created_at" (Optional/of (BigIntValue. column (long 1566376246)))
        "tx_state" (Optional/of (IntValue. column (Integer/parseInt id)))))))

(def mock-storage
  (reify
    DistributedStorage
    (^Optional get [_ ^Get g] ;; only for coordinator
      (let [k (->> g .getPartitionKey .get first .getString .get)]
        (Optional/of (mock-result k))))
    (close [_])))

(def mock-transaction
  (reify
    DistributedTransaction
    (commit [_])))

(def mock-tx-manager
  (reify
    DistributedTransactionManager
    (start [_] mock-transaction)
    (close [_])))

(def mock-2pc
  (reify
    TwoPhaseCommitTransaction
    (prepare [_])
    (commit [_])))

(def mock-2pc-manager
  (reify
    TwoPhaseCommitTransactionManager
    (start [_] mock-2pc)
    (join [_ _] mock-2pc)
    (close [_])))

(deftest close-all-test
  (let [test {:storage (atom mock-storage)
              :transaction (atom mock-tx-manager)
              :2pc (atom [mock-2pc-manager mock-2pc-manager])}]
    (scalar/close-all! test)
    (is (nil? @(:storage test)))
    (is (nil? @(:transaction test)))
    (is (nil? @(:2pc test)))))

(deftest prepare-storage-service-test
  (with-redefs [c/live-nodes (spy/stub ["n1" "n2" "n3"])
                scalar/create-service-instance (spy/stub mock-storage)]
    (let [test {:storage (atom nil)}]
      (scalar/prepare-storage-service! test)
      (is (= mock-storage @(:storage test))))))

(deftest prepare-transaction-service-test
  (with-redefs [c/live-nodes (spy/stub ["n1" "n2" "n3"])
                scalar/create-service-instance (spy/stub mock-tx-manager)]
    (let [test {:transaction (atom nil)}]
      (scalar/prepare-transaction-service! test)
      (is (= mock-tx-manager @(:transaction test))))))

(deftest prepare-service-fail-test
  (with-redefs [c/live-nodes (spy/stub ["n1" "n2" "n3"])
                c/exponential-backoff (spy/spy)
                scalar/create-service-instance (spy/stub nil)]
    (let [test {:storage (atom nil)}]
      (scalar/prepare-storage-service! test)
      (is (spy/called-n-times? c/exponential-backoff 8))
      (is (nil? @(:storage test))))))

(deftest check-connection-test
  (with-redefs [scalar/prepare-transaction-service! (spy/mock
                                                     (fn [t]
                                                       (reset!
                                                        (:transaction t)
                                                        mock-tx-manager)))]
    (let [test {:transaction (atom nil)}]
      (scalar/check-transaction-connection! test)
      (is (spy/called-once? scalar/prepare-transaction-service!))
      (is (= mock-tx-manager @(:transaction test))))))

(deftest try-reconnection-test
  (with-redefs [scalar/prepare-transaction-service! (spy/mock
                                                     (fn [t]
                                                       (reset!
                                                        (:transaction t)
                                                        mock-tx-manager)))]
    (let [test {:transaction (atom nil)
                :failures (atom 999)}]
      (scalar/try-reconnection-for-transaction! test)
      (is (spy/called-once? scalar/prepare-transaction-service!))
      (is (= mock-tx-manager @(:transaction test)))
      (is (= 0 @(:failures test)))

      ;; the next one doesn't reconnect
      (scalar/try-reconnection-for-transaction! test)
      (is (spy/called-once? scalar/prepare-transaction-service!))
      (is (= 1 @(:failures test))))))

(deftest start-transaction
  (let [test {:transaction (atom mock-tx-manager)}]
    (is (= mock-transaction (scalar/start-transaction test)))))

(deftest check-transaction-states-test
  (with-redefs [scalar/create-service-instance (spy/stub mock-storage)]
    (let [test {:storage (atom nil)}]
      (is (= 1 (scalar/check-transaction-states test #{"3" "4"}))))))

(deftest check-transaction-states-fail-test
  (with-redefs [c/exponential-backoff (spy/spy)
                scalar/prepare-storage-service! (spy/spy)]
    (let [test {:storage (atom mock-storage)}]
      (is (thrown? clojure.lang.ExceptionInfo
                   (scalar/check-transaction-states test #{"tx"})))
      (is (spy/called-n-times? c/exponential-backoff 8))
      (is (spy/called-n-times? scalar/prepare-storage-service! 3)))))
