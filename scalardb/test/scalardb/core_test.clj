(ns scalardb.core-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.db :as db]
            [scalardb.core :as scalar]
            [scalardb.db-extend :as ext]
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

(defn- mock-result
  "This is only for Coordinator/get and this returns ID as `tx_state`"
  [id]
  (reify
    Result
    (getValue [_ column]
      (condp = column
        "tx_id" (Optional/of (TextValue. column id))
        "tx_created_at" (Optional/of (BigIntValue. column (long 1566376246)))
        "tx_state" (Optional/of (IntValue. column (Integer/parseInt id)))
        ;; for the coordinator table
        "tx_child_ids" (Optional/empty)))))

(def mock-db
  (reify
    db/DB
    (setup! [_ _ _])
    (teardown! [_ _ _])
    db/Primary
    (primaries [_ _])
    (setup-primary! [_ _ _])
    db/LogFiles
    (log-files [_ _ _])
    ext/DbExtension
    (live-nodes [_ _] ["n1" "n2" "n3"])
    (wait-for-recovery [_ _])
    (create-table-opts [_ _])
    (create-properties [_ _])))

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
  (with-redefs [scalar/create-service-instance (spy/stub mock-storage)]
    (let [test {:db mock-db :storage (atom nil)}]
      (scalar/prepare-storage-service! test)
      (is (= mock-storage @(:storage test))))))

(deftest prepare-transaction-service-test
  (with-redefs [scalar/create-service-instance (spy/stub mock-tx-manager)]
    (let [test {:db mock-db :transaction (atom nil)}]
      (scalar/prepare-transaction-service! test)
      (is (= mock-tx-manager @(:transaction test))))))

(deftest prepare-service-fail-test
  (with-redefs [scalar/exponential-backoff (spy/spy)
                scalar/create-service-instance (spy/stub nil)]
    (let [test {:db mock-db :storage (atom nil)}]
      (scalar/prepare-storage-service! test)
      (is (spy/called-n-times? scalar/exponential-backoff 20))
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
      (scalar/try-reconnection! test scalar/prepare-transaction-service!)
      (is (spy/called-once? scalar/prepare-transaction-service!))
      (is (= mock-tx-manager @(:transaction test)))
      (is (= 0 @(:failures test)))

      ;; the next one doesn't reconnect
      (scalar/try-reconnection! test scalar/prepare-transaction-service!)
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
  (with-redefs [scalar/exponential-backoff (spy/spy)
                scalar/prepare-storage-service! (spy/spy)]
    (let [test {:storage (atom mock-storage)}]
      (is (thrown? clojure.lang.ExceptionInfo
                   (scalar/check-transaction-states test #{"tx"})))
      (is (spy/called-n-times? scalar/exponential-backoff 20))
      (is (spy/called-n-times? scalar/prepare-storage-service! 7)))))

(deftest compute-exponential-backoff-test
  (is (= 2000 (scalar/compute-exponential-backoff 1)))
  (is (= 4000 (scalar/compute-exponential-backoff 2)))
  (is (= 32000 (scalar/compute-exponential-backoff 5)))
  (is (= 32000 (scalar/compute-exponential-backoff 10))))
