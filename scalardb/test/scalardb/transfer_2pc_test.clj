(ns scalardb.transfer-2pc-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [scalardb.core :as scalar]
            [scalardb.core-test :refer [mock-db]]
            [scalardb.transfer :as transfer]
            [scalardb.transfer-2pc :as transfer-2pc]
            [spy.core :as spy])
  (:import (com.scalar.db.api DistributedTransaction
                              DistributedStorage
                              TwoPhaseCommitTransaction
                              Get
                              Put
                              Result)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction CommitException
                                                PreparationException
                                                ValidationException
                                                CrudException
                                                UnknownTransactionStatusException)
           (java.util Optional)))

(def ^:dynamic test-records (atom {0 0 1 0 2 0 3 0 4 0}))

(def ^:dynamic get-count (atom 0))
(def ^:dynamic put-count (atom 0))
(def ^:dynamic prepare-count (atom 0))
(def ^:dynamic validate-count (atom 0))
(def ^:dynamic commit-count (atom 0))
(def ^:dynamic rollback-count (atom 0))

(defn- key->id
  [^Key k]
  (-> k .get first .get))

(defn- mock-result [id]
  (reify
    Result
    (getValue [_ column]
      (->> (@test-records id)
           (IntValue. column)
           Optional/of))))

(defn- mock-get
  [^Get g]
  (let [id (-> g .getPartitionKey key->id)]
    (swap! get-count inc)
    (Optional/of (mock-result id))))

(defn- mock-put
  [^Put p]
  (let [id (-> p .getPartitionKey key->id)
        [_ v] (-> p .getValues first)]
    (swap! put-count inc)
    (swap! test-records #(assoc % id (.get v)))))

(def mock-transaction
  (reify
    DistributedTransaction
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))
    (^void commit [_] (swap! commit-count inc))))

(def mock-storage
  (reify
    DistributedStorage
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))))

(def mock-transaction-throws-exception
  (reify
    DistributedTransaction
    (^Optional get [_ ^Get _] (throw (CrudException. "get failed" nil)))
    (^void put [_ ^Put _] (throw (CrudException. "put failed" nil)))
    (^void commit [_] (throw (CommitException. "commit failed" nil)))))

(def mock-2pc
  (reify
    TwoPhaseCommitTransaction
    (getId [_] "dummy-id")
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))
    (^void prepare [_] (swap! prepare-count inc))
    (^void validate [_] (swap! validate-count inc))
    (^void commit [_] (swap! commit-count inc))
    (^void rollback [_] (swap! rollback-count inc))))

(def mock-2pc-throws-exception
  (reify
    TwoPhaseCommitTransaction
    (getId [_] "dummy-id")
    (^Optional get [_ ^Get _] (throw (CrudException. "get failed" nil)))
    (^void put [_ ^Put _] (throw (CrudException. "put failed" nil)))
    (^void prepare [_] (throw (PreparationException. "preparation failed" nil)))
    (^void validate [_] (throw (ValidationException. "validation failed" nil)))
    (^void commit [_] (throw (CommitException. "commit failed" nil)))
    (^void rollback [_] (swap! rollback-count inc))))

(def mock-2pc-throws-unknown
  (reify
    TwoPhaseCommitTransaction
    (getId [_] "unknown-state-tx")
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))
    (^void prepare [_] (swap! prepare-count inc))
    (^void validate [_] (swap! validate-count inc))
    (^void commit [_] (throw (UnknownTransactionStatusException. "unknown state" nil)))
    (^void rollback [_] (swap! rollback-count inc))))

(deftest transfer-client-init-test
  (binding [test-records (atom {0 0 1 0 2 0 3 0 4 0})
            put-count (atom 0)
            commit-count (atom 0)]
    (with-redefs [scalar/setup-transaction-tables (spy/spy)
                  scalar/prepare-2pc-service! (spy/spy)
                  scalar/prepare-transaction-service! (spy/spy)
                  scalar/start-transaction (spy/stub mock-transaction)]
      (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                                 nil nil)]
        (client/setup! client nil)
        (is (true? @(:initialized? client)))
        (is (spy/called-once? scalar/setup-transaction-tables))
        (is (spy/called-once? scalar/prepare-2pc-service!))
        (is (spy/called-once? scalar/prepare-transaction-service!))
        (is (spy/called-once? scalar/start-transaction))
        (is (= 5 @put-count))
        (is (= 1 @commit-count))
        (is (= {0 100 1 100 2 100 3 100 4 100} @test-records))

        ;; setup isn't executed
        (client/setup! client nil)
        (is (spy/called-once? scalar/setup-transaction-tables))))))

(deftest transfer-client-transfer-test
  (binding [test-records (atom {0 0 1 0 2 0 3 0 4 0})
            get-count (atom 0)
            put-count (atom 0)
            prepare-count (atom 0)
            validate-count (atom 0)
            commit-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc)
                  scalar/join-2pc (spy/stub mock-2pc)]
      (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client
                                   nil
                                   {:type :invoke
                                    :f :transfer
                                    :value [{:from 0 :to 1 :amount 10}]})]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (= 2 @get-count))
        (is (= 2 @put-count))
        (is (= 2 @prepare-count))
        (is (= 2 @validate-count))
        (is (= 2 @commit-count))
        (is (= {0 -10 1 10 2 0 3 0 4 0} @test-records))
        (is (= :ok (:type result)))))))

(deftest transfer-client-transfer-crud-exception-test
  (binding [rollback-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc-throws-exception)
                  scalar/join-2pc (spy/stub mock-2pc)
                  scalar/try-reconnection! (spy/spy)]
      (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client
                                   {:failures (atom 0)}
                                   (#'transfer/transfer {:client client}
                                                        nil))]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (spy/called-once? scalar/try-reconnection!))
        (is (= 2 @rollback-count))
        (is (= :fail (:type result)))))))

(deftest transfer-client-transfer-unknown-exception-test
  (binding [get-count (atom 0)
            put-count (atom 0)
            prepare-count (atom 0)
            validate-count (atom 0)
            rollback-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc-throws-unknown)
                  scalar/join-2pc (spy/stub mock-2pc)
                  scalar/try-reconnection! (spy/spy)]
      (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client
                                   {:unknown-tx (atom #{})
                                    :failures (atom 0)}
                                   (#'transfer/transfer {:client client}
                                                        nil))]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (spy/called-once? scalar/try-reconnection!))
        (is (= 2 @get-count))
        (is (= 2 @put-count))
        (is (= 2 @prepare-count))
        (is (= 2 @validate-count))
        (is (= 0 @rollback-count))
        (is (= :fail (:type result)))
        (is (= [:unknown-tx-status] (get-in result [:error :results])))))))

(deftest transfer-client-get-all-test
  (binding [test-records (atom {0 1000 1 100 2 10 3 1 4 0})]
    (with-redefs [scalar/check-transaction-connection! (spy/spy)
                  scalar/check-storage-connection! (spy/spy)
                  scalar/start-transaction (spy/stub mock-transaction)]
      (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client {:db mock-db
                                           :storage (ref mock-storage)}
                                   (#'transfer/get-all {:client client}
                                                       nil))]
        (is (spy/called-once? scalar/check-transaction-connection!))
        (is (spy/called-once? scalar/check-storage-connection!))
        (is (= :ok (:type result)))
        (is (= [1000 100 10 1 0] (get-in result [:value :balance])))
        (is (= [1000 100 10 1 0] (get-in result [:value :version])))))))

(deftest transfer-client-get-all-fail-test
  (with-redefs [scalar/exponential-backoff (spy/spy)
                scalar/check-transaction-connection! (spy/spy)
                scalar/check-storage-connection! (spy/spy)
                scalar/prepare-transaction-service! (spy/spy)
                scalar/prepare-storage-service! (spy/spy)
                scalar/start-transaction (spy/stub mock-transaction-throws-exception)]
    (let [num-accounts 5
          client (client/open! (transfer-2pc/->TransferClient (atom false)
                                                              num-accounts
                                                              100 1)
                               nil nil)
          retries-reconnection (* num-accounts
                                  (+ (quot scalar/RETRIES
                                           scalar/RETRIES_FOR_RECONNECTION)
                                     1))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (client/invoke! client {:db mock-db
                                           :storage (ref mock-storage)}
                                   (#'transfer/get-all {:client client}
                                                       nil))))
      (is (spy/called-n-times? scalar/exponential-backoff
                               (* scalar/RETRIES num-accounts)))
      (is (spy/called-n-times? scalar/prepare-transaction-service!
                               retries-reconnection))
      (is (spy/called-n-times? scalar/prepare-storage-service!
                               retries-reconnection)))))

(deftest transfer-client-check-tx-test
  (with-redefs [scalar/check-transaction-states (spy/stub 1)]
    (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                               nil nil)
          result (client/invoke! client {:unknown-tx (atom #{"tx1"})}
                                 (#'transfer/check-tx {:client client}
                                                      nil))]
      (is (spy/called-once? scalar/check-transaction-states))
      (is (= :ok (:type result)))
      (is (= 1 (:value result))))))

(deftest transfer-client-check-tx-fail-test
  (with-redefs [scalar/check-transaction-states (spy/stub nil)]
    (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 5 100 1)
                               nil nil)
          result (client/invoke! client {:unknown-tx (atom #{"tx1"})}
                                 (#'transfer/check-tx {:client client}
                                                      nil))]
      (is (spy/called-once? scalar/check-transaction-states))
      (is (= :fail (:type result))))))

(def correct-history
  [{:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :fail :f :transfer :error {:results [:unknown-tx-status]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :get-all :value {:balance [10120 10140 9980 9760 10000
                                            10500 9820 8700 10620 10360]
                                  :version [2 3 2 3 1 2 2 4 2 3]}}
   {:type :ok :f :check-tx :value 1}])

(deftest consistency-checker-test
  (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 10 10000 1)
                             nil nil)
        checker (#'transfer/consistency-checker)
        result (checker/check checker {:client client} correct-history nil)]
    (is (true? (:valid? result)))
    (is (= 24 (:total-version result)))
    (is (= 1 (:committed-unknown-tx result)))
    (is (nil? (:bad-balance result)))
    (is (nil? (:bad-version result)))))

(def bad-history
  [{:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :fail :f :transfer :error {:results [:unknown-tx-status]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :transfer :value {:results [:commit]}}
   {:type :ok :f :get-all :value {:balance [10120 10140 9980 9760 10001
                                            10500 9820 8700 10620 10360]
                                  :version [2 3 2 3 1 2 2 4 2 3]}}
   {:type :fail :f :check-tx}])

(deftest consistency-checker-fail-test
  (let [client (client/open! (transfer-2pc/->TransferClient (atom false) 10 10000 1)
                             nil nil)
        checker (#'transfer/consistency-checker)
        result (checker/check checker {:client client} bad-history nil)]
    (is (false? (:valid? result)))
    (is (= 24 (:total-version result)))
    (is (= 0 (:committed-unknown-tx result)))
    (is (= {:type :wrong-balance :expected 100000 :actual 100001}
           (:bad-balance result)))
    (is (= {:type :wrong-version :expected 22 :actual 24}
           (:bad-version result)))))
