(ns scalardb.transfer-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [scalardb.core-test :refer [mock-db]]
            [scalardb.core :as scalar]
            [scalardb.transfer :as transfer]
            [spy.core :as spy])
  (:import (com.scalar.db.api DistributedTransaction
                              Get
                              Put
                              Result)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction CommitException
                                                CrudException
                                                UnknownTransactionStatusException)
           (java.util Optional)))

(def ^:dynamic test-records (atom {0 0 1 0 2 0 3 0 4 0}))

(def ^:dynamic get-count (atom 0))
(def ^:dynamic put-count (atom 0))
(def ^:dynamic commit-count (atom 0))

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
    (^String getId [_] "dummy-tx-id")
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))
    (^void commit [_] (swap! commit-count inc))))

(def mock-transaction-throws-exception
  (reify
    DistributedTransaction
    (^String getId [_] "dummy-tx-id")
    (^Optional get [_ ^Get _] (throw (CrudException. "get failed" nil)))
    (^void put [_ ^Put _] (throw (CrudException. "put failed" nil)))
    (^void commit [_] (throw (CommitException. "commit failed" nil)))))

(def mock-transaction-throws-unknown
  (reify
    DistributedTransaction
    (getId [_] "unknown-state-tx")
    (^Optional get [_ ^Get g] (mock-get g))
    (^void put [_ ^Put p] (mock-put p))
    (^void commit [_] (throw (UnknownTransactionStatusException. "unknown state" nil)))))

(deftest transfer-client-init-test
  (binding [test-records (atom {0 0 1 0 2 0 3 0 4 0})
            put-count (atom 0)
            commit-count (atom 0)]
    (with-redefs [scalar/setup-transaction-tables (spy/spy)
                  scalar/prepare-transaction-service! (spy/spy)
                  scalar/start-transaction (spy/stub mock-transaction)]
      (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                                 nil nil)]
        (client/setup! client nil)
        (is (true? @(:initialized? client)))
        (is (spy/called-once? scalar/setup-transaction-tables))
        (is (spy/called-once? scalar/prepare-transaction-service!))
        (is (spy/called-once? scalar/start-transaction))
        (is (= 5 @put-count))
        (is (= 1 @commit-count))
        (is (= {0 100 1 100 2 100 3 100 4 100} @test-records))

        ;; setup isn't executed
        (client/setup! client nil)
        (is (spy/called-once? scalar/setup-transaction-tables))))))

;; skip because it takes a long time due to backoff
(comment
  (deftest transfer-client-init-fail-test
    (with-redefs [scalar/setup-transaction-tables (spy/spy)
                  scalar/prepare-transaction-service! (spy/spy)
                  scalar/start-transaction (spy/stub mock-transaction-throws-exception)]
      (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                                 nil nil)]
        (is (thrown? CrudException (client/setup! client nil)))))))

(deftest transfer-client-transfer-test
  (binding [test-records (atom {0 0 1 0 2 0 3 0 4 0})
            get-count (atom 0)
            put-count (atom 0)
            commit-count (atom 0)]
    (with-redefs [scalar/start-transaction (spy/stub mock-transaction)]
      (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client
                                   nil
                                   {:type :invoke
                                    :f :transfer
                                    :value [{:from 0 :to 1 :amount 10}]})]
        (is (spy/called-once? scalar/start-transaction))
        (is (= 2 @get-count))
        (is (= 2 @put-count))
        (is (= 1 @commit-count))
        (is (= {0 -10 1 10 2 0 3 0 4 0} @test-records))
        (is (= :ok (:type result)))))))

(deftest transfer-client-transfer-no-tx-test
  (with-redefs [scalar/start-transaction (spy/stub nil)
                scalar/try-reconnection! (spy/spy)]
    (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                               nil nil)
          result (client/invoke! client
                                 nil
                                 (#'transfer/transfer {:client client}
                                                      nil))]
      (is (spy/called-once? scalar/start-transaction))
      (is (spy/called-once? scalar/try-reconnection!))
      (is (= :fail (:type result))))))

(deftest transfer-client-transfer-crud-exception-test
  (with-redefs [scalar/start-transaction (spy/stub mock-transaction-throws-exception)
                scalar/try-reconnection! (spy/spy)]
    (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                               nil nil)
          result (client/invoke! client
                                 nil
                                 (#'transfer/transfer {:client client}
                                                      nil))]
      (is (spy/called-once? scalar/start-transaction))
      (is (spy/called-once? scalar/try-reconnection!))
      (is (= :fail (:type result))))))

(deftest transfer-client-transfer-unknown-exception-test
  (binding [get-count (atom 0)
            put-count (atom 0)]
    (with-redefs [scalar/start-transaction (spy/stub mock-transaction-throws-unknown)
                  scalar/try-reconnection! (spy/spy)]
      (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client
                                   {:unknown-tx (atom #{})}
                                   (#'transfer/transfer {:client client}
                                                        nil))]
        (is (spy/called-once? scalar/start-transaction))
        (is (spy/called-once? scalar/try-reconnection!))
        (is (= 2 @get-count))
        (is (= 2 @put-count))
        (is (= :fail (:type result)))
        (is (= [:unknown-tx-status] (get-in result [:error :results])))))))

(deftest transfer-client-get-all-test
  (binding [test-records (atom {0 1000 1 100 2 10 3 1 4 0})]
    (with-redefs [scalar/check-transaction-connection! (spy/spy)
                  scalar/start-transaction (spy/stub mock-transaction)]
      (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                                 nil nil)
            result (client/invoke! client {:db mock-db}
                                   (#'transfer/get-all {:client client}
                                                       nil))]
        (is (spy/called-once? scalar/check-transaction-connection!))
        (is (= :ok (:type result)))
        (is (= [1000 100 10 1 0] (get-in result [:value :balance])))
        (is (= [1000 100 10 1 0] (get-in result [:value :version])))))))

(deftest transfer-client-get-all-fail-test
  (with-redefs [scalar/exponential-backoff (spy/spy)
                scalar/check-transaction-connection! (spy/spy)
                scalar/prepare-transaction-service! (spy/spy)
                scalar/start-transaction (spy/stub mock-transaction-throws-exception)]
    (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                               nil nil)]
      (is (thrown? clojure.lang.ExceptionInfo
                   (client/invoke! client {:db mock-db}
                                   (#'transfer/get-all {:client client}
                                                       nil))))
      (is (spy/called-n-times? scalar/exponential-backoff scalar/RETRIES))
      (is (spy/called-n-times? scalar/prepare-transaction-service!
                               (+ (quot scalar/RETRIES scalar/RETRIES_FOR_RECONNECTION) 1))))))

(deftest transfer-client-check-tx-test
  (with-redefs [scalar/check-transaction-states (spy/stub 1)]
    (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
                               nil nil)
          result (client/invoke! client {:unknown-tx (atom #{"tx1"})}
                                 (#'transfer/check-tx {:client client}
                                                      nil))]
      (is (spy/called-once? scalar/check-transaction-states))
      (is (= :ok (:type result)))
      (is (= 1 (:value result))))))

(deftest transfer-client-check-tx-fail-test
  (with-redefs [scalar/check-transaction-states (spy/stub nil)]
    (let [client (client/open! (transfer/->TransferClient (atom false) 5 100 1)
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
  (let [client (client/open! (transfer/->TransferClient (atom false) 10 10000 1)
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
  (let [client (client/open! (transfer/->TransferClient (atom false) 10 10000 1)
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
