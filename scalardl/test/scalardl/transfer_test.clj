(ns scalardl.transfer-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [scalardl.cassandra :as cassandra]
            [scalardl.core :as dl]
            [scalardl.transfer :as transfer]
            [spy.core :as spy])
  (:import (com.scalar.dl.client.service ClientService)
           (com.scalar.dl.ledger.service StatusCode)
           (com.scalar.dl.rpc ContractExecutionResponse
                              LedgerServiceResponse)
           (javax.json Json)))

(def ^:dynamic contract-count (atom 0))
(def ^:dynamic execute-count (atom 0))
(def ^:dynamic test-records (atom []))

(def mock-client-service
  (proxy [ClientService] [nil nil nil nil nil nil]
    (registerCertificate []
      (-> (LedgerServiceResponse/newBuilder)
          (.setStatus (.get StatusCode/OK))
          (.build)))
    (registerContract [_ _ _ _]
      (swap! contract-count inc)
      (-> (LedgerServiceResponse/newBuilder)
          (.setStatus (.get StatusCode/OK))
          (.build)))
    (executeContract [_ _]
      (swap! execute-count inc)
      (-> (ContractExecutionResponse/newBuilder)
          (.setStatus (.get StatusCode/OK))
          (.setResult (-> (Json/createObjectBuilder)
                          (.add "balance" 1000)
                          (.add "age" 111)
                          .build
                          .toString))
          (.build)))))

(def mock-failure-client-service
  (proxy [ClientService] [nil nil nil nil nil nil]
    (registerCertificate []
      (-> (LedgerServiceResponse/newBuilder)
          (.setStatus (.get StatusCode/OK))
          (.build)))
    (registerContract [_ _ _ _]
      (swap! contract-count inc)
      (-> (LedgerServiceResponse/newBuilder)
          (.setStatus (.get StatusCode/OK))
          (.build)))
    (executeContract [_ _]
      (swap! execute-count inc)
      (-> (ContractExecutionResponse/newBuilder)
          (.setStatus (.get StatusCode/UNKNOWN_TRANSACTION_STATUS))
          (.build)))))

(deftest transfer-client-init-test
  (binding [contract-count (atom 0)
            execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)
                  cassandra/create-tables (spy/spy)]
      (let [client (client/open! (transfer/->TransferClient (atom false)
                                                            (atom nil) 5)
                                 nil nil)]
        (client/setup! client nil)
        (is (spy/called-once? cassandra/create-tables))
        (is (= 3 @contract-count))
        (is (= 5 @execute-count))
        (is (true? @(:initialized? client)))

        ;; setup isn't executed
        (client/setup! client nil)
        (is (spy/called-once? cassandra/create-tables))))))

(deftest transfer-client-transfer-test
  (binding [execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
      (let [client (client/open! (transfer/->TransferClient (atom false)
                                                            (atom nil) 5)
                                 nil nil)
            result (client/invoke! client
                                   nil
                                   (#'transfer/transfer {:client client} nil))]
        (is (= 1 @execute-count))
        (is (= :ok (:type result)))))))

(deftest transfer-client-transfer-unknown-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-failure-client-service)
                dl/try-switch-server! (spy/stub mock-client-service)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 5)
                               nil nil)
          test {:unknown-tx (atom #{})}
          result (client/invoke! client
                                 test
                                 (#'transfer/transfer {:client client} nil))]
      (is (spy/called-once? dl/try-switch-server!))
      (is (= 1 (count @(:unknown-tx test))))
      (is (= mock-client-service @(:client-service client)))
      (is (= :fail (:type result))))))

(deftest transfer-client-check-tx-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)
                dl/check-tx-committed (spy/stub true)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 5)
                               nil nil)
          result (client/invoke! client
                                 {:unknown-tx (atom #{"tx1"})}
                                 (#'transfer/check-tx {} nil))]
      (is (= :ok (:type result)))
      (is (= 1 (:value result))))))

(deftest transfer-client-check-tx-fail-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)
                dl/check-tx-committed (spy/stub nil)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 5)
                               nil nil)
          result (client/invoke! client
                                 {:unknown-tx (atom #{"tx1"})}
                                 (#'transfer/check-tx {} nil))]
      (is (= :fail (:type result))))))

(deftest transfer-client-get-all-test
  (binding [execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
      (let [client (client/open! (transfer/->TransferClient (atom false)
                                                            (atom nil) 1)
                                 nil nil)
            result (client/invoke! client
                                   nil
                                   (#'transfer/get-all {} nil))]
        (is (= 1 @execute-count))
        (is (= :ok (:type result)))
        (is (= [{:balance 1000 :age 111}] (:value result)))))))

(deftest transfer-client-get-all-fail-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-failure-client-service)
                dl/exponential-backoff (spy/spy)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 1)
                               nil nil)
          result (client/invoke! client
                                 nil
                                 (#'transfer/get-all {} nil))]
      (is (spy/called-n-times? dl/exponential-backoff 8))
      (is (= :fail (:type result))))))

(def correct-history
  [{:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :fail :f :transfer :error {:unknown-tx-status "unknown-state-tx"}}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :get-all :value [{:balance 10120 :age 1}
                                  {:balance 10140 :age 2}
                                  {:balance  9980 :age 1}
                                  {:balance  9760 :age 2}
                                  {:balance 10000 :age 0}
                                  {:balance 10500 :age 1}
                                  {:balance  9820 :age 1}
                                  {:balance  8700 :age 3}
                                  {:balance 10620 :age 1}
                                  {:balance 10360 :age 2}]}
   {:type :ok :f :check-tx :value 1}])

(deftest asset-checker-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 10)
                               nil nil)
          checker (#'transfer/asset-checker)
          result (checker/check checker {:client client} correct-history nil)]
      (is (true? (:valid? result)))
      (is (= 14 (:total-age result)))
      (is (= 1 (:committed-unknown-tx result)))
      (is (nil? (:bad-balance result)))
      (is (nil? (:bad-age result))))))

(def bad-history
  [{:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :fail :f :transfer :error {:unknown-tx-status "unknown-state-tx"}}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :transfer}
   {:type :ok :f :get-all :value [{:balance 10120 :age 1}
                                  {:balance 10140 :age 2}
                                  {:balance  9980 :age 1}
                                  {:balance  9760 :age 2}
                                  {:balance 10001 :age 0}
                                  {:balance 10500 :age 1}
                                  {:balance  9820 :age 1}
                                  {:balance  8700 :age 3}
                                  {:balance 10620 :age 1}
                                  {:balance 10360 :age 2}]}
   {:type :fail :f :check-tx}])

(deftest asset-checker-fail-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
    (let [client (client/open! (transfer/->TransferClient (atom false)
                                                          (atom nil) 10)
                               nil nil)
          checker (#'transfer/asset-checker)
          result (checker/check checker {:client client} bad-history nil)]
      (is (false? (:valid? result)))
      (is (= 14 (:total-age result)))
      (is (= 0 (:committed-unknown-tx result)))
      (is (= {:type :wrong-balance :expected 100000 :actual 100001}
             (:bad-balance result)))
      (is (= {:type :wrong-age :expected 12 :actual 14}
             (:bad-age result))))))
