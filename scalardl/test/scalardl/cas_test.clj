(ns scalardl.cas-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [scalardl.cas :as cas]
            [scalardl.cassandra :as cassandra]
            [scalardl.core :as dl]
            [spy.core :as spy])
  (:import (com.scalar.dl.client.exception ClientException)
           (com.scalar.dl.client.service ClientService)
           (com.scalar.dl.ledger.model ContractExecutionResult)
           (com.scalar.dl.ledger.service StatusCode)
           (javax.json Json)))

(def ^:dynamic contract-count (atom 0))
(def ^:dynamic execute-count (atom 0))

(def mock-client-service
  (proxy [ClientService] [nil nil nil nil]
    (registerCertificate [])
    (registerContract [_ _ _ _]
      (swap! contract-count inc)
      nil)
    (executeContract [& _]
      (swap! execute-count inc)
      (ContractExecutionResult. (-> (Json/createObjectBuilder)
                                    (.add "value" 3)
                                    .build)
                                nil
                                nil))))

(def mock-client-service-throws-unknown-status
  (proxy [ClientService] [nil nil nil nil]
    (registerCertificate [])
    (registerContract [_ _ _ _]
      (swap! contract-count inc)
      nil)
    (executeContract [& _]
      (swap! execute-count inc)
      (throw (ClientException. "the status is unknown"
                               (Exception.)
                               StatusCode/UNKNOWN_TRANSACTION_STATUS)))))

(def mock-client-service-throws-database-error
  (proxy [ClientService] [nil nil nil nil]
    (registerCertificate [])
    (registerContract [_ _ _ _]
      (swap! contract-count inc)
      nil)
    (executeContract [& _]
      (swap! execute-count inc)
      (throw (ClientException. "the status is unknown"
                               (Exception.)
                               StatusCode/DATABASE_ERROR)))))

(deftest cas-client-init-test
  (binding [contract-count (atom 0)
            execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)
                  cassandra/create-tables (spy/spy)]
      (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                                 nil nil)]
        (client/setup! client nil)
        (is (spy/called-once? cassandra/create-tables))
        (is (= 3 @contract-count))
        (is (zero? @execute-count))
        (is (true? @(:initialized? client)))

        ;; setup isn't executed
        (client/setup! client nil)
        (is (spy/called-once? cassandra/create-tables))))))

(deftest cas-client-read-test
  (binding [execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
      (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                                 nil nil)
            result (client/invoke! client nil cas/r)]
        (is (= 1 @execute-count))
        (is (= :ok (:type result)))
        (is (= 3 (:value result)))))))

(deftest cas-client-write-test
  (binding [execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
      (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                                 nil nil)
            result (client/invoke! client nil (#'cas/w nil nil))]
        (is (= 1 @execute-count))
        (is (= :ok (:type result)))))))

(deftest cas-client-cas-test
  (binding [execute-count (atom 0)]
    (with-redefs [dl/prepare-client-service (spy/stub mock-client-service)]
      (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                                 nil nil)
            result (client/invoke! client nil (#'cas/cas nil nil))]
        (is (= 1 @execute-count))
        (is (= :ok (:type result)))))))

(deftest cas-client-cas-unknown-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service-throws-unknown-status)
                dl/try-switch-server! (spy/stub mock-client-service)
                dl/committed? (spy/mock (fn [_ _]
                                          (throw
                                           (ex-info "fail" {}))))]
    (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                               nil nil)
          test {:unknown-tx (atom #{})}
          result (client/invoke! client test (#'cas/cas nil nil))]
      (is (spy/called-once? dl/try-switch-server!))
      (is (spy/called-once? dl/committed?))
      (is (= mock-client-service @(:client-service client)))
      (is (= :info (:type result))))))

(deftest cas-client-cas-commit-after-unknown-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service-throws-unknown-status)
                dl/try-switch-server! (spy/stub mock-client-service)
                dl/committed? (spy/stub true)]
    (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                               nil nil)
          test {:unknown-tx (atom #{})}
          result (client/invoke! client test (#'cas/cas nil nil))]
      (is (spy/called-once? dl/try-switch-server!))
      (is (spy/called-once? dl/committed?))
      (is (= mock-client-service @(:client-service client)))
      (is (= :ok (:type result))))))

(deftest cas-client-cas-abort-after-unknown-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service-throws-unknown-status)
                dl/try-switch-server! (spy/stub mock-client-service)
                dl/committed? (spy/stub false)]
    (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                               nil nil)
          test {:unknown-tx (atom #{})}
          result (client/invoke! client test (#'cas/cas nil nil))]
      (is (spy/called-once? dl/try-switch-server!))
      (is (spy/called-once? dl/committed?))
      (is (= mock-client-service @(:client-service client)))
      (is (= :fail (:type result))))))

(deftest cas-client-cas-failure-test
  (with-redefs [dl/prepare-client-service (spy/stub mock-client-service-throws-database-error)
                dl/try-switch-server! (spy/stub mock-client-service)]
    (let [client (client/open! (cas/->CasRegisterClient (atom false) (atom nil))
                               nil nil)
          test {:unknown-tx (atom #{})}
          result (client/invoke! client test (#'cas/cas nil nil))]
      (is (spy/called-once? dl/try-switch-server!))
      (is (spy/not-called? dl/committed?))
      (is (= mock-client-service @(:client-service client)))
      (is (= :fail (:type result))))))
