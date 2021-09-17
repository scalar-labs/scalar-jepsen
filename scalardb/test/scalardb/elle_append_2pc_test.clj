(ns scalardb.elle-append-2pc-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [scalardb.core :as scalar]
            [scalardb.elle-append-2pc :as elle-append]
            [spy.core :as spy])
  (:import (com.scalar.db.api TwoPhaseCommitTransaction
                              Get
                              Put
                              Result)
           (com.scalar.db.io Key
                             TextValue)
           (com.scalar.db.exception.transaction CommitException
                                                PreparationException
                                                ValidationException
                                                CrudException
                                                UnknownTransactionStatusException)
           (java.util Optional)))

(def ^:dynamic test-records (atom {0 {} 1 {} 2 {} 3 {} 4 {}}))

(def ^:dynamic get-count (atom 0))
(def ^:dynamic put-count (atom 0))
(def ^:dynamic prepare-count (atom 0))
(def ^:dynamic validate-count (atom 0))
(def ^:dynamic commit-count (atom 0))
(def ^:dynamic rollback-count (atom 0))

(defn- key->id
  [^Key k]
  (-> k .get first .get))

(defn- mock-result
  [id]
  (reify
    Result
    (getValue [this column]
      (if-let [v (get (@test-records id) (keyword column))]
        (->> v (TextValue. column) Optional/of)
        (Optional/empty)))))

(defn- mock-get
  [^Get g]
  (let [id (-> g .getPartitionKey key->id)]
    (swap! get-count inc)
    (if (seq (@test-records id))
      (Optional/of (mock-result id))
      (Optional/empty))))

(defn- mock-put
  [^Put p]
  (let [id (-> p .getPartitionKey key->id)
        v (some-> p .getValues (get "val") .getString .get)]
    (swap! test-records #(update % id assoc :val v))
    (swap! put-count inc)))

(def mock-2pc
  (reify
    TwoPhaseCommitTransaction
    (getId [this] "dummy-id")
    (^Optional get [this ^Get g] (mock-get g))
    (^void put [this ^Put p] (mock-put p))
    (^void prepare [this] (swap! prepare-count inc))
    (^void validate [this] (swap! validate-count inc))
    (^void commit [this] (swap! commit-count inc))
    (^void rollback [this] (swap! rollback-count inc))))

(def mock-2pc-throws-exception
  (reify
    TwoPhaseCommitTransaction
    (getId [this] "dummy-id")
    (^Optional get [this ^Get g] (throw (CrudException. "get failed")))
    (^void put [this ^Put p] (throw (CrudException. "put failed")))
    (^void prepare [this] (throw (PreparationException. "preparation failed")))
    (^void validate [this] (throw (ValidationException. "validation failed")))
    (^void commit [this] (throw (CommitException. "commit failed")))
    (^void rollback [this] (swap! rollback-count inc))))

(def mock-2pc-throws-unknown
  (reify
    TwoPhaseCommitTransaction
    (getId [this] "unknown-state-tx")
    (^Optional get [this ^Get g] (mock-get g))
    (^void put [this ^Put p] (mock-put p))
    (^void prepare [this] (swap! prepare-count inc))
    (^void validate [this] (swap! validate-count inc))
    (^void commit [this] (throw (UnknownTransactionStatusException. "unknown state")))
    (^void rollback [this] (swap! rollback-count inc))))

(deftest append-client-init-test
  (with-redefs [scalar/setup-transaction-tables (spy/spy)
                scalar/prepare-2pc-service! (spy/spy)]
    (let [client (client/open! (elle-append/->AppendClient (atom false))
                               nil nil)]
      (client/setup! client nil)
      (is (true? @(:initialized? client)))
      (is (spy/called-n-times? scalar/setup-transaction-tables 3))
      (is (spy/called-once? scalar/prepare-2pc-service!))

      ;; setup isn't executed
      (client/setup! client nil)
      (is (spy/called-n-times? scalar/setup-transaction-tables 3)))))

(deftest append-client-invoke-test
  (binding [test-records (atom {0 {} 1 {} 2 {} 3 {} 4 {}})
            get-count (atom 0)
            put-count (atom 0)
            prepare-count (atom 0)
            validate-count (atom 0)
            commit-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc)
                  scalar/join-2pc (spy/stub mock-2pc)]
      (let [client (client/open! (elle-append/->AppendClient (atom false))
                                 nil nil)
            result (client/invoke! client
                                   {:isolation-level :serializable
                                    :serializable-strategy :extra-write}
                                   {:type :invoke
                                    :f :txn
                                    :value [[:r 1 nil]
                                            [:append 1 0]
                                            [:r 0 nil]
                                            [:append 1 1]
                                            [:append 3 0]]})]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (= 5 @get-count))
        (is (= 3 @put-count))
        (is (= 2 @prepare-count))
        (is (= 2 @validate-count))
        (is (= 2 @commit-count))
        (is (= {0 {} 1 {:val "0,1"} 2 {} 3 {:val "0"} 4 {}}
               @test-records))
        (is (= :ok (:type result)))))))

(deftest append-client-invoke-crud-exception-test
  (binding [rollback-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc-throws-exception)
                  scalar/join-2pc (spy/stub mock-2pc)
                  scalar/try-reconnection-for-2pc! (spy/spy)]
      (let [client (client/open! (elle-append/->AppendClient (atom false))
                                 nil nil)
            result (client/invoke! client
                                   {:isolation-level :serializable
                                    :serializable-strategy :extra-write}
                                   {:type :invoke
                                    :f :txn
                                    :value [[:r 1 nil]]})]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (spy/called-once? scalar/try-reconnection-for-2pc!))
        (is (= 2 @rollback-count))
        (is (= :fail (:type result)))))))

(deftest append-client-invoke-unknown-exception-test
  (binding [get-count (atom 0)
            put-count (atom 0)
            prepare-count (atom 0)
            validate-count (atom 0)
            rollback-count (atom 0)]
    (with-redefs [scalar/start-2pc (spy/stub mock-2pc-throws-unknown)
                  scalar/join-2pc (spy/stub mock-2pc)
                  scalar/try-reconnection-for-2pc! (spy/spy)]
      (let [client (client/open! (elle-append/->AppendClient (atom false))
                                 nil nil)
            result (client/invoke! client
                                   {:isolation-level :serializable
                                    :serializable-strategy :extra-write
                                    :unknown-tx (atom #{})}
                                   {:type :invoke
                                    :f :txn
                                    :value [[:r 1 nil]
                                            [:append 1 0]]})]
        (is (spy/called-once? scalar/start-2pc))
        (is (spy/called-once? scalar/join-2pc))
        (is (spy/not-called? scalar/try-reconnection-for-2pc!))
        (is (= 2 @get-count))
        (is (= 1 @put-count))
        (is (= 2 @prepare-count))
        (is (= 2 @validate-count))
        (is (= 0 @rollback-count))
        (is (= :info (:type result)))
        (is (= "unknown-state-tx" (get-in result
                                          [:error :unknown-tx-status])))))))
