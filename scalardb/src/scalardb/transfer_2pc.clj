(ns scalardb.transfer-2pc
  (:require [cassandra.core :as cassandra]
            [jepsen
             [client :as client]
             [generator :as gen]]
            [scalardb.core :as scalar]
            [scalardb.transfer :as transfer])
  (:import (com.scalar.db.exception.transaction UnknownTransactionStatusException)))

(defn- tx-transfer
  [tx1 tx2 {:keys [from to amount]}]
  (try
    (let [fromResult (.get tx1 (transfer/prepare-get from))
          toResult (.get tx2 (transfer/prepare-get to))]
      (->> (transfer/calc-new-balance fromResult (- amount))
           (transfer/prepare-put from)
           (.put tx1))
      (->> (transfer/calc-new-balance toResult amount)
           (transfer/prepare-put to)
           (.put tx2)))
    (scalar/prepare-validate-commit-txs [tx1 tx2])
    (catch UnknownTransactionStatusException e
      (throw e))
    (catch Exception e
      (scalar/rollback-txs [tx1 tx2])
      (throw e))))

(defrecord TransferClient [initialized? n initial-balance]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (transfer/setup-tables test)
        (scalar/prepare-2pc-service! test)
        (scalar/prepare-transaction-service! test)
        (transfer/populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (let [tx1 (scalar/start-2pc test)
                      tx2 (scalar/join-2pc test (.getId tx1))]
                  (try
                    (tx-transfer tx1 tx2 (:value op))
                    (assoc op :type :ok)
                    (catch UnknownTransactionStatusException _
                      (swap! (:unknown-tx test) conj (.getId tx1))
                      (assoc op
                             :type :info
                             :error {:unknown-tx-status (.getId tx1)}))
                    (catch Exception e
                      (scalar/try-reconnection-for-2pc! test)
                      (assoc op :type :fail :error (.getMessage e)))))
      :get-all (do
                 (cassandra/wait-rf-nodes test)
                 (if-let [results (transfer/read-all-with-retry test (:num op))]
                   (assoc op :type :ok :value {:balance
                                               (transfer/get-balances results)
                                               :version
                                               (transfer/get-versions results)})
                   (assoc op :type :fail :error "Failed to get balances")))
      :check-tx (if-let [num-committed
                         (scalar/check-transaction-states test
                                                          @(:unknown-tx test))]
                  (assoc op :type :ok, :value num-committed)
                  (assoc op :type :fail, :error "Failed to check status"))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn workload
  [_]
  {:client (->TransferClient (atom false)
                             transfer/NUM_ACCOUNTS
                             transfer/INITIAL_BALANCE)
   :generator [transfer/diff-transfer]
   :final-generator (gen/phases
                     (gen/once transfer/get-all)
                     (gen/once transfer/check-tx))
   :checker (transfer/consistency-checker)})
