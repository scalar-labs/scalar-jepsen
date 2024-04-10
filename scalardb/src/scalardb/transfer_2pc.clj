(ns scalardb.transfer-2pc
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen
             [client :as client]
             [generator :as gen]]
            [scalardb.core :as scalar]
            [scalardb.transfer :as transfer]
            [scalardb.db-extend :refer [wait-for-recovery]])
  (:import (com.scalar.db.exception.transaction UnknownTransactionStatusException)))

(defn- tx-transfer
  [tx1 tx2 from to amount]
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

(defn- try-tx-transfer
  [test {:keys [from to amount]}]
  (let [tx1 (scalar/start-2pc test)
        tx2 (scalar/join-2pc test (.getId tx1))]
    (try
      (tx-transfer tx1 tx2 from to amount)
      :commit
      (catch UnknownTransactionStatusException _
        (swap! (:unknown-tx test) conj (.getId tx1))
        (warn "Unknown transaction: " (.getId tx1))
        :unknown-tx-status)
      (catch Exception e
        (warn "transaction" (.getId tx1) "failed:" (.getMessage e))
        :fail))))

(defrecord TransferClient [initialized?]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (transfer/setup-tables test)
        (scalar/prepare-2pc-service! test)
        (scalar/prepare-transaction-service! test)
        (transfer/populate-accounts test
                                    transfer/NUM_ACCOUNTS
                                    transfer/INITIAL_BALANCE))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (transfer/exec-transfers test op try-tx-transfer)
      :get-all (do
                 (wait-for-recovery (:db test) test)
                 (if-let [results (transfer/read-all-with-retry
                                   test
                                   transfer/NUM_ACCOUNTS)]
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
  {:client (->TransferClient (atom false))
   :generator [transfer/transfer]
   :final-generator (gen/phases
                     (gen/once transfer/get-all)
                     (gen/once transfer/check-tx))
   :checker (transfer/consistency-checker)})
