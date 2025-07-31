(ns scalardb.transfer-2pc
  (:require [clojure.tools.logging :refer [infof warn]]
            [jepsen
             [client :as client]
             [generator :as gen]]
            [scalardb.core :as scalar]
            [scalardb.transfer :as transfer]
            [scalardb.db-extend :refer [wait-for-recovery]])
  (:import (com.scalar.db.exception.transaction UnknownTransactionStatusException)))

(defn- tx-transfer
  [tx1 tx2 from to amount]
  (infof "Transferring %d from %d to %d (tx: %s)" amount from to (.getId tx1))
  (let [fromResult (.get tx1 (transfer/prepare-get from))
        toResult (.get tx2 (transfer/prepare-get to))]
    (infof "fromID: %d, the balance: %d, the version: %d (tx: %s)" from (transfer/get-balance fromResult) (transfer/get-version fromResult) (.getId tx1))
    (->> (transfer/calc-new-balance fromResult (- amount))
         (transfer/prepare-put from)
         (.put tx1))
    (infof "toID: %d, the balance: %d, the version: %d (tx: %s)" to (transfer/get-balance toResult) (transfer/get-version toResult) (.getId tx1))
    (->> (transfer/calc-new-balance toResult amount)
         (transfer/prepare-put to)
         (.put tx2)))
  (scalar/prepare-validate-commit-txs [tx1 tx2]))

(defn- try-tx-transfer
  [test {:keys [from to amount]}]
  (let [tx1 (try (scalar/start-2pc test)
                 (catch Exception e
                   (warn (.getMessage e))))
        tx2 (if tx1
              (try (scalar/join-2pc test (.getId tx1))
                   (catch Exception e
                     (warn (.getMessage e))))
              nil)]
    (if (and tx1 tx2)
      (try
        (tx-transfer tx1 tx2 from to amount)
        :commit
        (catch UnknownTransactionStatusException _
          (swap! (:unknown-tx test) conj (.getId tx1))
          (warn "Unknown transaction: " (.getId tx1))
          :unknown-tx-status)
        (catch Exception e
          (warn (.getMessage e))
          (scalar/rollback-txs [tx1 tx2])
          :fail))
      (do
        (when tx1 (scalar/rollback-txs [tx1]))
        :start-fail))))

(defrecord TransferClient [initialized? n initial-balance max-txs]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance max-txs))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (transfer/setup-tables test)
        (scalar/prepare-2pc-service! test)
        (scalar/prepare-transaction-service! test)
        (transfer/populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (transfer/exec-transfers test op try-tx-transfer)
      :get-all (do
                 (wait-for-recovery (:db test) test)
                 (if-let [results (transfer/read-all-with-retry test n)]
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
                             transfer/INITIAL_BALANCE
                             transfer/MAX_NUM_TXS)
   :generator [transfer/transfer]
   :final-generator (gen/phases
                     (gen/once transfer/get-all)
                     (gen/once transfer/check-tx))
   :checker (transfer/consistency-checker)})
