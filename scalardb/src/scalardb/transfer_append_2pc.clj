(ns scalardb.transfer-append-2pc
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [client :as client]
             [generator :as gen]]
            [scalardb.core :as scalar]
            [scalardb.db-extend :refer [wait-for-recovery]]
            [scalardb.transfer :as transfer]
            [scalardb.transfer-append :as t-append])
  (:import (com.scalar.db.api Result)
           (com.scalar.db.exception.transaction UnknownTransactionStatusException)))

(defn- tx-transfer
  [tx1 tx2 from to amount]
  (try
    (let [^Result from-result
          (t-append/scan-for-latest tx1 (t-append/prepare-scan-for-latest from))
          ^Result to-result
          (t-append/scan-for-latest tx2 (t-append/prepare-scan-for-latest to))]
      (info "fromID:" from "the latest age:" (t-append/get-age from-result))
      (->> (t-append/prepare-put from
                                 (t-append/calc-new-age from-result)
                                 (t-append/calc-new-balance from-result
                                                            (- amount)))
           (.put tx1))
      (info "toID:" to "the latest age:" (t-append/get-age to-result))
      (->> (t-append/prepare-put to
                                 (t-append/calc-new-age to-result)
                                 (t-append/calc-new-balance to-result amount))
           (.put tx2)))
    (scalar/prepare-validate-commit-txs [tx1 tx2])
    (catch UnknownTransactionStatusException e
      (throw e))
    (catch Exception e
      (scalar/rollback-txs [tx1 tx2])
      (throw e))))

(defn- try-tx-transfer
  [test {:keys [from to amount]}]
  (let [tx1 (try (scalar/start-2pc test)
                 (catch Exception e
                   (warn (.getMessage e))))
        tx2 (try (scalar/join-2pc test (.getId tx1))
                 (catch Exception e
                   (warn (.getMessage e))))]
    (try
      (tx-transfer tx1 tx2 from to amount)
      :commit
      (catch UnknownTransactionStatusException _
        (swap! (:unknown-tx test) conj (.getId tx1))
        (warn "Unknown transaction: " (.getId tx1))
        :unknown-tx-status)
      (catch Exception e
        (warn (.getMessage e))
        :fail))))

(defrecord TransferClient [initialized? n initial-balance max-txs]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance max-txs))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (t-append/setup-tables test)
        (scalar/prepare-2pc-service! test)
        (scalar/prepare-transaction-service! test)
        (t-append/populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (transfer/exec-transfers test op try-tx-transfer)
      :get-all (do
                 (wait-for-recovery (:db test) test)
                 (if-let [results (t-append/scan-all-records-with-retry test n)]
                   (assoc op :type :ok
                          :value {:balance (t-append/get-balances results)
                                  :age (t-append/get-ages results)
                                  :num (t-append/get-nums results)})
                   (assoc op :type, :fail, :error "Failed to get all records")))
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
                     (gen/once t-append/get-all)
                     (gen/once t-append/check-tx))
   :checker (t-append/consistency-checker)})
