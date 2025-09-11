(ns scalardb.elle-write-read-2pc
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [scalardb.core :as scalar :refer [DEFAULT_TABLE_COUNT]]
            [scalardb.elle-write-read :as wr])
  (:import (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(defn- tx-execute
  [seq-id tx1 tx2 [f k v]]
  (let [key_hash (hash k)
        table (str wr/TABLE seq-id \_ (mod key_hash DEFAULT_TABLE_COUNT))
        tx (if (= (mod key_hash 2) 0) tx1 tx2)
        result (.get tx (wr/prepare-get table k))]
    [f k (case f
           :r (when (.isPresent result) (wr/get-value result))
           :w (wr/tx-write tx table k v))]))

(defrecord WriteReadClient [initialized?]
  client/Client
  (open! [_ _ _]
    (WriteReadClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (wr/setup-tables test)
        (scalar/prepare-2pc-service! test))))

  (invoke! [_ test op]
    (let [tx1 (try (scalar/start-2pc test)
                   (catch Exception e (warn e "Starting a transaction failed")))
          tx2 (when tx1
                (try (scalar/join-2pc test (.getId tx1))
                     (catch Exception e (warn e "Joining the transaction failed"))))]
      (if (and tx1 tx2)
        (let [[seq-id txn] (:value op)]
          (try
            (when (<= @(:table-id test) seq-id)
              ;; add tables for the next sequence
              (wr/add-tables test (inc seq-id)))
            (let [txn' (mapv (partial tx-execute seq-id tx1 tx2) txn)]
              (scalar/prepare-validate-commit-txs [tx1 tx2])
              (assoc op :type :ok :value (independent/tuple seq-id txn')))
            (catch UnknownTransactionStatusException e
              (swap! (:unknown-tx test) conj (.getId tx1))
              (warn e "Unknown transaction: " (.getId tx1))
              (assoc op :type :info :error {:unknown-tx-status (.getId tx1)}))
            (catch Exception e
              (warn e "An error occurred during the transaction")
              (scalar/rollback-txs [tx1 tx2])
              (scalar/try-reconnection! test scalar/prepare-2pc-service!)
              (assoc op :type :fail :error {:crud-error (.getMessage e)}))))
        (do
          (when tx1 (scalar/rollback-txs [tx1]))
          (scalar/try-reconnection! test scalar/prepare-2pc-service!)
          (assoc op :type :fail :error {:tx-error "starting tx failed"})))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn workload
  [opts]
  {:client (->WriteReadClient (atom false))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_]
                 (->> wr/write-read-gen
                      (gen/limit 100)
                      (gen/process-limit
                       (:concurrency opts)))))
   :checker (scalar/independent-checker (wr/write-read-checker opts))})
