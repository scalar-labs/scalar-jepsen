(ns scalardb.elle-append-2pc
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [scalardb.core :as scalar :refer [DEFAULT_TABLE_COUNT]]
            [scalardb.elle-append :as append])
  (:import (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(defn- tx-execute
  [seq-id tx1 tx2 [f k v]]
  (let [key_hash (hash k)
        table (str append/TABLE seq-id \_ (mod key_hash DEFAULT_TABLE_COUNT))
        tx (if (= (mod key_hash 2) 0) tx1 tx2)
        result (.get tx (append/prepare-get table k))]
    [f k (case f
           :r (when (.isPresent result)
                (mapv #(Long/valueOf %)
                      (str/split (append/get-value result) #",")))
           :append (let [v' (str v)]
                     (if (.isPresent result)
                       (append/tx-update tx table
                                         k (append/get-value result) v')
                       (append/tx-insert tx table k v'))
                     v))]))

(defrecord AppendClient [initialized?]
  client/Client
  (open! [_ _ _]
    (AppendClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (append/setup-tables test)
        (scalar/prepare-2pc-service! test))))

  (invoke! [_ test op]
    (let [tx1 (scalar/start-2pc test)
          tx2 (scalar/join-2pc test (.getId tx1))
          [seq-id txn] (:value op)]
      (try
        (when (<= @(:table-id test) seq-id)
          ;; add tables for the next sequence
          (append/add-tables test (inc seq-id)))
        (let [txn' (mapv (partial tx-execute seq-id tx1 tx2) txn)]
          (.prepare tx1)
          (.prepare tx2)
          (.validate tx1)
          (.validate tx2)
          (.commit tx1)
          (.commit tx2)
          (assoc op :type :ok :value (independent/tuple seq-id txn')))
        (catch UnknownTransactionStatusException _
          (swap! (:unknown-tx test) conj (.getId tx1))
          (assoc op :type :info :error {:unknown-tx-status (.getId tx1)}))
        (catch Exception e
          (.rollback tx1)
          (.rollback tx2)
          (scalar/try-reconnection-for-2pc! test)
          (assoc op :type :fail :error {:crud-error (.getMessage e)})))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn workload
  [opts]
  {:client (->AppendClient (atom false))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_]
                 (->> (:generator (append/append-test opts))
                      (gen/limit 100)
                      (gen/process-limit
                       (:concurrency opts)))))
   :checker (scalar/independent-checker
             (:checker (append/append-test opts)))})
