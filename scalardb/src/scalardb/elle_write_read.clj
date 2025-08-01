(ns scalardb.elle-write-read
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.tests.cycle.wr :as wr]
            [scalardb.core :as scalar :refer [KEYSPACE
                                              INITIAL_TABLE_ID
                                              DEFAULT_TABLE_COUNT]])
  (:import (com.scalar.db.api Get
                              Put)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(def ^:const TABLE "txn")
(def ^:private ^:const ID "id")
(def ^:private ^:const VALUE "val")
(def ^:const SCHEMA {:transaction true
                     :partition-key [ID]
                     :clustering-key []
                     :columns {(keyword ID) "INT" (keyword VALUE) "INT"}})

(defn prepare-get
  [table id]
  (-> (Key. [(IntValue. ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable table)))

(defn- prepare-put
  [table id value]
  (-> (Key. [(IntValue. ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable table)
      (.withValue (IntValue. VALUE value))))

(defn get-value
  [r]
  (some-> r .get (.getValue VALUE) .get .get long))

(defn- tx-read
  [tx table id]
  (let [result (.get tx (prepare-get table id))]
    (when (.isPresent result)
      (get-value result))))

(defn tx-write
  [tx table id value]
  (.put tx (prepare-put table id value))
  value)

(defn- tx-execute
  [seq-id tx [f k v]]
  (let [table (str TABLE seq-id \_ (mod (hash k) DEFAULT_TABLE_COUNT))]
    [f k (case f
           :r (tx-read tx table k)
           :w (do
                (tx-read tx table k)
                (tx-write tx table k v)))]))

(defn setup-tables
  [test]
  (doseq [id (range (inc INITIAL_TABLE_ID))
          i (range DEFAULT_TABLE_COUNT)]
    (scalar/setup-transaction-tables
     test [{(keyword (str KEYSPACE \. TABLE id \_ i)) SCHEMA}])))

(defn add-tables
  [test next-id]
  (let [current-id @(:table-id test)]
    (when (< current-id next-id)
      (locking (:table-id test)
        (when (compare-and-set! (:table-id test) current-id next-id)
          (info (str "Creating new tables for " next-id))
          (doseq [i (range DEFAULT_TABLE_COUNT)]
            (scalar/setup-transaction-tables
             test
             [{(keyword (str KEYSPACE \. TABLE next-id \_ i)) SCHEMA}])))))))

(defrecord WriteReadClient [initialized?]
  client/Client
  (open! [_ _ _]
    (WriteReadClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (setup-tables test)
        (scalar/prepare-transaction-service! test))))

  (invoke! [_ test op]
    (if-let [tx (try (scalar/start-transaction test)
                     (catch Exception e (warn (.getMessage e))))]
      (let [[seq-id txn] (:value op)]
        (try
          (when (<= @(:table-id test) seq-id)
            ;; add tables for the next sequence
            (add-tables test (inc seq-id)))
          (let [txn' (mapv (partial tx-execute seq-id tx) txn)]
            (.commit tx)
            (assoc op :type :ok :value (independent/tuple seq-id txn')))
          (catch UnknownTransactionStatusException _
            (swap! (:unknown-tx test) conj (.getId tx))
            (assoc op :type :info :error {:unknown-tx-status (.getId tx)}))
          (catch Exception e
            (scalar/rollback-txs [tx])
            (scalar/try-reconnection! test scalar/prepare-transaction-service!)
            (assoc op :type :fail :error {:crud-error (.getMessage e)}))))
      (do
        (scalar/try-reconnection! test scalar/prepare-transaction-service!)
        (assoc op :type :fail :error {:tx-error "starting tx failed"}))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn write-read-gen
  []
  (wr/gen {:key-count 10
           :min-txn-length 1
           :max-txn-length 10
           :max-writes-per-key 10}))

(defn write-read-checker
  [opts]
  (wr/checker {:consistency-models (:consistency-model opts)}))

(defn workload
  [opts]
  {:client (->WriteReadClient (atom false))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_]
                 (->> write-read-gen
                      (gen/limit 100)
                      (gen/process-limit
                       (:concurrency opts)))))
   :checker (scalar/independent-checker (write-read-checker opts))})
