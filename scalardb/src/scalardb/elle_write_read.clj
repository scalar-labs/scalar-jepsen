(ns scalardb.elle-write-read
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.tests.cycle.wr :as wr]
            [cassandra.conductors :as cond]
            [scalardb.core :as scalar])
  (:import (com.scalar.db.api Get
                              Put
                              PutIfNotExists)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(def ^:private ^:const KEYSPACE "jepsen")
(def ^:private ^:const TABLE "txn")
(def ^:private ^:const NUM_TABLE_ID_COUNT 2)
(def ^:private ^:const DEFAULT_TABLE_COUNT 3)
(def ^:private ^:const SCHEMA {:id                     :int
                               :val                    :int
                               :tx_id                  :text
                               :tx_version             :int
                               :tx_state               :int
                               :tx_prepared_at         :bigint
                               :tx_committed_at        :bigint
                               :before_val             :int
                               :before_tx_id           :text
                               :before_tx_version      :int
                               :before_tx_state        :int
                               :before_tx_prepared_at  :bigint
                               :before_tx_committed_at :bigint
                               :primary-key [:id]})
(def ^:private ^:const ID "id")
(def ^:private ^:const VALUE "val")

(defn- prepare-get
  [table id]
  (-> (Key. [(IntValue. ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable table)))

(defn- prepare-put
  [table id value insert?]
  (let [put (-> (Key. [(IntValue. ID id)])
                (Put.)
                (.forNamespace KEYSPACE)
                (.forTable table)
                (.withValue (IntValue. VALUE value)))]
    (if insert?
      (.withCondition put (PutIfNotExists.))
      put)))

(defn- get-value
  [r]
  (some-> r .get (.getValue VALUE) .get .get long))

(defn- tx-insert
  [tx table id value]
  (.put tx (prepare-put table id value true))
  value)

(defn- tx-update
  [tx table id value]
  (.put tx (prepare-put table id value false))
  value)

(defn- tx-execute
  [seq-id tx [f k v]]
  (let [table (str TABLE seq-id \_ (mod (hash k) DEFAULT_TABLE_COUNT))
        result (.get tx (prepare-get table k))]
    [f k (case f
           :r (when (.isPresent result) (get-value result))
           :w (if (.isPresent result)
                (tx-update tx table k v)
                (tx-insert tx table k v)))]))

(defn- add-tables
  [test next-id]
  (let [current-id @(:table-id test)]
    (when (< current-id next-id)
      (locking (:table-id test)
        (when (compare-and-set! (:table-id test) current-id next-id)
          (info (str "Creating new tables for " next-id))
          (doseq [i (range DEFAULT_TABLE_COUNT)]
            (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                                    :table (str TABLE
                                                                next-id
                                                                \_
                                                                i)
                                                    :schema SCHEMA}])))))))

(defrecord WriteReadClient [initialized?]
  client/Client
  (open! [_ _ _]
    (WriteReadClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (doseq [id (range NUM_TABLE_ID_COUNT)
                i (range DEFAULT_TABLE_COUNT)]
          (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                                  :table (str TABLE id \_ i)
                                                  :schema SCHEMA}]))
        (scalar/prepare-transaction-service! test))))

  (invoke! [_ test op]
    (let [tx (scalar/start-transaction test)
          [seq-id txn] (:value op)]
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
          (scalar/try-reconnection-for-transaction! test)
          (assoc op :type :fail :error {:crud-error (.getMessage e)})))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn- write-read-gen
  []
  (wr/gen {:key-count 10
           :min-txn-length 1
           :max-txn-length 10
           :max-writes-per-key 10}))

(defn- write-read-checker
  [opts]
  (wr/checker {:consistency-models (:consistency-model opts)}))

(defn elle-write-read-test
  [opts]
  (merge (scalar/scalardb-test
          (str "elle-wr-" (:suffix opts))
          {:table-id (atom (dec NUM_TABLE_ID_COUNT))
           :unknown-tx (atom #{})
           :failures (atom 0)
           :generator (->> (independent/concurrent-generator
                            (:concurrency opts)
                            (range)
                            (fn [_]
                              (->> write-read-gen
                                   (gen/limit 100)
                                   (gen/process-limit
                                    (:concurrency opts)))))
                           (gen/nemesis
                            (cond/mix-failure-seq opts))
                           (gen/time-limit (:time-limit opts)))
           :client (WriteReadClient. (atom false))
           :checker (scalar/independent-checker (write-read-checker opts))})
         opts))
