(ns scalardb.elle-write-read
  (:require [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle.wr :as wr]
            [cassandra.conductors :as cond]
            [scalardb.core :as scalar])
  (:import (com.scalar.db.api Get
                              Put)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(def ^:private ^:const KEYSPACE "jepsen")
(def ^:private ^:const TABLE "txn")
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
  [table id value]
  (-> (Key. [(IntValue. ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable table)
      (.withValue (IntValue. VALUE value))))

(defn- get-value
  [r]
  (some-> r .get (.getValue VALUE) .get .get long))

(defn- tx-write
  [tx table id value]
  (.put tx (prepare-put table id value))
  value)

(defn- tx-execute
  [tx [f k v]]
  (let [table (str TABLE (mod (hash k) DEFAULT_TABLE_COUNT))
        result (.get tx (prepare-get table k))]
    [f k (case f
           :r (when (.isPresent result) (get-value result))
           :w (tx-write tx table k v))]))

(defrecord WriteReadClient [initialized?]
  client/Client
  (open! [_ _ _]
    (WriteReadClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (doseq [i (range DEFAULT_TABLE_COUNT)]
          (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                                  :table (str TABLE i)
                                                  :schema SCHEMA}]))
        (scalar/prepare-transaction-service! test))))

  (invoke! [_ test op]
    (let [tx (scalar/start-transaction test)
          txn (:value op)]
      (try
        (let [txn' (mapv (partial tx-execute tx) txn)]
          (.commit tx)
          (assoc op :type :ok :value txn'))
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
  (wr/checker {:consistency-models [(:consistency-model opts)]}))

(defn elle-write-read-test
  [opts]
  (merge (scalar/scalardb-test (str "elle-wr-" (:suffix opts))
                               {:unknown-tx (atom #{})
                                :failures (atom 0)
                                :generator (gen/phases
                                            (->> (write-read-gen)
                                                 (gen/nemesis
                                                  (cond/mix-failure-seq opts))
                                                 (gen/time-limit
                                                  (:time-limit opts))))
                                :client (WriteReadClient. (atom false))
                                :checker (checker/compose
                                          {:clock
                                           (checker/clock-plot)
                                           :stats
                                           (checker/stats)
                                           :exceptions
                                           (checker/unhandled-exceptions)
                                           :workload
                                           (write-read-checker opts)})})
         opts))
