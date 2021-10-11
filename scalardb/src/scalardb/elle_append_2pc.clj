(ns scalardb.elle-append-2pc
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests.cycle.append :as append]
            [cassandra.conductors :as cond]
            [scalardb.core :as scalar])
  (:import (com.scalar.db.api Get
                              Put)
           (com.scalar.db.io IntValue
                             TextValue
                             Key)
           (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(def ^:private ^:const KEYSPACE "jepsen")
(def ^:private ^:const TABLE "txn")
(def ^:private ^:const DEFAULT_TABLE_COUNT 3)
(def ^:private ^:const SCHEMA {:id                     :int
                               :val                    :text
                               :tx_id                  :text
                               :tx_version             :int
                               :tx_state               :int
                               :tx_prepared_at         :bigint
                               :tx_committed_at        :bigint
                               :before_val             :text
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
      (.withValue (TextValue. VALUE value))))

(defn- get-value
  [r]
  (some-> r .get (.getValue VALUE) .get .getString .get))

(defn- tx-insert
  [tx table id value]
  (.put tx (prepare-put table id value)))

(defn- tx-update
  [tx table id prev value]
  (.put tx (prepare-put table id (str prev "," value))))

(defn- tx-execute
  [tx1 tx2 [f k v]]
  (let [key_hash (hash k)
        table (str TABLE (mod key_hash DEFAULT_TABLE_COUNT))
        tx (if (= (mod key_hash 2) 0) tx1 tx2)
        result (.get tx (prepare-get table k))]
    [f k (case f
           :r (when (.isPresent result)
                (mapv #(Long/valueOf %) (str/split (get-value result) #",")))
           :append (let [v' (str v)]
                     (if (.isPresent result)
                       (tx-update tx table k (get-value result) v')
                       (tx-insert tx table k v'))
                     v))]))

(defrecord AppendClient [initialized?]
  client/Client
  (open! [_ _ _]
    (AppendClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (doseq [i (range DEFAULT_TABLE_COUNT)]
          (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                                  :table (str TABLE i)
                                                  :schema SCHEMA}]))
        (scalar/prepare-2pc-service! test))))

  (invoke! [_ test op]
    (let [tx1 (scalar/start-2pc test)
          tx2 (scalar/join-2pc test (.getId tx1))
          txn (:value op)]
      (try
        (let [txn' (mapv (partial tx-execute tx1 tx2) txn)]
          (.prepare tx1)
          (.prepare tx2)
          (.validate tx1)
          (.validate tx2)
          (.commit tx1)
          (.commit tx2)
          (assoc op :type :ok :value txn'))
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

(defn- append-test
  [opts]
  (append/test {:key-count 10
                :min-txn-length 1
                :max-txn-length 10
                :max-writes-per-key 10
                :consistency-models [(:consistency-model opts)]}))

(defn elle-append-2pc-test
  [opts]
  (merge (scalar/scalardb-test (str "elle-append-2pc-" (:suffix opts))
                               {:unknown-tx (atom #{})
                                :failures (atom 0)
                                :generator (gen/phases
                                            (->> (:generator (append-test opts))
                                                 (gen/nemesis
                                                  (cond/mix-failure-seq opts))
                                                 (gen/time-limit
                                                  (:time-limit opts))))
                                :client (AppendClient. (atom false))
                                :checker (checker/compose
                                          {:clock
                                           (checker/clock-plot)
                                           :stats
                                           (checker/stats)
                                           :exceptions
                                           (checker/unhandled-exceptions)
                                           :workload
                                           (:checker (append-test opts))})})
         opts))
