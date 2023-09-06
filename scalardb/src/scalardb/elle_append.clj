(ns scalardb.elle-append
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.tests.cycle.append :as append]
            [scalardb.core :as scalar :refer [KEYSPACE
                                              INITIAL_TABLE_ID
                                              DEFAULT_TABLE_COUNT]])
  (:import (com.scalar.db.api Get
                              Put)
           (com.scalar.db.io IntValue
                             TextValue
                             Key)
           (com.scalar.db.exception.transaction
            UnknownTransactionStatusException)))

(def ^:const TABLE "txn")
(def ^:const SCHEMA {:id                     :int
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

(defn prepare-get
  [table id]
  (-> (Key. [(IntValue. ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable table)))

(defn prepare-put
  [table id value]
  (-> (Key. [(IntValue. ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable table)
      (.withValue (TextValue. VALUE value))))

(defn get-value
  [r]
  (some-> r .get (.getValue VALUE) .get .getString .get))

(defn tx-insert
  [tx table id value]
  (.put tx (prepare-put table id value)))

(defn tx-update
  [tx table id prev value]
  (.put tx (prepare-put table id (str prev "," value))))

(defn- tx-execute
  [seq-id tx [f k v]]
  (let [table (str TABLE seq-id \_ (mod (hash k) DEFAULT_TABLE_COUNT))]
    [f k (case f
           :r (let [result (.get tx (prepare-get table k))]
                (when (.isPresent result)
                  (mapv #(Long/valueOf %)
                        (str/split (get-value result) #","))))
           :append (let [v' (str v)
                         result (.get tx (prepare-get table k))]
                     (if (.isPresent result)
                       (tx-update tx table k (get-value result) v')
                       (tx-insert tx table k v'))
                     v))]))

(defn setup-tables
  [test]
  (doseq [id (range (inc INITIAL_TABLE_ID))
          i (range DEFAULT_TABLE_COUNT)]
    (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                            :table (str TABLE id \_ i)
                                            :schema SCHEMA}])))

(defn add-tables
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

(defrecord AppendClient [initialized?]
  client/Client
  (open! [_ _ _]
    (AppendClient. initialized?))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (setup-tables test)
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

(defn append-test
  [opts]
  (append/test {:key-count 10
                :min-txn-length 1
                :max-txn-length 10
                :max-writes-per-key 10
                :consistency-models (:consistency-model opts)}))

(defn workload
  [opts]
  {:client (->AppendClient (atom false))
   :generator (independent/concurrent-generator
               (:concurrency opts)
               (range)
               (fn [_]
                 (->> (:generator (append-test opts))
                      (gen/limit 100)
                      (gen/process-limit
                       (:concurrency opts)))))
   :checker (scalar/independent-checker (:checker (append-test opts)))})
