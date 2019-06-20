(ns scalardb.core
  (:require [cassandra.core :as c]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.tests :as tests]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (com.scalar.database.api TransactionState)
           (com.scalar.database.config DatabaseConfig)
           (com.scalar.database.storage.cassandra Cassandra)
           (com.scalar.database.service StorageModule
                                        StorageService
                                        TransactionModule
                                        TransactionService)
           (com.scalar.database.transaction.consensuscommit Coordinator)
           (com.google.inject Guice)
           (java.util Properties)))

(def ^:const RETRIES 8)
(def ^:const RETRIES_FOR_RECONNECTION 3)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(def ^:private ^:const COORDINATOR "coordinator")
(def ^:private ^:const STATE_TABLE "state")
(def ^:const VERSION "tx_version")

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn create-transfer-table
  [test session {:keys [keyspace table schema]}]
  (alia/execute session (create-keyspace keyspace
                                         (if-exists false)
                                         (with {:replication {"class"              "SimpleStrategy"
                                                              "replication_factor" (:rf test)}})))
  (alia/execute session (use-keyspace keyspace))
  (alia/execute session (create-table table
                                      (if-exists false)
                                      (column-definitions schema)))
  (alia/execute session (alter-table table
                                     (with {:compaction {:class :SizeTieredCompactionStrategy}}))))

(defn create-coordinator-table
  [test session]
  (alia/execute session (create-keyspace COORDINATOR
                                         (if-exists false)
                                         (with {:replication {"class"              "SimpleStrategy"
                                                              "replication_factor" (:rf test)}})))
  (alia/execute session (use-keyspace COORDINATOR))
  (alia/execute session (create-table STATE_TABLE
                                      (if-exists false)
                                      (column-definitions {:tx_id         :text
                                                           :tx_state      :int
                                                           :tx_created_at :bigint
                                                           :primary-key   [:tx_id]}))))

(defn setup-transaction-tables
  [test schemata]
  (let [session (alia/connect
                 (alia/cluster {:contact-points (->> test :nodes (map name))}))]
    (doseq [schema schemata]
      (create-transfer-table test session schema))
    (create-coordinator-table test session)
    (alia/shutdown session)))

(defn- create-properties
  [nodes]
  (let [props (Properties.)]
    (if (= (count nodes) 1)
      (.setProperty props
                    "scalar.database.contact_points"
                    (first nodes))
      (.setProperty props
                    "scalar.database.contact_points"
                    (reduce #(str %1 "," %2) nodes)))
    (.setProperty props "scalar.database.username" "cassandra")
    (.setProperty props "scalar.database.password" "cassandra")
    props))

(defn- close-storage!
  [test]
  (let [storage (:storage test)]
    (locking storage
      (when-not (nil? @storage)
        (.close @storage)
        (reset! storage nil)
        (info "The current storage service closed")))))

(defn- close-transaction!
  [test]
  (let [transaction (:transaction test)]
    (locking transaction
      (when-not (nil? @transaction)
        (.close @transaction)
        (reset! transaction nil)
        (info "The current transaction service closed")))))

(defn close-all!
  [test]
  (close-storage! test)
  (close-transaction! test))

(defn prepare-storage-service!
  [test]
  (close-storage! test)
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (if-not (pos? tries)
      (warn "Failed to connect to the cluster")
      (if-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 StorageModule.
                                 vector
                                 Guice/createInjector)]
        (try
          (->> (.getInstance injector StorageService)
               (reset! (:storage test)))
          (catch Exception e
            (warn (.getMessage e))))
        (when-not (nil? (:storage test))
          (recur (dec tries)))))))

(defn prepare-transaction-service!
  [test]
  (close-transaction! test)
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (if-not (pos? tries)
      (warn "Failed to connect to the cluster")
      (if-let [injector (some->> (c/live-nodes test)
                                 not-empty
                                 create-properties
                                 DatabaseConfig.
                                 TransactionModule.
                                 vector
                                 Guice/createInjector)]
        (try
          (->> (.getInstance injector TransactionService)
               (reset! (:transaction test)))
          (catch Exception e
            (warn (.getMessage e))))
        (when-not (nil? (:transaction test))
          (recur (dec tries)))))))

(defn check-connection!
  [test]
  (when (nil? @(:transaction test))
    (prepare-transaction-service! test)))

(defn try-reconnection!
  [test]
  (when (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (prepare-transaction-service! test)
    (reset! (:failures test) 0)))

(defn start-transaction
  [test]
  (some-> test :transaction deref .start))

(defmacro with-retry
  [connect-fn test & body]
  "If the result of the body is nil, it retries it"
  `(loop [tries# RETRIES]
     (when (< tries# RETRIES)
       (exponential-backoff (- RETRIES tries#)))
     (when (zero? (mod tries# RETRIES_FOR_RECONNECTION))
       (~connect-fn ~test))
     (let [results# ~@body]
       (if-not (nil? results#)
         results#
         (if (pos? tries#)
           (recur (dec tries#))
           (throw (ex-info "Failed to read records"
                           {:cause "Failed to read records"})))))))

(defn- is-committed-state?
  "Return true if the status is COMMITTED. Return nil if the read fails."
  [coordinator id]
  (try
    (let [state (.getState coordinator id)]
      (and (.isPresent state)
           (-> state .get .getState (.equals TransactionState/COMMITTED))))
    (catch Exception e nil)))

(defn check-transaction-states
  "Return the number of COMMITTED states by checking the coordinator."
  [test ids]
  (if (seq ids)
    (do
      (when (nil? @(:storage test))
        (prepare-storage-service! test))
      (with-retry prepare-storage-service! test
        (let [coordinator (Coordinator. @(:storage test))
              committed (map (partial is-committed-state? coordinator) ids)]
          (if (some nil? committed)
            nil
            (count (filter true? committed))))))
    0))

(defn scalardb-test
  [name opts]
  (merge tests/noop-test
         {:name        (str "scalardb-" name)
          :storage     (atom nil)
          :transaction (atom nil)}
         opts))
