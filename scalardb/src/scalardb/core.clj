(ns scalardb.core
  (:require [cassandra.core :as c]
            [clojure.string :as string]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.independent :as independent]
            [jepsen.tests :as tests]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (com.scalar.db.api TransactionState)
           (com.scalar.db.config DatabaseConfig)
           (com.scalar.db.service StorageModule
                                  StorageService
                                  TransactionModule
                                  TransactionService
                                  TwoPhaseCommitTransactionService)
           (com.scalar.db.transaction.consensuscommit Coordinator)
           (com.google.inject Guice)
           (java.util Properties)))

(def ^:const RETRIES 8)
(def ^:const RETRIES_FOR_RECONNECTION 3)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(def ^:private ^:const COORDINATOR "coordinator")
(def ^:private ^:const STATE_TABLE "state")
(def ^:const VERSION "tx_version")

(def ^:private ISOLATION_LEVELS {:snapshot "SNAPSHOT"
                                 :serializable "SERIALIZABLE"})

(def ^:private SERIALIZABLE_STRATEGIES {:extra-read "EXTRA_READ"
                                        :extra-write "EXTRA_WRITE"})

(defn setup-transaction-tables
  [test schemata]
  (let [cluster (alia/cluster {:contact-points (:nodes test)})
        session (alia/connect cluster)]
    (doseq [schema schemata]
      (c/create-my-keyspace session test schema)
      (c/create-my-table session schema))

    (c/create-my-keyspace session test {:keyspace COORDINATOR})
    (c/create-my-table session {:keyspace COORDINATOR
                                :table    STATE_TABLE
                                :schema   {:tx_id         :text
                                           :tx_state      :int
                                           :tx_created_at :bigint
                                           :primary-key   [:tx_id]}})
    (c/close-cassandra cluster session)))

(defn- create-properties
  [test nodes]
  (doto (Properties.)
    (.setProperty "scalar.db.contact_points" (string/join "," nodes))
    (.setProperty "scalar.db.username" "cassandra")
    (.setProperty "scalar.db.password" "cassandra")
    (.setProperty "scalar.db.isolation_level"
                  ((:isolation-level test) ISOLATION_LEVELS))
    (.setProperty "scalar.db.consensus_commit.serializable_strategy"
                  ((:serializable-strategy test) SERIALIZABLE_STRATEGIES))))

(defn- close-storage!
  [test]
  (let [storage (:storage test)]
    (locking storage
      (when @storage
        (.close @storage)
        (reset! storage nil)
        (info "The current storage service closed")))))

(defn- close-transaction!
  [test]
  (let [transaction (:transaction test)]
    (locking transaction
      (when @transaction
        (.close @transaction)
        (reset! transaction nil)
        (info "The current transaction service closed")))))

(defn- close-2pc!
  [test]
  (let [tx (:2pc test)]
    (locking tx
      (when @tx
        (.close @tx)
        (reset! tx nil)
        (info "The current 2pc service closed")))))

(defn close-all!
  [test]
  (close-storage! test)
  (close-transaction! test)
  (close-2pc! test))

(defn- create-service-instance
  [test mode]
  (when-let [config (some->> (c/live-nodes test)
                             not-empty
                             (create-properties test)
                             DatabaseConfig.)]
    (let [[module service] (condp = mode
                             :storage [(StorageModule. config)
                                       StorageService]
                             :transaction [(TransactionModule. config)
                                           TransactionService]
                             :2pc [(TransactionModule. config)
                                   TwoPhaseCommitTransactionService])
          injector (Guice/createInjector (vector module))]
      (try
        (.getInstance injector service)
        (catch Exception e
          (warn (.getMessage e)))))))

(defn- prepare-service!
  [test mode]
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (c/exponential-backoff (- RETRIES tries)))
    (if-not (pos? tries)
      (warn "Failed to connect to the cluster")
      (if-let [instance (create-service-instance test mode)]
        (do
          (condp = mode
            :storage (close-storage! test)
            :transaction (close-transaction! test)
            :2pc (close-2pc! test))
          (reset! (mode test) instance)
          (info "reconnected to the cluster"))
        (when-not @(mode test)
          (recur (dec tries)))))))

(defn prepare-storage-service!
  [test]
  (prepare-service! test :storage))

(defn prepare-transaction-service!
  [test]
  (prepare-service! test :transaction))

(defn prepare-2pc-service!
  [test]
  (prepare-service! test :2pc))

(defn check-storage-connection!
  [test]
  (when-not @(:storage test)
    (prepare-storage-service! test)))

(defn check-transaction-connection!
  [test]
  (when-not @(:transaction test)
    (prepare-transaction-service! test)))

(defn try-reconnection-for-transaction!
  [test]
  (when (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (prepare-transaction-service! test)
    (reset! (:failures test) 0)))

(defn try-reconnection-for-2pc!
  [test]
  (when (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (prepare-2pc-service! test)
    (reset! (:failures test) 0)))

(defn start-transaction
  [test]
  (some-> test :transaction deref .start))

(defn start-2pc
  [test]
  (some-> test :2pc deref .start))

(defn join-2pc
  [test tx-id]
  (some-> test :2pc deref (.join tx-id)))

(defmacro with-retry
  "If the result of the body is nil, it retries it"
  [connect-fn test & body]
  `(loop [tries# RETRIES]
     (when (< tries# RETRIES)
       (c/exponential-backoff (- RETRIES tries#)))
     (when (zero? (mod tries# RETRIES_FOR_RECONNECTION))
       (~connect-fn ~test))
     (if-let [results# ~@body]
       results#
       (if (pos? tries#)
         (recur (dec tries#))
         (throw (ex-info "Failed to read records"
                         {:cause "Failed to read records"}))))))

(defn- retry-when-exception*
  [tries f args fallback]
  (when (pos? tries)
    (let [res (try {:value (apply f args)}
                   (catch Exception e
                     (if (= tries 1)
                       (throw e)
                       {:exception e})))]
      (if-let [e (:exception res)]
        (do
          (warn e)
          (when fallback (fallback))
          (c/exponential-backoff (- RETRIES tries))
          (recur (dec tries) f args fallback))
        (:value res)))))

(defn retry-when-exception
  [f args & fallback]
  (retry-when-exception* RETRIES f args fallback))

(defn- is-committed-state?
  "Return true if the status is COMMITTED. Return nil if the read fails."
  [coordinator id]
  (try
    (let [state (.getState coordinator id)]
      (and (.isPresent state)
           (-> state .get .getState (.equals TransactionState/COMMITTED))))
    (catch Exception _ nil)))

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

(defn- independent-stats-checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [result (checker/check (checker/stats) test history opts)]
        ;; ignore if no transaction succeeded
        (if (and (not (:valid? result))
                 (zero? (:ok-count result)))
          (assoc result :valid? true)
          result)))))

(defn- independent-workload-checker
  [workload-checker]
  (reify checker/Checker
    (check [_ test history opts]
      (let [result (checker/check workload-checker test history opts)]
        ;; ignore if no transaction succeeded
        (if (and (= (:valid? result) :unknown)
                 (= (:anomalies result) {:empty-transaction-graph true}))
          (assoc result :valid? true)
          result)))))

(defn independent-checker
  "wrapped checker for jepsen/independent"
  [workload-checker]
  (independent/checker
   (checker/compose
    {:stats (independent-stats-checker)
     :exceptions (checker/unhandled-exceptions)
     :workload (independent-workload-checker workload-checker)})))

(defn scalardb-test
  [name opts]
  (merge tests/noop-test
         {:name        (str "scalardb-" name)
          :storage     (atom nil)
          :transaction (atom nil)
          :2pc (atom nil)}
         opts))
