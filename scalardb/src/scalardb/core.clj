(ns scalardb.core
  (:require [cheshire.core :as cheshire]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.independent :as independent]
            [scalardb.db-extend :as ext])
  (:import (com.scalar.db.api TransactionState)
           (com.scalar.db.schemaloader SchemaLoader)
           (com.scalar.db.service TransactionFactory
                                  StorageFactory)
           (com.scalar.db.transaction.consensuscommit Coordinator)))

(def ^:const RETRIES 8)
(def ^:const RETRIES_FOR_RECONNECTION 3)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)
(def ^:const INITIAL_TABLE_ID 1)
(def ^:const DEFAULT_TABLE_COUNT 3)

(def ^:const KEYSPACE "jepsen")
(def ^:const VERSION "tx_version")

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn setup-transaction-tables
  [test schemata]
  (let [properties (ext/create-properties (:db test) test)
        options (ext/create-table-opts (:db test) test)]
    (doseq [schema (map cheshire/generate-string schemata)]
      (loop [retries RETRIES]
        (when (zero? retries)
          (throw (ex-info "Failed to set up tables" {:schema schema})))
        (when (< retries RETRIES)
          (exponential-backoff (- RETRIES retries))
          (try
            (SchemaLoader/unload properties schema true)
            (catch Exception e (warn (.getMessage e))))
          (exponential-backoff (- RETRIES retries)))
        (let [result (try
                       (SchemaLoader/load properties schema options true)
                       :success
                       (catch Exception e
                         (warn (.getMessage e))
                         :fail))]
          (when (= result :fail)
            (recur (dec retries))))))))

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
  (let [tms (:2pc test)]
    (locking tms
      (mapv #(.close %) @tms)
      (reset! tms nil)
      (info "The current 2pc service closed"))))

(defn close-all!
  [test]
  (close-storage! test)
  (close-transaction! test)
  (close-2pc! test))

(defn- create-service-instance
  [test mode]
  (when-let [properties (ext/create-properties (:db test) test)]
    (try
      (condp = mode
        :storage (.getStorage (StorageFactory/create properties))
        :transaction (.getTransactionManager
                      (TransactionFactory/create properties))
        :2pc (let [factory (TransactionFactory/create properties)]
               ; create two Two-phase commit transaction managers
               [(.getTwoPhaseCommitTransactionManager factory)
                (.getTwoPhaseCommitTransactionManager factory)]))
      (catch Exception e
        (warn (.getMessage e))))))

(defn- prepare-service!
  [test mode]
  (info "reconnecting to the cluster")
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
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
  ; use the first transaction manager to start a transaction
  (some-> test :2pc deref first .start))

(defn join-2pc
  [test tx-id]
  ; use the second transaction manager to join a transaction
  (some-> test :2pc deref second (.join tx-id)))

(defn prepare-validate-commit-txs
  "Given transactions as a vector are prepared, validated,
  then committed for 2pc."
  [txs]
  (doseq [f [#(.prepare %) #(.validate %) #(.commit %)]
          tx txs]
    (f tx)))

(defn rollback-txs
  "Given transactions as a vector are rollbacked."
  [txs]
  (doseq [tx txs] (.rollback tx)))

(defmacro with-retry
  "If the result of the body is nil, it retries it"
  [connect-fn test & body]
  `(loop [tries# RETRIES]
     (when (< tries# RETRIES)
       (exponential-backoff (- RETRIES tries#)))
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
          (exponential-backoff (- RETRIES tries))
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
