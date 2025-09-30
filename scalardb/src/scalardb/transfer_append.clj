(ns scalardb.transfer-append
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [infof warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [scalardb.core :as scalar :refer [KEYSPACE]]
            [scalardb.db-extend :refer [wait-for-recovery]]
            [scalardb.transfer :as transfer])
  (:import (com.scalar.db.api Put
                              Scan
                              Scan$Ordering
                              Scan$Ordering$Order
                              Result)
           (com.scalar.db.exception.transaction UnknownTransactionStatusException)
           (com.scalar.db.io IntValue
                             Key)))

(def ^:private ^:const TABLE "transfer")
(def ^:private ^:const ACCOUNT_ID "account_id")
(def ^:private ^:const BALANCE "balance")
(def ^:private ^:const AGE "age")
(def ^:const SCHEMA {(keyword (str KEYSPACE \. TABLE))
                     {:transaction true
                      :partition-key [ACCOUNT_ID]
                      :clustering-key [AGE]
                      :columns {(keyword ACCOUNT_ID) "INT"
                                (keyword AGE) "INT"
                                (keyword BALANCE) "INT"}}})

(defn setup-tables
  [test]
  (scalar/setup-transaction-tables test [SCHEMA]))

(defn- prepare-scan
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Scan.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withOrdering (Scan$Ordering. AGE Scan$Ordering$Order/DESC))))

(defn prepare-scan-for-latest
  [id]
  (-> id prepare-scan (.withLimit 1)))

(defn scan-for-latest
  [tx scan]
  (first (.scan tx scan)))

(defn prepare-put
  [id age balance]
  (-> (Put. (Key. [(IntValue. ACCOUNT_ID id)]) (Key. [(IntValue. AGE age)]))
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue (IntValue. BALANCE balance))))

(defn populate-accounts
  "Insert initial records with transaction.
  This method assumes that n is small (< 100)"
  [test n balance]
  (scalar/retry-when-exception
   (fn [num]
     (let [tx (scalar/start-transaction test)]
       (dotimes [i num]
         (.put tx (prepare-put i 1 balance)))
       (.commit tx)))
   [n]))

(defn get-balance
  [^Result result]
  (-> result (.getValue BALANCE) .get .get))

(defn get-age
  [^Result result]
  (-> result (.getValue AGE) .get .get))

(defn get-balances
  [results]
  (mapv #(get-balance (first %)) results))

(defn get-ages
  [results]
  (mapv #(get-age (first %)) results))

(defn get-nums
  [results]
  (mapv count results))

(defn calc-new-balance
  [^Result r amount]
  (-> r get-balance (+ amount)))

(defn calc-new-age
  [^Result r]
  (-> r get-age inc))

(defn- tx-transfer
  [tx from to amount]
  (infof "Transferring %d from %d to %d (tx: %s)" amount from to (.getId tx))
  (let [^Result from-result (scan-for-latest tx (prepare-scan-for-latest from))
        ^Result to-result (scan-for-latest tx (prepare-scan-for-latest to))]
    (infof "fromID: %d, the latest balance: %d, the latest age: %d (tx: %s)" from (get-balance from-result) (get-age from-result) (.getId tx))
    (->> (prepare-put from
                      (calc-new-age from-result)
                      (calc-new-balance from-result (- amount)))
         (.put tx))
    (infof "toID: %d, the latest balance: %d, the latest age: %d (tx: %s)" to (get-balance to-result) (get-age to-result) (.getId tx))
    (->> (prepare-put to
                      (calc-new-age to-result)
                      (calc-new-balance to-result amount))
         (.put tx))
    (.commit tx)
    (infof "Transferring %d from %d to %d succeeded (tx: %s)" amount from to (.getId tx))))

(defn- try-tx-transfer
  [test {:keys [from to amount]}]
  (if-let [tx (try (scalar/start-transaction test)
                   (catch Exception e (warn e "Starting a transaction failed")))]
    (try
      (tx-transfer tx from to amount)
      :commit
      (catch UnknownTransactionStatusException e
        (swap! (:unknown-tx test) conj (.getId tx))
        (warn e "Unknown transaction: " (.getId tx))
        :unknown-tx-status)
      (catch Exception e
        (warn e "An error occurred during the transaction")
        (scalar/rollback-txs [tx])
        :fail))
    :start-fail))

(defn- scan-all-records
  "Scan all records and append new records with a transaction"
  [test n]
  (try
    (let [tx (scalar/start-transaction test)
          results (map #(.scan tx (prepare-scan %)) (range n))]
      ;; Put the same balance to check conflicts with in-flight transactions
      (mapv #(->> (prepare-put %1
                               (-> %2 first calc-new-age)
                               (-> %2 first (calc-new-balance 0)))
                  (.put tx))
            (range n)
            results)
      (.commit tx)
      results)
    (catch Exception e
      (warn e "scan-all failed.")
      nil)))

(defn scan-all-records-with-retry
  [test n]
  (scalar/check-transaction-connection! test)
  (scalar/with-retry
    scalar/prepare-transaction-service!
    test
    (scan-all-records test n)))

(defrecord TransferClient [initialized? n initial-balance max-txs]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance max-txs))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (setup-tables test)
        (scalar/prepare-transaction-service! test)
        (populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (transfer/exec-transfers test op try-tx-transfer)
      :get-all (do
                 (wait-for-recovery (:db test) test)
                 (if-let [results (scan-all-records-with-retry test n)]
                   (assoc op :type, :ok :value {:balance (get-balances results)
                                                :age (get-ages results)
                                                :num (get-nums results)})
                   (assoc op :type, :fail, :error "Failed to get all records")))
      :check-tx (if-let [num-committed (scalar/check-transaction-states
                                        test
                                        @(:unknown-tx test))]
                  (assoc op :type :ok, :value num-committed)
                  (assoc op :type :fail, :error "Failed to check status"))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn get-all
  [_ _]
  {:type :invoke
   :f    :get-all})

(defn check-tx
  [_ _]
  {:type :invoke
   :f    :check-tx})

(defn consistency-checker
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [num-accs (-> test :client :n)
            initial-balance (-> test :client :initial-balance)
            total-balance (* num-accs initial-balance)
            read-result (->> history
                             (r/filter #(= :get-all (:f %)))
                             (into [])
                             last
                             :value)
            actual-balance (->> (:balance read-result)
                                (reduce +))
            bad-balance (when-not (= actual-balance total-balance)
                          {:type     :wrong-balance
                           :expected total-balance
                           :actual   actual-balance})
            actual-age (->> (:age read-result)
                            (reduce +))
            expected-age (->> (:num read-result)
                              (reduce +))
            bad-age (when-not (= actual-age expected-age)
                      {:type     :wrong-age
                       :expected expected-age
                       :actual   actual-age})
            checked-committed (->> history
                                   (r/filter #(= :check-tx (:f %)))
                                   (into [])
                                   last
                                   ((fn [x]
                                      (if (= (:type x) :ok) (:value x) 0))))]
        {:valid?               (and (empty? bad-balance) (empty? bad-age))
         :total-balance        actual-balance
         :total-age            actual-age
         :committed-unknown-tx checked-committed
         :bad-balance          bad-balance
         :bad-age              bad-age}))))

(defn workload
  [_]
  {:client (->TransferClient (atom false)
                             transfer/NUM_ACCOUNTS
                             transfer/INITIAL_BALANCE
                             transfer/MAX_NUM_TXS)
   :generator [transfer/transfer]
   :final-generator (gen/phases
                     (gen/once get-all)
                     (gen/once check-tx))
   :checker (consistency-checker)})
