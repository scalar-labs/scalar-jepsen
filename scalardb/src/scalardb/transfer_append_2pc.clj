(ns scalardb.transfer-append-2pc
  (:require [cassandra.core :as cassandra]
            [clojure.core.reducers :as r]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [scalardb.core :as scalar])
  (:import (com.scalar.db.api Put
                              Scan
                              Scan$Ordering
                              Scan$Ordering$Order
                              Result)
           (com.scalar.db.exception.transaction CrudException
                                                UnknownTransactionStatusException)
           (com.scalar.db.io IntValue
                             Key)))

(def ^:private ^:const KEYSPACE "jepsen_keyspace")
(def ^:private ^:const TABLE "transfer")
(def ^:private ^:const ACCOUNT_ID "account_id")
(def ^:private ^:const BALANCE "balance")
(def ^:private ^:const AGE "age")
(def ^:private ^:const INITIAL_BALANCE 10000)
(def ^:private ^:const NUM_ACCOUNTS 10)
(def ^:private ^:const TOTAL_BALANCE (* NUM_ACCOUNTS INITIAL_BALANCE))
(def ^:private ^:const SCHEMA {:account_id             :int
                               :age                    :int
                               :balance                :int
                               :tx_id                  :text
                               :tx_prepared_at         :bigint
                               :tx_committed_at        :bigint
                               :tx_state               :int
                               :tx_version             :int
                               :before_balance         :int
                               :before_tx_committed_at :bigint
                               :before_tx_id           :text
                               :before_tx_prepared_at  :bigint
                               :before_tx_state        :int
                               :before_tx_version      :int
                               :primary-key            [:account_id :age]})

(defn- prepare-scan
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Scan.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withOrdering (Scan$Ordering. AGE Scan$Ordering$Order/DESC))))

(defn- prepare-scan-for-latest
  [id]
  (-> id prepare-scan (.withLimit 1)))

(defn- scan-for-latest
  [tx scan]
  (first (.scan tx scan)))

(defn- prepare-put
  [id age balance]
  (-> (Put. (Key. [(IntValue. ACCOUNT_ID id)]) (Key. [(IntValue. AGE age)]))
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue (IntValue. BALANCE balance))))

(defn- populate-accounts
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

(defn- get-balance
  [^Result result]
  (-> result (.getValue BALANCE) .get .get))

(defn- get-age
  [^Result result]
  (-> result (.getValue AGE) .get .get))

(defn- get-balances
  [results]
  (mapv #(get-balance (first %)) results))

(defn- get-ages
  [results]
  (mapv #(get-age (first %)) results))

(defn- get-nums
  [results]
  (mapv count results))

(defn- calc-new-balance
  [^Result r amount]
  (-> r get-balance (+ amount)))

(defn- calc-new-age
  [^Result r]
  (-> r get-age inc))

(defn- tx-transfer
  [tx1 tx2  {:keys [from to amount]}]
  (try
    (let [^Result from-result (scan-for-latest tx1 (prepare-scan-for-latest from))
          ^Result to-result (scan-for-latest tx2 (prepare-scan-for-latest to))]
      (->> (prepare-put from
                        (calc-new-age from-result)
                        (calc-new-balance from-result (- amount)))
           (.put tx1))
      (->> (prepare-put to
                        (calc-new-age to-result)
                        (calc-new-balance to-result amount))
           (.put tx2)))
    (.prepare tx1)
    (.prepare tx2)
    (.validate tx1)
    (.validate tx2)
    (.commit tx1)
    (.commit tx2)
    (catch UnknownTransactionStatusException e
      (throw e))
    (catch Exception e
      (.rollback tx1)
      (.rollback tx2)
      (throw e))))

(defn- scan-records
  [tx id]
  (try
    (.scan tx (prepare-scan id))
    (catch CrudException _ nil)))

(defn- scan-all-records-with-retry
  [test n]
  (scalar/check-transaction-connection! test)
  (scalar/with-retry scalar/prepare-transaction-service! test
    (let [tx (scalar/start-transaction test)
          results (map #(scan-records tx %) (range n))]
      (if (some nil? results) nil results))))

(defrecord TransferClient [initialized? n initial-balance]
  client/Client
  (open! [_ _ _]
    (TransferClient. initialized? n initial-balance))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (scalar/setup-transaction-tables test [{:keyspace KEYSPACE
                                                :table TABLE
                                                :schema SCHEMA}])
        (scalar/prepare-2pc-service! test)
        (scalar/prepare-transaction-service! test)
        (populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (let [tx1 (scalar/start-2pc test)
                      tx2 (scalar/join-2pc test (.getId tx1))]
                  (try
                    (tx-transfer tx1 tx2 (:value op))
                    (assoc op :type :ok)
                    (catch UnknownTransactionStatusException _
                      (swap! (:unknown-tx test) conj (.getId tx1))
                      (assoc op :type :info, :error {:unknown-tx-status (.getId tx1)}))
                    (catch Exception e
                      (scalar/try-reconnection-for-2pc! test)
                      (assoc op :type :fail, :error (.getMessage e)))))
      :get-all (do
                 (cassandra/wait-rf-nodes test)
                 (if-let [results (scan-all-records-with-retry test (:num op))]
                   (assoc op :type, :ok :value {:balance (get-balances results)
                                                :age (get-ages results)
                                                :num (get-nums results)})
                   (assoc op :type, :fail, :error "Failed to get all records")))
      :check-tx (if-let [num-committed (scalar/check-transaction-states test
                                                                        @(:unknown-tx test))]
                  (assoc op :type :ok, :value num-committed)
                  (assoc op :type :fail, :error "Failed to check status"))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn- transfer
  [test _]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (+ 1 (rand-int 1000))}}))

(def diff-transfer
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

(defn- get-all
  [test _]
  {:type :invoke
   :f    :get-all
   :num  (-> test :client :n)})

(defn- check-tx
  [_ _]
  {:type :invoke
   :f    :check-tx})

(defn- consistency-checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [read-result (->> history
                             (r/filter #(= :get-all (:f %)))
                             (into [])
                             last
                             :value)
            actual-balance (->> (:balance read-result)
                                (reduce +))
            bad-balance (if-not (= actual-balance TOTAL_BALANCE)
                          {:type     :wrong-balance
                           :expected TOTAL_BALANCE
                           :actual   actual-balance})
            actual-age (->> (:age read-result)
                            (reduce +))
            expected-age (->> (:num read-result)
                              (reduce +))
            bad-age (if-not (= actual-age expected-age)
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
  {:client (->TransferClient (atom false) NUM_ACCOUNTS INITIAL_BALANCE)
   :generator [diff-transfer]
   :final-generator (gen/phases
                     (gen/once check-tx)
                     (gen/once get-all))
   :checker (consistency-checker)})
