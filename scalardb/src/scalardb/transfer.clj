(ns scalardb.transfer
  (:require [cassandra.conductors :as conductors]
            [cassandra.core :as cassandra]
            [clojure.core.reducers :as r]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.op :as op]
            [scalardb.core :as scalar])
  (:import (com.scalar.db.api Consistency
                              Get
                              Put
                              Result)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.storage ExecutionException)
           (com.scalar.db.exception.transaction CrudException
                                                UnknownTransactionStatusException)))

(def ^:private ^:const KEYSPACE "jepsen")
(def ^:private ^:const TABLE "transfer")
(def ^:private ^:const ACCOUNT_ID "account_id")
(def ^:private ^:const BALANCE "balance")

(def ^:private ^:const INITIAL_BALANCE 10000)
(def ^:private ^:const NUM_ACCOUNTS 10)
(def ^:private ^:const TOTAL_BALANCE (* NUM_ACCOUNTS INITIAL_BALANCE))

(def ^:private ^:const SCHEMA {:account_id             :int
                               :balance                :int
                               :tx_id                  :text
                               :tx_version             :int
                               :tx_state               :int
                               :tx_prepared_at         :bigint
                               :tx_committed_at        :bigint
                               :before_balance         :int
                               :before_tx_id           :text
                               :before_tx_version      :int
                               :before_tx_state        :int
                               :before_tx_prepared_at  :bigint
                               :before_tx_committed_at :bigint
                               :primary-key            [:account_id]})

(defn- prepare-get
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withConsistency Consistency/LINEARIZABLE)))

(defn- prepare-put
  [id balance]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue (IntValue. BALANCE balance))
      (.withConsistency Consistency/LINEARIZABLE)))

(defn- populate-accounts
  "Insert initial records with transaction.
  This method assumes that n is small (< 100)"
  [test n balance]
  (scalar/retry-when-exception
   (fn [num]
     (let [tx (scalar/start-transaction test)]
       (dotimes [i num]
         (.put tx (prepare-put i balance)))
       (.commit tx)))
   [n]))

(defn- get-balance
  [^Result r]
  (-> r .get (.getValue BALANCE) .get .get))

(defn- get-version
  [^Result r]
  (-> r .get (.getValue scalar/VERSION) .get .get))

(defn- get-balances
  [results]
  (mapv get-balance results))

(defn- get-versions
  [results]
  (mapv get-version results))

(defn- calc-new-balance
  [^Result r ^long amount]
  (-> r get-balance (+ amount)))

(defn- tx-transfer
  [tx {:keys [from to amount]}]
  (let [fromResult (.get tx (prepare-get from))
        toResult (.get tx (prepare-get to))]
    (->> (calc-new-balance fromResult (- amount))
         (prepare-put from)
         (.put tx))
    (->> (calc-new-balance toResult amount)
         (prepare-put to)
         (.put tx))
    (.commit tx)))

(defn- read-record
  "Read a record with a transaction. If read fails, this function returns nil."
  [tx storage i]
  (try
    (.get tx (prepare-get i))
    (.get storage (prepare-get i))
    (catch CrudException _ nil)
    (catch ExecutionException _ nil)))

(defn- read-all-with-retry
  [test n]
  (scalar/check-transaction-connection! test)
  (scalar/check-storage-connection! test)
  (scalar/with-retry (fn [test] (scalar/prepare-transaction-service! test) (scalar/prepare-storage-service! test)) test
    (let [tx (scalar/start-transaction test)
          results (map #(read-record tx @(:storage test) %) (range n))]
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
        (scalar/prepare-transaction-service! test)
        (populate-accounts test n initial-balance))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (if-let [tx (scalar/start-transaction test)]
                  (try
                    (tx-transfer tx (:value op))
                    (assoc op :type :ok)
                    (catch UnknownTransactionStatusException _
                      (swap! (:unknown-tx test) conj (.getId tx))
                      (assoc op :type :fail :error {:unknown-tx-status (.getId tx)}))
                    (catch Exception e
                      (scalar/try-reconnection-for-transaction! test)
                      (assoc op :type :fail :error (.getMessage e))))
                  (do
                    (scalar/try-reconnection-for-transaction! test)
                    (assoc op :type :fail :error "Skipped due to no connection")))
      :get-all (do
                 (cassandra/wait-rf-nodes test)
                 (if-let [results (read-all-with-retry test (:num op))]
                   (assoc op :type :ok :value {:balance (get-balances results)
                                               :version (get-versions results)})
                   (assoc op :type :fail :error "Failed to get balances")))
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
                             (r/filter identity)
                             (into [])
                             last
                             :value)
            actual-balance (->> (:balance read-result)
                                (reduce +))
            bad-balance (if-not (= actual-balance TOTAL_BALANCE)
                          {:type     :wrong-balance
                           :expected TOTAL_BALANCE
                           :actual   actual-balance})
            actual-version (->> (:version read-result)
                                (reduce +))
            checked-committed (->> history
                                   (r/filter #(= :check-tx (:f %)))
                                   (r/filter identity)
                                   (into [])
                                   last
                                   ((fn [x]
                                      (if (= (:type x) :ok) (:value x) 0))))
            total-ok (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :transfer (:f %)))
                          (r/filter identity)
                          (into [])
                          count
                          (+ checked-committed))
            expected-version (-> total-ok
                                 (* 2)                      ; update 2 records per a transfer
                                 (+ (-> test :client :n)))  ; initial insertions
            bad-version (if-not (= actual-version expected-version)
                          {:type     :wrong-version
                           :expected expected-version
                           :actual   actual-version})]
        {:valid?               (and (empty? bad-balance) (empty? bad-version))
         :total-version        actual-version
         :committed-unknown-tx checked-committed
         :bad-balance          bad-balance
         :bad-version          bad-version}))))

(defn transfer-test
  [opts]
  (merge (scalar/scalardb-test (str "transfer-" (:suffix opts))
                               {:client     (TransferClient. (atom false)
                                                             NUM_ACCOUNTS
                                                             INITIAL_BALANCE)
                                :unknown-tx (atom #{})
                                :failures   (atom 0)
                                :generator  (gen/phases
                                             (->> [diff-transfer]
                                                  (conductors/std-gen opts))
                                             (conductors/terminate-nemesis opts)
                                             (gen/clients (gen/once get-all))
                                             (gen/clients (gen/once check-tx)))
                                :checker    (consistency-checker)})
         opts))
