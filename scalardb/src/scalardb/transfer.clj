(ns scalardb.transfer
  (:require [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.op :as op]
            [scalardb.core :as scalar :refer [KEYSPACE]]
            [scalardb.db-extend :refer [wait-for-recovery]])
  (:import (com.scalar.db.api Consistency
                              Get
                              Put
                              Result)
           (com.scalar.db.io IntValue
                             Key)
           (com.scalar.db.exception.transaction UnknownTransactionStatusException)))

(def ^:private ^:const TABLE "transfer")
(def ^:private ^:const ACCOUNT_ID "account_id")
(def ^:private ^:const BALANCE "balance")

(def ^:const INITIAL_BALANCE 10000)
(def ^:const NUM_ACCOUNTS 10)
(def ^:const MAX_NUM_TXS 8)

(def ^:const SCHEMA {(keyword (str KEYSPACE \. TABLE))
                     {:transaction true
                      :partition-key [ACCOUNT_ID]
                      :clustering-key []
                      :columns {(keyword ACCOUNT_ID) "INT"
                                (keyword BALANCE) "INT"}}})

(defn setup-tables
  [test]
  (scalar/setup-transaction-tables test [SCHEMA]))

(defn prepare-get
  [id]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Get.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withConsistency Consistency/LINEARIZABLE)))

(defn prepare-put
  [id balance]
  (-> (Key. [(IntValue. ACCOUNT_ID id)])
      (Put.)
      (.forNamespace KEYSPACE)
      (.forTable TABLE)
      (.withValue (IntValue. BALANCE balance))
      (.withConsistency Consistency/LINEARIZABLE)))

(defn populate-accounts
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

(defn get-balance
  [^Result r]
  (-> r .get (.getValue BALANCE) .get .get))

(defn get-version
  [^Result r]
  (-> r .get (.getValue scalar/VERSION) .get .get))

(defn get-balances
  [results]
  (mapv get-balance results))

(defn get-versions
  [results]
  (mapv get-version results))

(defn calc-new-balance
  [^Result r ^long amount]
  (-> r get-balance (+ amount)))

(defn- tx-transfer
  [tx from to amount]
  (info "Transferring" amount "from" from "to" to "by tx" (.getId tx))
  (let [fromResult (.get tx (prepare-get from))
        toResult (.get tx (prepare-get to))]
    (->> (calc-new-balance fromResult (- amount))
         (prepare-put from)
         (.put tx))
    (->> (calc-new-balance toResult amount)
         (prepare-put to)
         (.put tx))
    (.commit tx)))

(defn- try-tx-transfer
  [test {:keys [from to amount]}]
  (if-let [tx (try (scalar/start-transaction test)
                   (catch Exception e
                     (warn (.getMessage e))))]
    (try
      (tx-transfer tx from to amount)
      :commit
      (catch UnknownTransactionStatusException _
        (swap! (:unknown-tx test) conj (.getId tx))
        (warn "Unknown transaction: " (.getId tx))
        :unknown-tx-status)
      (catch Exception e
        (warn (.getMessage e))
        :fail))
    :start-fail))

(defn exec-transfers
  "Execute transfers in parallel. Give the transfer function."
  [test op transfer-fn]
  (let [results (doall (pmap #(transfer-fn test %) (:value op)))]
    (if (some #{:commit} results)
      ;; return :ok when at least 1 transaction is committed
      (assoc op :type :ok :value {:results results})
      ;; :info type could be better in some cases
      ;; However, our checker doesn't care about the type for now
      (do
        (scalar/try-reconnection! test scalar/prepare-transaction-service!)
        (assoc op :type :fail :error {:results results})))))

(defn- read-record
  "Read and update the specified record with a transaction"
  [test id]
  (let [tx (scalar/start-transaction test)
        tx-result (.get tx (prepare-get id))
        ;; Need Storage API to read the transaction metadata
        result (.get @(:storage test) (prepare-get id))]
    ;; Put the same balance to check conflicts with in-flight transactions
    (->> (calc-new-balance tx-result 0)
         (prepare-put id)
         (.put tx))
    (.commit tx)
    result))

(defn- read-record-with-retry
  [test id]
  (scalar/with-retry
    (fn [test]
      (scalar/prepare-transaction-service! test)
      (scalar/prepare-storage-service! test))
    test
    (try
      (read-record test id)
      (catch Exception e
        ;; Read failure or the transaction conflicted
        (warn (.getMessage e))
        nil))))

(defn read-all-with-retry
  [test n]
  (scalar/check-transaction-connection! test)
  (scalar/check-storage-connection! test)
  (doall (map #(read-record-with-retry test %) (range n))))

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
      :transfer (exec-transfers test op try-tx-transfer)
      :get-all (do
                 (wait-for-recovery (:db test) test)
                 (if-let [results (read-all-with-retry test n)]
                   (assoc op :type :ok :value {:balance (get-balances results)
                                               :version (get-versions results)})
                   (assoc op :type :fail :error "Failed to get balances")))
      :check-tx (if-let [num-committed
                         (scalar/check-transaction-states test
                                                          @(:unknown-tx test))]
                  (assoc op :type :ok, :value num-committed)
                  (assoc op :type :fail, :error "Failed to check status"))))

  (close! [_ _])

  (teardown! [_ test]
    (scalar/close-all! test)))

(defn- generate-acc-pair
  [n]
  (loop []
    (let [from (rand-int n)
          to (rand-int n)]
      (if-not (= from to) [from to] (recur)))))

(defn transfer
  [test _]
  (let [num-accs (-> test :client :n)
        num-txs (-> test :client :max-txs rand-int inc)]
    {:type  :invoke
     :f     :transfer
     :value (repeatedly num-txs
                        (fn []
                          (let [[from to] (generate-acc-pair num-accs)]
                            {:from from
                             :to   to
                             :amount (+ 1 (rand-int 1000))})))}))

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
                             (r/filter identity)
                             (into [])
                             last
                             :value)
            actual-balance (->> (:balance read-result)
                                (reduce +))
            bad-balance (when-not (= actual-balance total-balance)
                          {:type     :wrong-balance
                           :expected total-balance
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
            total-commits (->> history
                               (r/filter op/ok?)
                               (r/filter #(= :transfer (:f %)))
                               (r/reduce (fn [cnt op]
                                           (->> op :value :results
                                                (filter #{:commit})
                                                count
                                                (+ cnt)))
                                         checked-committed))
            expected-version (-> total-commits
                                 (* 2)          ; update 2 records per transfer
                                 (+ num-accs))  ; initial insertions
            bad-version (when-not (= actual-version expected-version)
                          {:type     :wrong-version
                           :expected expected-version
                           :actual   actual-version})]
        {:valid?               (and (empty? bad-balance) (empty? bad-version))
         :total-version        actual-version
         :committed-unknown-tx checked-committed
         :bad-balance          bad-balance
         :bad-version          bad-version}))))

(defn workload
  [_]
  {:client (->TransferClient (atom false)
                             NUM_ACCOUNTS
                             INITIAL_BALANCE
                             MAX_NUM_TXS)
   :generator [transfer]
   :final-generator (gen/phases
                     (gen/once get-all)
                     (gen/once check-tx))
   :checker (consistency-checker)})
