(ns scalardl.transfer
  (:require  [cassandra.conductors :as conductors]
             [clojure.tools.logging :refer [debug info warn]]
             [clojure.core.reducers :as r]
             [jepsen
              [client :as client]
              [checker :as checker]
              [generator :as gen]]
             [knossos.op :as op]
             [scalardl
              [core :as dl]
              [cassandra :as cassandra]
              [util :as util]])
  (:import (com.scalar.dl.client.exception ClientException)
           (javax.json Json)))

(def ^:private ^:const INITIAL_BALANCE 10000)
(def ^:private ^:const NUM_ACCOUNTS 10)
(def ^:private ^:const TOTAL_BALANCE (* NUM_ACCOUNTS INITIAL_BALANCE))

(def ^:private ^:const CONTRACTS [{:name "create"
                                   :class "com.scalar.jepsen.scalardl.Create"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Create.class"}
                                  {:name "balance"
                                   :class "com.scalar.jepsen.scalardl.Balance"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Balance.class"}
                                  {:name "transfer"
                                   :class "com.scalar.jepsen.scalardl.Transfer"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Transfer.class"}])

(def ^:private ^:const ASSET_ID "id")
(def ^:private ^:const ASSET_ID_FROM "from")
(def ^:private ^:const ASSET_ID_TO "to")
(def ^:private ^:const ASSET_AMOUNT "amount")
(def ^:private ^:const ASSET_BALANCE "balance")
(def ^:private ^:const ASSET_AGE "age")
(def ^:private ^:const NONCE "nonce")

(defn- create-argument
  ([id]
   (-> (Json/createObjectBuilder)
       (.add ASSET_ID id)
       (.build)))
  ([id initial-balance]
   (-> (Json/createObjectBuilder)
       (.add ASSET_ID id)
       (.add ASSET_BALANCE initial-balance)
       (.build)))
  ([txid from to amount]
   (-> (Json/createObjectBuilder)
       (.add NONCE txid)
       (.add ASSET_ID_FROM from)
       (.add ASSET_ID_TO to)
       (.add ASSET_AMOUNT amount)
       (.build))))

(defn- create-asset
  [client-service id]
  (dl/retry-when-exception (fn [i]
                             (->> (create-argument i INITIAL_BALANCE)
                                  (.executeContract client-service "create")))
                           [id]))

(defn- get-balance
  [client-service id]
  (try
    (let [result (->> (create-argument id)
                      (.executeContract client-service "balance"))
          balance (-> (util/result->json result) (.getInt ASSET_BALANCE))
          age (-> (util/result->json result) (.getInt ASSET_AGE))]
      {:balance balance :age age})
    (catch ClientException e
      (warn "Failed to read a balance from id =" id))))

(defn- read-with-retry
  [client-service n]
  (dl/retry-when-exception (fn [n]
                             (let [balances (mapv (partial get-balance
                                                           client-service)
                                                  (range 0 n))]
                               (if (some nil? balances)
                                 (throw (ex-info
                                         "Failed to read at least 1 record"
                                         {:cause :read-record-failure}))
                                 balances)))
                           [n]))

(defn- check-tx-states
  [test]
  (->> @(:unknown-tx test)
       (mapv #(dl/committed? % test))
       (filter true?)
       count))

(defrecord TransferClient [initialized? client-service n]
  client/Client
  (open! [_ test _]
    (->TransferClient initialized? (atom (dl/prepare-client-service test)) n))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (cassandra/create-tables test)
        (Thread/sleep 10000)  ;; Wait for the table creation
        (info "register a certificate and contracts")
        (dl/register-certificate @client-service)
        (dl/register-contracts @client-service CONTRACTS)
        (doseq [id (range 0 n)]
          (create-asset @client-service id)))))

  (invoke! [_ test op]
    (case (:f op)
      :transfer (let [txid (str (java.util.UUID/randomUUID))
                      {:keys [from to amount]} (:value op)
                      arg (create-argument txid from to amount)]
                  (try
                    (.executeContract @client-service "transfer" arg)
                    (assoc op :type :ok)
                    (catch ClientException e
                      (reset! client-service
                              (dl/try-switch-server! @client-service test))
                      (when (util/unknown? e)
                        (warn "The state of transaction is unknown:" txid)
                        (swap! (:unknown-tx test) conj txid))
                      (assoc op :type :fail
                             :error (util/get-exception-info e)))))

      :check-tx (let [num-committed (check-tx-states test)]
                  (if num-committed
                    (assoc op :type :ok :value num-committed)
                    (assoc op :type :fail :error "Failed to check status")))

      :get-all (let [balances (read-with-retry @client-service n)]
                 (if balances
                   (assoc op :type :ok :value balances)
                   (assoc op :type :fail :error "Failed to get balances")))))

  (close! [_ _]
    (.close @client-service))

  (teardown! [_ _]))

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
  [_ _]
  {:type :invoke
   :f    :get-all})

(defn- check-tx
  [_ _]
  {:type :invoke
   :f    :check-tx})

(defn- asset-checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [read-results (->> history
                              (r/filter #(= :get-all (:f %)))
                              (r/filter identity)
                              (into []) last :value)
            actual-balance (reduce #(+ %1 (:balance %2)) 0 read-results)
            actual-age (reduce #(+ %1 (:age %2)) 0 read-results)
            checked-committed (->> history
                                   (r/filter #(= :check-tx (:f %)))
                                   (r/filter identity)
                                   (into [])
                                   last
                                   ((fn [x]
                                      (if (= (:type x) :ok) (:value x) 0))))
            expected-age (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :transfer (:f %)))
                              (r/filter identity)
                              (into [])
                              count
                              (+ checked-committed)
                              (* 2))  ; update 2 records per transfer
            bad-balance (when-not (= actual-balance TOTAL_BALANCE)
                          {:type     :wrong-balance
                           :expected TOTAL_BALANCE
                           :actual   actual-balance})
            bad-age (when-not (= actual-age expected-age)
                      {:type :wrong-age
                       :expected expected-age
                       :actual actual-age})]
        {:valid? (and (empty? bad-balance) (empty? bad-age))
         :total-age actual-age
         :committed-unknown-tx checked-committed
         :bad-balance bad-balance
         :bad-age bad-age}))))

(defn transfer-test
  [opts]
  (merge (dl/scalardl-test (str "transfer-" (:suffix opts))
                           {:client     (->TransferClient (atom false)
                                                          (atom nil)
                                                          NUM_ACCOUNTS)
                            :failures   (atom 0)
                            :unknown-tx (atom #{})
                            :generator  (gen/phases
                                         (conductors/std-gen opts [diff-transfer])
                                         (conductors/terminate-nemesis opts)
                                         (gen/clients (gen/once get-all))
                                         (gen/clients (gen/once check-tx)))
                            :checker   (asset-checker)})
         opts))
