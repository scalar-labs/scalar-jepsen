(ns cassandra.counter
  (:require [cassandra.conductors :as conductors]
            [cassandra.core :refer :all]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.policies FallthroughRetryPolicy)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(defrecord CQLCounterClient [tbl-created? conn writec]
  client/Client
  (open! [this test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (CQLCounterClient. tbl-created? conn writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace conn test {:keyspace "jepsen_keyspace"})
        (create-my-table conn test {:keyspace "jepsen_keyspace"
                                    :table "counters"
                                    :schema {:id          :int
                                             :count       :counter
                                             :primary-key [:id]}})
        (alia/execute conn (update :counters
                                   (set-columns :count [+ 0])
                                   (where [[= :id 0]]))))))

  (invoke! [this test op]
    (try
      (alia/execute conn (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute conn
                               (update :counters
                                       (set-columns :count [+ (:value op)])
                                       (where [[= :id 0]]))
                               {:consistency  writec
                                :retry-policy FallthroughRetryPolicy/INSTANCE})
                 (assoc op :type :ok))
        :read (let [value (->> (alia/execute conn
                                             (select :counters (where [[= :id 0]]))
                                             {:consistency  :all
                                              :retry-policy FallthroughRetryPolicy/INSTANCE})
                               first
                               :count)]
                (assoc op :type :ok, :value value)))

      (catch ExceptionInfo e
        (let [e (class (:exception (ex-data e)))]
          (condp = e
            WriteTimeoutException (assoc op :type :info, :value :write-timed-out)
            ReadTimeoutException (assoc op :type :fail, :error :read-timed-out)
            UnavailableException (assoc op :type :fail, :error :unavailable)
            NoHostAvailableException (do
                                       (info "All the servers are down - waiting 2s")
                                       (Thread/sleep 2000)
                                       (assoc op :type :fail, :error :no-host-available)))))))

  (close! [_ _]
    (info "Closing client with conn" conn)
    (alia/shutdown conn))

  (teardown! [_ _]))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (CQLCounterClient. (atom false) nil :one))
  ([writec] (CQLCounterClient. (atom false) nil writec)))

(defn cnt-inc-test
  [opts]
  (merge (cassandra-test (str "counter-inc-" (:suffix opts))
                         {:client    (cql-counter-client)
                          :checker   (checker/counter)
                          :generator (->> (repeat 100 add)
                                          (cons r)
                                          (conductors/std-gen opts))})
         opts))
