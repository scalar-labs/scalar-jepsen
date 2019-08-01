(ns cassandra.counter
  (:require [cassandra.conductors :as conductors]
            [cassandra.core :refer :all]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]
            [qbits.alia.policy.retry :as retry])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(defrecord CQLCounterClient [tbl-created? session writec]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLCounterClient tbl-created? session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (create-my-table session test {:keyspace "jepsen_keyspace"
                                       :table "counters"
                                       :schema {:id          :int
                                                :count       :counter
                                                :primary-key [:id]}})
        (alia/execute session (update :counters
                                      (set-columns :count [+ 0])
                                      (where [[= :id 0]]))))))

  (invoke! [_ _ op]
    (try
      (alia/execute session (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute session
                               (update :counters
                                       (set-columns {:count [+ (:value op)]})
                                       (where [[= :id 0]]))
                               {:consistency  writec
                                :retry-policy (retry/fallthrough-retry-policy)})
                 (assoc op :type :ok))
        :read (let [value (->> (alia/execute session
                                             (select :counters (where [[= :id 0]]))
                                             {:consistency  :all
                                              :retry-policy (retry/fallthrough-retry-policy)})
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
    (alia/shutdown session))

  (teardown! [_ _]))

(defn cnt-inc-test
  [opts]
  (merge (cassandra-test (str "counter-inc-" (:suffix opts))
                         {:client    (->CQLCounterClient (atom false) nil :quorum)
                          :checker   (checker/counter)
                          :generator (gen/phases
                                      (->> [add]
                                           (conductors/std-gen opts))
                                      (conductors/terminate-nemesis opts)
                                      (read-once))})
         opts))
