(ns cassandra.collections.set
  (:require [clojure.pprint :refer :all]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]
            [qbits.hayt.utils :refer [set-type]]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(defrecord CQLSetClient [tbl-created? session writec]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLSetClient tbl-created? session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (create-my-table session {:keyspace "jepsen_keyspace"
                                  :table "sets"
                                  :schema {:id          :int
                                           :elements    (set-type :int)
                                           :primary-key [:id]}})
        (alia/execute session (insert :sets
                                      (values [[:id 0]
                                               [:elements #{}]])
                                      (if-exists false))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (try
      (case (:f op)
        :add (do (alia/execute session
                               (update :sets
                                       (set-columns {:elements [+ #{(:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency writec})
                 (assoc op :type :ok))
        :read (let [value (->> (alia/execute session
                                             (select :sets (where [[= :id 0]]))
                                             {:consistency :all})
                               first
                               :elements
                               (into (sorted-set)))]
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

(defn set-test
  [opts]
  (merge (cassandra-test (str "set-" (:suffix opts))
                         {:client    (->CQLSetClient (atom false) nil :quorum)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (conductors/terminate-nemesis opts)
                                      (read-once))
                          :checker   (checker/set)})
         opts))
