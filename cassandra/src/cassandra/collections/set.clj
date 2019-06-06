(ns cassandra.collections.set
  (:require [clojure.pprint :refer :all]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt]
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

(defrecord CQLSetClient [tbl-created? conn writec]
  client/Client
  (open! [this test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (CQLSetClient. tbl-created? conn writec)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (alia/execute conn (create-keyspace :jepsen_keyspace
                                            (if-exists false)
                                            (with {:replication {"class"              "SimpleStrategy"
                                                                 "replication_factor" (:rf test)}})))
        (alia/execute conn (use-keyspace :jepsen_keyspace))
        (alia/execute conn (create-table :sets
                                         (if-exists false)
                                         (column-definitions {:id          :int
                                                              :elements    (set-type :int)
                                                              :primary-key [:id]})))
        (alia/execute conn (alter-table :sets (with {:compaction {:class :SizeTieredCompactionStrategy}})))
        (alia/execute conn (insert :sets
                                   (values [[:id 0]
                                            [:elements #{}]])
                                   (if-exists false))))))

  (invoke! [this test op]
    (alia/execute conn (use-keyspace :jepsen_keyspace))
    (try
      (case (:f op)
        :add (do (alia/execute conn
                               (update :sets
                                       (set-columns {:elements [+ #{(:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency-level writec})
                 (assoc op :type :ok))
        :read (do (wait-for-recovery 30 conn)
                  (let [value (->> (alia/execute conn
                                                 (select :sets (where [[= :id 0]]))
                                                 {:consistency  :all
                                                  :retry-policy aggressive-read})
                                   first
                                   :elements
                                   (into (sorted-set)))]
                    (assoc op :type :ok, :value value))))

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

(defn cql-set-client
  "A set implemented using CQL sets"
  ([] (CQLSetClient. (atom false) nil :one))
  ([writec] (CQLSetClient. (atom false) nil writec)))

(defn set-test
  [opts]
  (merge (cassandra-test (str "set-" (:suffix opts))
                         {:client    (cql-set-client)
                          :model     (model/set)
                          :generator (gen/phases
                                       (->> [(adds)]
                                            (conductors/std-gen opts))
                                       (read-once))
                          :checker   (checker/set)})
         opts))
