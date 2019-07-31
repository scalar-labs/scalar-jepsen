(ns cassandra.batch
  (:require [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(defrecord BatchSetClient [tbl-created? conn]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          conn (alia/connect cluster)]
      (->BatchSetClient tbl-created? conn)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace conn test {:keyspace "jepsen_keyspace"})
        (create-my-table conn test {:keyspace "jepsen_keyspace"
                                    :table "bat"
                                    :schema {:pid         :int
                                             :cid         :int
                                             :value       :int
                                             :primary-key [:pid :cid]}}))))

  (invoke! [this test op]
    (alia/execute conn (use-keyspace :jepsen_keyspace))
    (try
      (case (:f op)
        :add (let [value (:value op)]
               (alia/execute conn
                             (str "BEGIN BATCH "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ",0," value "); "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ",1," value "); "
                                  "APPLY BATCH;")
                             {:consistency :quorum})
               (assoc op :type :ok))
        :read (let [results (alia/execute conn
                                          (select :bat)
                                          {:consistency :all
                                           :retry-policy      aggressive-read})
                    value-a (->> results
                                 (filter (fn [ret] (= (:cid ret) 0)))
                                 (map :value)
                                 (into (sorted-set)))
                    value-b (->> results
                                 (filter (fn [ret] (= (:cid ret) 1)))
                                 (map :value)
                                 (into (sorted-set)))]
                (if (= value-a value-b)
                  (assoc op :type :ok :value value-a)
                  (assoc op :type :fail :value [value-a value-b]))))

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
    (info "Closing client conn" conn)
    (alia/shutdown conn))

  (teardown! [_ _]))

(defn batch-test
  [opts]
  (merge (cassandra-test (str "batch-set-" (:suffix opts))
                         {:client    (->BatchSetClient (atom false) nil)
                          :checker   (checker/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (conductors/terminate-nemesis opts)
                                      (read-once))})
         opts))
