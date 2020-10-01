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
  (:import (clojure.lang ExceptionInfo)))

(defrecord BatchSetClient [tbl-created? cluster session]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (map name (:nodes test))})
          session (alia/connect cluster)]
      (->BatchSetClient tbl-created? cluster session)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (create-my-table session {:keyspace "jepsen_keyspace"
                                  :table "bat"
                                  :schema {:pid         :int
                                           :cid         :int
                                           :value       :int
                                           :primary-key [:pid :cid]}}))))

  (invoke! [_ _ op]
    (try
      (alia/execute session (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (let [value (:value op)]
               (alia/execute session
                             (str "BEGIN BATCH "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ",0," value "); "
                                  "INSERT INTO bat (pid, cid, value) VALUES ("
                                  value ",1," value "); "
                                  "APPLY BATCH;")
                             {:consistency :quorum})
               (assoc op :type :ok))
        :read (let [results (alia/execute session
                                          (select :bat)
                                          {:consistency :all})
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
        (handle-exception op e))))

  (close! [_ _]
    (close-cassandra cluster session))

  (teardown! [_ _]))

(defn batch-test
  [opts]
  (merge (cassandra-test (str "batch-set-" (:suffix opts))
                         {:client    (->BatchSetClient (atom false) nil nil)
                          :checker   (checker/set)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (conductors/terminate-nemesis opts)
                                      ; read after waiting for batchlog replay
                                      (gen/sleep 60)
                                      (read-once))})
         opts))
