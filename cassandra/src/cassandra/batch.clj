(ns cassandra.batch
  (:require [cassandra.core :as cass]
            [jepsen
             [client :as client]
             [checker :as checker]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.statement :as st])
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
        (cass/create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (cass/create-my-table session {:keyspace "jepsen_keyspace"
                                       :table "bat"
                                       :schema {:pid         :int
                                                :cid         :int
                                                :value       :int
                                                :primary-key [:pid :cid]}}))))

  (invoke! [_ test op]
    (try
      (alia/execute session (st/use-keyspace :jepsen_keyspace))
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
        :read (do (cass/wait-rf-nodes test)
                  (let [results (alia/execute session
                                              (st/select :bat)
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
                      (assoc op :type :fail :value [value-a value-b])))))

      (catch ExceptionInfo e
        (cass/handle-exception op e))))

  (close! [_ _]
    (cass/close-cassandra cluster session))

  (teardown! [_ _]))

(defn workload
  [_]
  {:client (->BatchSetClient (atom false) nil nil)
   :generator [(cass/adds)]
   :final-generator (cass/read-once)
   :checker (checker/set)})
