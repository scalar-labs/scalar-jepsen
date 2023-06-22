(ns cassandra.counter
  (:require [cassandra.core :as cass]
            [jepsen
             [client :as client]
             [checker :as checker]]
            [qbits.alia :as alia]
            [qbits.hayt]
            [qbits.hayt.dsl.clause :as clause]
            [qbits.hayt.dsl.statement :as st]
            [qbits.alia.policy.retry :as retry])
  (:import (clojure.lang ExceptionInfo)))

(def add {:type :invoke, :f :add, :value 1})

(defrecord CQLCounterClient [tbl-created? cluster session writec]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLCounterClient tbl-created? cluster session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (cass/create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (cass/create-my-table session {:keyspace "jepsen_keyspace"
                                       :table "counters"
                                       :schema {:id          :int
                                                :count       :counter
                                                :primary-key [:id]}})
        (alia/execute session (st/update :counters
                                         (clause/set-columns :count [+ 0])
                                         (clause/where [[= :id 0]]))))))

  (invoke! [_ _ op]
    (try
      (alia/execute session (st/use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute session
                               (st/update :counters
                                          (clause/set-columns
                                           {:count [+ (:value op)]})
                                          (clause/where [[= :id 0]]))
                               {:consistency  writec})
                 (assoc op :type :ok))
        :read (do (cass/wait-rf-nodes test)
                  (let [value (->> (alia/execute
                                    session
                                    (st/select :counters
                                               (clause/where [[= :id 0]]))
                                    {:consistency  :all
                                     :retry-policy (retry/fallthrough-retry-policy)})
                                   first
                                   :count)]
                    (assoc op :type :ok, :value value))))

      (catch ExceptionInfo e
        (cass/handle-exception op e))))

  (close! [_ _]
    (cass/close-cassandra cluster session))

  (teardown! [_ _]))

(defn workload
  [_]
  {:client (->CQLCounterClient (atom false) nil nil :quorum)
   :generator [add]
   :final-generator (cass/read-once)
   :checker (checker/counter)})
