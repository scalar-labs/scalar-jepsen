(ns cassandra.collections.set
  (:require [jepsen
             [client :as client]
             [checker :as checker]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :as clause]
            [qbits.hayt.dsl.statement :as st]
            [qbits.hayt.utils :refer [set-type]]
            [cassandra.core :as cass])
  (:import (clojure.lang ExceptionInfo)))

(defrecord CQLSetClient [tbl-created? cluster session writec]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLSetClient tbl-created? cluster session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (cass/create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (cass/create-my-table session {:keyspace "jepsen_keyspace"
                                       :table "sets"
                                       :schema {:id          :int
                                                :elements    (set-type :int)
                                                :primary-key [:id]}})
        (alia/execute session (st/insert :sets
                                         (clause/values [[:id 0]
                                                         [:elements #{}]])
                                         (clause/if-exists false))))))

  (invoke! [_ test op]
    (try
      (alia/execute session (st/use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute session
                               (st/update :sets
                                          (clause/set-columns
                                           {:elements [+ #{(:value op)}]})
                                          (clause/where [[= :id 0]]))
                               {:consistency writec})
                 (assoc op :type :ok))
        :read (do (cass/wait-rf-nodes test)
                  (let [value (->> (alia/execute session
                                                 (st/select
                                                  :sets
                                                  (clause/where [[= :id 0]]))
                                                 {:consistency :all})
                                   first
                                   :elements
                                   (into (sorted-set)))]
                    (assoc op :type :ok, :value value))))

      (catch ExceptionInfo e
        (cass/handle-exception op e))))

  (close! [_ _]
    (cass/close-cassandra cluster session))

  (teardown! [_ _]))

(defn workload
  [_]
  {:client (->CQLSetClient (atom false) nil nil :quorum)
   :generator [(cass/adds)]
   :final-generator (cass/read-once)
   :checker (checker/set)})
