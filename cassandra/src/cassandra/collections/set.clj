(ns cassandra.collections.set
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]
            [qbits.hayt.utils :refer [set-type]]
            [cassandra.core :refer :all])
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

  (invoke! [_ test op]
    (try
      (alia/execute session (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute session
                               (update :sets
                                       (set-columns {:elements [+ #{(:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency writec})
                 (assoc op :type :ok))
        :read (do (wait-rf-nodes test)
                  (let [value (->> (alia/execute session
                                                 (select :sets (where [[= :id 0]]))
                                                 {:consistency :all})
                                   first
                                   :elements
                                   (into (sorted-set)))]
                    (assoc op :type :ok, :value value))))

      (catch ExceptionInfo e
        (handle-exception op e))))

  (close! [_ _]
    (close-cassandra cluster session))

  (teardown! [_ _]))

(defn workload
  [_]
  {:client (->CQLSetClient (atom false) nil nil :quorum)
   :generator [(adds)]
   :final-generator (read-once)
   :checker (checker/set)})
