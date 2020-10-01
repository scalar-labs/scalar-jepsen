(ns cassandra.collections.map
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all]
            [qbits.hayt.utils :refer [map-type]]
            [cassandra.core :refer :all]
            [cassandra.conductors :as conductors])
  (:import (clojure.lang ExceptionInfo)))

(defrecord CQLMapClient [tbl-created? cluster session writec]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLMapClient tbl-created? cluster session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (create-my-table session {:keyspace "jepsen_keyspace"
                                  :table "maps"
                                  :schema {:id          :int
                                           :elements    (map-type :int :int)
                                           :primary-key [:id]}})
        (alia/execute session (insert :maps (values [[:id 0]
                                                     [:elements {}]]))))))

  (invoke! [_ _ op]
    (try
      (alia/execute session (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :add (do (alia/execute session
                               (update :maps
                                       (set-columns {:elements [+ {(:value op) (:value op)}]})
                                       (where [[= :id 0]]))
                               {:consistency writec})
                 (assoc op :type :ok))
        :read (let [value (->> (alia/execute session
                                             (select :maps (where [[= :id 0]]))
                                             {:consistency :all})
                               first
                               :elements
                               vals
                               (into (sorted-set)))]
                (assoc op :type :ok, :value value)))

      (catch ExceptionInfo e
        (handle-exception op e))))

  (close! [_ _]
    (close-cassandra cluster session))

  (teardown! [_ _]))

(defn map-test
  [opts]
  (merge (cassandra-test (str "map-" (:suffix opts))
                         {:client    (->CQLMapClient (atom false)
                                                     nil nil :quorum)
                          :generator (gen/phases
                                      (->> [(adds)]
                                           (conductors/std-gen opts))
                                      (conductors/terminate-nemesis opts)
                                      (gen/sleep 60)
                                      (read-once))
                          :checker   (checker/set)})
         opts))
