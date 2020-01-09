(ns cassandra.lwt
  (:require [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(def ak (keyword "[applied]"))  ; this is the name C* returns, define now because
                                ; it isn't really a valid keyword from reader's
                                ; perspective

(defrecord CasRegisterClient [tbl-created? cluster session]
  client/Client
  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CasRegisterClient tbl-created? cluster session)))

  (setup! [_ test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (create-my-keyspace session test {:keyspace "jepsen_keyspace"})
        (create-my-table session {:keyspace "jepsen_keyspace"
                                  :table "lwt"
                                  :schema {:id          :int
                                           :value       :int
                                           :primary-key [:id]}}))))

  (invoke! [_ _ op]
    (try
      (alia/execute session (use-keyspace :jepsen_keyspace))
      (case (:f op)
        :cas (let [[old new] (:value op)
                   result (alia/execute session
                                        (update :lwt
                                                (set-columns {:value new})
                                                (where [[= :id 0]])
                                                (only-if [[:value old]])))]
               (assoc op :type (if (-> result first ak)
                                 :ok
                                 :fail)))

        :write (let [v (:value op)
                     result (alia/execute session (update :lwt
                                                          (set-columns {:value v})
                                                          (only-if [[:in :value (range 5)]])
                                                          (where [[= :id 0]])))]
                 (if (-> result first ak)
                   (assoc op :type :ok)
                   (let [result' (alia/execute session (insert :lwt
                                                               (values [[:id 0]
                                                                        [:value v]])
                                                               (if-exists false)))]
                     (if (-> result' first ak)
                       (assoc op :type :ok)
                       (assoc op :type :fail)))))

        :read (let [v (->> (alia/execute session
                                         (select :lwt (where [[= :id 0]]))
                                         {:consistency :serial})
                           first
                           :value)]
                (assoc op :type :ok, :value v)))

      (catch ExceptionInfo e
        (handle-exception op e true))))

  (close! [_ _]
    (close-cassandra cluster session))

  (teardown! [_ _]))

(defn lwt-test
  [opts]
  (merge (cassandra-test (str "lwt-" (:suffix opts))
                         {:client    (->CasRegisterClient (atom false) nil nil)
                          :checker   (checker/linearizable {:model     (model/cas-register)
                                                            :algorithm :linear})
                          :generator (gen/phases
                                      (->> [r w cas cas cas]
                                           (conductors/std-gen opts)))})
         opts))
