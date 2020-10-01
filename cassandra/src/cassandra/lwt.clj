(ns cassandra.lwt
  (:require [cassandra.core :refer :all]
            [cassandra.conductors :as conductors]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)))

(def ak (keyword "[applied]"))  ; this is the name C* returns, define now because
                                ; it isn't really a valid keyword from reader's
                                ; perspective

(defn r [_ _] {:type :invoke :f :read :value nil})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

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
        :cas (let [id (key (:value op))
                   [old new] (val (:value op))
                   result (alia/execute session
                                        (update :lwt
                                                (set-columns {:value new})
                                                (where [[= :id id]])
                                                (only-if [[:value old]])))]
               (assoc op :type (if (-> result first ak) :ok :fail)))

        :write (let [id (key (:value op))
                     v (val (:value op))
                     result (alia/execute session (update :lwt
                                                          (set-columns {:value v})
                                                          (only-if [[:in :value (range 5)]])
                                                          (where [[= :id id]])))]
                 (if (-> result first ak)
                   (assoc op :type :ok)
                   (let [result' (alia/execute session (insert :lwt
                                                               (values [[:id id]
                                                                        [:value v]])
                                                               (if-exists false)))]
                     (if (-> result' first ak)
                       (assoc op :type :ok)
                       (assoc op :type :fail)))))

        :read (let [id (key (:value op))
                    v (->> (alia/execute session
                                         (select :lwt (where [[= :id id]]))
                                         {:consistency :serial})
                           first
                           :value)]
                (assoc op :type :ok
                       :value (independent/tuple id v))))

      (catch ExceptionInfo e
        (handle-exception op e true))))

  (close! [_ _]
    (close-cassandra cluster session))

  (teardown! [_ _]))

(defn lwt-test
  [opts]
  (merge (cassandra-test (str "lwt-" (:suffix opts))
                         {:client    (->CasRegisterClient (atom false) nil nil)
                          :checker   (independent/checker
                                      (checker/compose
                                       {:timeline (timeline/html)
                                        :linear (checker/linearizable
                                                 {:model (model/cas-register)})}))
                          :generator (->> (independent/concurrent-generator
                                           (:concurrency opts)
                                           (range)
                                           (fn [_]
                                             (->> (gen/reserve
                                                    (quot (:concurrency opts) 2)
                                                    r
                                                    (gen/mix [w cas cas]))
                                                  (gen/limit 100)
                                                  (gen/process-limit (:concurrency opts)))))
                                          (gen/nemesis
                                           (conductors/mix-failure-seq opts))
                                          (gen/time-limit (:time-limit opts)))})
         opts))
