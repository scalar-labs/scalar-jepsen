(ns cassandra.lwt-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [qbits.alia :as alia]
            [cassandra.lwt :refer [->CasRegisterClient] :as lwt]
            [spy.core :as spy])
  (:import (com.datastax.driver.core WriteType)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(deftest lwt-client-init-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (spy/called-n-times? alia/execute 3)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 3)))))

(deftest lwt-client-cas-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :update)
                                           [{lwt/ak true}])))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :cas
                                  :value (independent/tuple 0 [1 2])})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :lwt
                             :set-columns {:value 2}
                             :where [[= :id 0]]
                             :if [[:value 1]]}))
      (is (= :ok (:type result))))))

(deftest lwt-client-write-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :update)
                                           [{lwt/ak true}])))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :write
                                  :value (independent/tuple 0 3)})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :lwt
                             :set-columns {:value 3}
                             :where [[= :id 0]]
                             :if [[:in :value (range 5)]]}))
      (is (= :ok (:type result))))))

(deftest lwt-client-insert-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (if (contains? cql :update)
                                           [{lwt/ak false}]
                                           (when (contains? cql :insert)
                                             [{lwt/ak true}]))))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :write
                                  :value (independent/tuple 0 4)})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :lwt
                             :set-columns {:value 4}
                             :where [[= :id 0]]
                             :if [[:in :value (range 5)]]}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:insert :lwt
                             :values [[:id 0] [:value 4]]
                             :if-exists false}))
      (is (= :ok (:type result))))))

(deftest lwt-client-read-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :value 4}])))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :read
                                  :value (independent/tuple 0 nil)})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :lwt
                             :columns :*
                             :where [[= :id 0]]}
                            {:consistency :serial}))
      (is (= :ok (:type result)))
      (is (= [0 4] (:value result))))))

(deftest lwt-client-read-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Timed out"
                                                   {:type ::execute
                                                    :exception (ReadTimeoutException. nil nil 0 0 false)})))))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :read
                                  :value (independent/tuple 0 nil)})]
      (is (= :fail (:type result)))
      (is (= :read-timed-out (:error result))))))

(deftest lwt-client-write-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil WriteType/CAS 0 0)})))))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :write
                                  :value (independent/tuple 0 1)})]
      (is (= :info (:type result)))
      (is (= :write-timed-out (:error result))))))

(deftest lwt-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (UnavailableException. nil nil 0 0)})))))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :read
                                  :value (independent/tuple 0 nil)})]
      (is (= :fail (:type result)))
      (is (= :unavailable (:error result))))))

(deftest lwt-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (NoHostAvailableException. {})})))))]
    (let [client (client/open! (->CasRegisterClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :read
                                  :value (independent/tuple 0 nil)})]
      (is (= :fail (:type result)))
      (is (= :no-host-available (:error result))))))
