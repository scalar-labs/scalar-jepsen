(ns cassandra.counter-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cass]
            [cassandra.counter :refer [->CQLCounterClient] :as counter]
            [spy.core :as spy])
  (:import (com.datastax.oss.driver.api.core NoNodeAvailableException)
           (com.datastax.oss.driver.api.core.servererrors ReadTimeoutException
                                                          WriteTimeoutException
                                                          WriteType)))

(deftest counter-client-init-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (= :quorum (.writec client)))
      (is (spy/called-n-times? alia/execute 4)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 4)))))

(deftest counter-client-add-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :add :value 1})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :counters
                             :set-columns {:count [+ 1]}
                             :where [[= :id 0]]}
                            {:consistency-level :quorum}))
      (is (= :ok (:type result))))))

(deftest counter-client-read-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :count 123}])))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :counters
                             :columns :*
                             :where [[= :id 0]]}
                            {:consistency-level :all}))
      (is (= :ok (:type result)))
      (is (= 123 (:value result))))))

(deftest counter-client-read-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Timed out"
                                                   {}
                                                   (ReadTimeoutException. nil nil 0 0 false))))))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= :read-timed-out (:error result))))))

(deftest counter-client-write-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
<<<<<<< HEAD
                                                  {}
                                                  (WriteTimeoutException. nil nil 0 0 WriteType/COUNTER))))))]
=======
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil 0 0 WriteType/COUNTER)})))))]
>>>>>>> f02ac27 (cassandra tests)
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :add :value 1})]
      (is (= :info (:type result)))
      (is (= :write-timed-out (:error result))))))

<<<<<<< HEAD
(deftest counter-client-no-node-available-exception-test
=======
(deftest counter-client-no-host-available-exception-test
>>>>>>> f02ac27 (cassandra tests)
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
<<<<<<< HEAD
                                                   {}
                                                   (NoNodeAvailableException.))))))
=======
                                                   {:type ::execute
                                                    :exception (NoNodeAvailableException.)})))))
>>>>>>> f02ac27 (cassandra tests)
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
<<<<<<< HEAD
      (is (= :no-node-available (:error result))))))
=======
      (is (= :no-host-available (:error result))))))
>>>>>>> f02ac27 (cassandra tests)
