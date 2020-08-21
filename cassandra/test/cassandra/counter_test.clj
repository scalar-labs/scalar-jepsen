(ns cassandra.counter-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [cassandra.core :as cassandra]
            [cassandra.counter :refer [->CQLCounterClient] :as counter]
            [spy.core :as spy])
  (:import (com.datastax.driver.core WriteType)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(deftest counter-client-init-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (= :quorum (.writec client)))
      (is (spy/called-n-times? alia/execute 4)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 4)))))

(deftest counter-client-add-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
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
                            {:consistency :quorum}))
      (is (= :ok (:type result))))))

(deftest counter-client-read-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :count 123}])))]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
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
                            {:consistency :all
                             :retry-policy (retry/fallthrough-retry-policy)}))
      (is (= :ok (:type result)))
      (is (= 123 (:value result))))))

(deftest counter-client-read-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Timed out"
                                                   {:type ::execute
                                                    :exception (ReadTimeoutException. nil nil 0 0 false)})))))]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= :read-timed-out (:error result))))))

(deftest counter-client-write-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil WriteType/COUNTER 0 0)})))))]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {}
                                 {:type :invoke :f :add :value 1})]
      (is (= :info (:type result)))
      (is (= :write-timed-out (:error result))))))

(deftest counter-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (UnavailableException. nil nil 0 0)})))))]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= :unavailable (:error result))))))

(deftest counter-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (NoHostAvailableException. {})})))))]
    (let [client (client/open! (->CQLCounterClient (atom false) nil nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= :no-host-available (:error result))))))
