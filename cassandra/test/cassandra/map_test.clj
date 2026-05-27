(ns cassandra.map-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cass]
            [cassandra.collections.map :refer [->CQLMapClient] :as map]
            [spy.core :as spy])
  (:import (com.datastax.oss.driver.api.core NoNodeAvailableException)
           (com.datastax.oss.driver.api.core.servererrors ReadTimeoutException
                                                          WriteTimeoutException
                                                          WriteType)))

(deftest map-client-init-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (= :quorum (.writec client)))
      (is (spy/called-n-times? alia/execute 4)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 4)))))

(deftest map-client-add-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :add :value 1})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :maps
                             :set-columns {:elements [+ {1 1}]}
                             :where [[= :id 0]]}
                            {:consistency-level :quorum}))
      (is (= :ok (:type result))))))

(deftest map-client-read-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :elements {1 1 3 3 2 2}}])))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :maps
                             :columns :*
                             :where [[= :id 0]]}
                            {:consistency-level :all}))
      (is (= :ok (:type result)))
      (is (= #{1 2 3} (:value result))))))

(deftest map-client-read-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Timed out"
                                                   {}
                                                   (ReadTimeoutException. nil nil 0 0 false))))))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :read-timed-out (:error read-result))))))

(deftest map-client-write-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
<<<<<<< HEAD
                                                  {}
                                                  (WriteTimeoutException. nil nil 0 0 WriteType/UNLOGGED_BATCH))))))]
=======
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil 0 0 WriteType/UNLOGGED_BATCH)})))))]
>>>>>>> f02ac27 (cassandra tests)
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          add-result (client/invoke! client {}
                                     {:type :invoke :f :add :value 1})]
      (is (= :fail (:type add-result)))
      (is (= :write-timed-out (:error add-result))))))

(deftest map-client-no-node-available-exception-test
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
    (let [client (client/open! (->CQLMapClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
<<<<<<< HEAD
      (is (= :no-node-available (:error read-result))))))
=======
      (is (= :no-host-available (:error read-result))))))
>>>>>>> f02ac27 (cassandra tests)
