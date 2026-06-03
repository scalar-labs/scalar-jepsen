(ns cassandra.set-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cass]
            [cassandra.collections.set :refer [->CQLSetClient] :as set]
            [spy.core :as spy])
  (:import (com.datastax.oss.driver.api.core NoNodeAvailableException)
           (com.datastax.oss.driver.api.core.servererrors WriteTimeoutException
                                                          WriteType)))

(deftest set-client-init-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLSetClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (= :quorum (.writec client)))
      (is (spy/called-n-times? alia/execute 4)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 4)))))

(deftest set-client-add-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->CQLSetClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :add :value 1})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:update :sets
                             :set-columns {:elements [+ #{1}]}
                             :where [[= :id 0]]}
                            {:consistency-level :quorum}))
      (is (= :ok (:type result))))))

(deftest set-client-read-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :elements #{1 3 2}}])))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLSetClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :sets
                             :columns :*
                             :where [[= :id 0]]}
                            {:consistency-level :all}))
      (is (= :ok (:type result)))
      (is (= #{1 2 3} (:value result))))))

(deftest set-client-write-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
                                                  {}
                                                  (WriteTimeoutException. nil nil 0 0 WriteType/UNLOGGED_BATCH))))))]
    (let [client (client/open! (->CQLSetClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          add-result (client/invoke! client {}
                                     {:type :invoke :f :add :value 1})]
      (is (= :fail (:type add-result)))
      (is (= :write-timed-out (:error add-result))))))

(deftest set-client-no-node-available-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {}
                                                   (NoNodeAvailableException.))))))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->CQLSetClient (atom false) nil :quorum)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :no-node-available (:error read-result))))))
