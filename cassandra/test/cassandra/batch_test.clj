(ns cassandra.batch-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cass]
            [cassandra.batch :as batch :refer (->BatchSetClient)]
            [spy.core :as spy])
  (:import (com.datastax.oss.driver.api.core NoNodeAvailableException)
           (com.datastax.oss.driver.api.core.servererrors WriteTimeoutException
                                                          WriteType)))

(deftest batch-client-init-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (spy/called-n-times? alia/execute 3)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 3)))))

(deftest batch-client-add-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :add :value 1})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            (str "BEGIN BATCH "
                                 "INSERT INTO bat (pid, cid, value) VALUES (1,0,1); "
                                 "INSERT INTO bat (pid, cid, value) VALUES (1,1,1); "
                                 "APPLY BATCH;")
                            {:consistency-level :quorum}))
      (is (= :ok (:type result))))))

(deftest batch-client-read-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:pid 0 :cid 0 :value 0}
                                            {:pid 0 :cid 1 :value 0}
                                            {:pid 2 :cid 0 :value 2}
                                            {:pid 2 :cid 1 :value 2}
                                            {:pid 1 :cid 0 :value 1}
                                            {:pid 1 :cid 1 :value 1}])))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :bat
                             :columns :*}
                            {:consistency-level :all}))
      (is (= :ok (:type result)))
      (is (= #{0 1 2} (:value result))))))

(deftest batch-client-read-fail-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:pid 0 :cid 0 :value 0}
                                            {:pid 0 :cid 1 :value 0}
                                            {:pid 2 :cid 0 :value 2}
                                            {:pid 1 :cid 0 :value 1}
                                            {:pid 1 :cid 1 :value 1}])))
                cass/wait-rf-nodes (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= [#{0 1 2} #{0 1}] (:value result))))))

(deftest batch-client-write-timeout-exception-test
  (with-redefs [alia/session (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (and (string? cql) (re-find #"BATCH" cql))
                                  (throw (ex-info "Timed out"
<<<<<<< HEAD
                                                  {}
                                                  (WriteTimeoutException. nil nil 0 0 WriteType/BATCH_LOG))))))]
=======
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil 0 0 WriteType/BATCH_LOG)})))))]
>>>>>>> f02ac27 (cassandra tests)
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          add-result (client/invoke! client {}
                                     {:type :invoke :f :add :value 1})]
      (is (= :info (:type add-result)))
      (is (= :write-timed-out (:error add-result))))))

(deftest batch-client-no-node-available-exception-test
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
    (let [client (client/open! (->BatchSetClient (atom false) nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
<<<<<<< HEAD
      (is (= :no-node-available (:error read-result))))))
=======
      (is (= :no-host-available (:error read-result))))))
>>>>>>> f02ac27 (cassandra tests)
