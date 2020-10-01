(ns cassandra.batch-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cassandra]
            [cassandra.batch :as batch :refer (->BatchSetClient)]
            [spy.core :as spy])
  (:import (com.datastax.driver.core WriteType)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))
(deftest batch-client-init-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (spy/called-n-times? alia/execute 3)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 3)))))

(deftest batch-client-add-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/spy)]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
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
                            {:consistency :quorum}))
      (is (= :ok (:type result))))))

(deftest batch-client-read-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:pid 0 :cid 0 :value 0}
                                            {:pid 0 :cid 1 :value 0}
                                            {:pid 2 :cid 0 :value 2}
                                            {:pid 2 :cid 1 :value 2}
                                            {:pid 1 :cid 0 :value 1}
                                            {:pid 1 :cid 1 :value 1}])))]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "session"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "session"
                            {:select :bat
                             :columns :*}
                            {:consistency :all}))
      (is (= :ok (:type result)))
      (is (= #{0 1 2} (:value result))))))

(deftest batch-client-read-fail-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:pid 0 :cid 0 :value 0}
                                            {:pid 0 :cid 1 :value 0}
                                            {:pid 2 :cid 0 :value 2}
                                            {:pid 1 :cid 0 :value 1}
                                            {:pid 1 :cid 1 :value 1}])))]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type result)))
      (is (= [#{0 1 2} #{0 1}] (:value result))))))

(deftest batch-client-write-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (and (string? cql) (re-find #"BATCH" cql))
                                  (throw (ex-info "Timed out"
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil WriteType/BATCH_LOG 0 0)})))))]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          add-result (client/invoke! client {}
                                     {:type :invoke :f :add :value 1})]
      (is (= :info (:type add-result)))
      (is (= :write-timed-out (:error add-result))))))

(deftest batch-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (UnavailableException. nil nil 0 0)})))))]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :unavailable (:error read-result))))))

(deftest batch-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "session")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (NoHostAvailableException. {})})))))]
    (let [client (client/open! (->BatchSetClient (atom false) nil nil)
                               {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :no-host-available (:error read-result))))))
