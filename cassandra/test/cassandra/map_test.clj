(ns cassandra.map-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [qbits.alia :as alia]
            [cassandra.core :as cassandra]
            [cassandra.collections.map :as map]
            [spy.core :as spy])
  (:import (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                UnavailableException)))

(deftest map-client-init-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/spy)]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)]
      (client/setup! client {:rf 3})
      (is (true? @(.tbl-created? client)))
      (is (= :quorum (.writec client)))
      (is (spy/called-n-times? alia/execute 5)) ;; for table creation

      ;; tables have been already created
      (client/setup! client {:rf 3})
      (is (spy/called-n-times? alia/execute 5)))))

(deftest map-client-add-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/spy)]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)]
      (client/invoke! client {} {:type :invoke :f :add :value 1})
      (is (spy/called-with? alia/execute
                            "test-conn"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "test-conn"
                            {:update :maps
                             :set-columns {:elements [+ {1 1}]}
                             :where [[= :id 0]]}
                            {:consistency :quorum})))))

(deftest map-client-read-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/mock (fn [_ cql & _]
                                         (when (contains? cql :select)
                                           [{:id 0 :elements {1 1 3 3 2 2}}])))]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)
          result (client/invoke! client {} {:type :invoke :f :read})]
      (is (spy/called-with? alia/execute
                            "test-conn"
                            {:use-keyspace :jepsen_keyspace}))
      (is (spy/called-with? alia/execute
                            "test-conn"
                            {:select :maps
                             :columns :*
                             :where [[= :id 0]]}
                            {:consistency :all
                             :retry-policy cassandra/aggressive-read}))
      (is (= :ok (:type result)))
      (is (= #{1 2 3} (:value result))))))

(deftest map-client-read-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Timed out"
                                                   {:type ::execute
                                                    :exception (ReadTimeoutException. nil nil 0 0 false)})))))]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :read-timed-out (:error read-result))))))

(deftest map-client-write-timeout-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :update)
                                  (throw (ex-info "Timed out"
                                                  {:type ::execute
                                                   :exception (WriteTimeoutException. nil nil nil 0 0)})))))]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)
          add-result (client/invoke! client {}
                                     {:type :invoke :f :add :value 1})]
      (is (= :info (:type add-result)))
      (is (= :write-timed-out (:value add-result))))))

(deftest map-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (UnavailableException. nil nil 0 0)})))))]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :unavailable (:error read-result))))))

(deftest map-client-unavailable-exception-test
  (with-redefs [alia/cluster (spy/spy)
                alia/connect (spy/stub "test-conn")
                alia/execute (spy/mock
                              (fn [_ cql & _]
                                (when (contains? cql :select)
                                  (throw (ex-info  "Unavailable"
                                                   {:type ::execute
                                                    :exception (NoHostAvailableException. {})})))))]
    (let [client-quorum (map/cql-map-client)
          client (client/open! client-quorum {:nodes ["n1" "n2" "n3"]} nil)
          read-result (client/invoke! client {} {:type :invoke :f :read})]
      (is (= :fail (:type read-result)))
      (is (= :no-host-available (:error read-result))))))
