(ns scalardl.cassandra-test
  (:require [clojure.test :refer :all]
            [qbits.alia :as alia]
            [scalardl.cassandra :as cassandra]
            [spy.core :as spy]))

(def TX_COMMITTED 3)
(def TX_ABORTED 4)

(deftest committed-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/stub [{:tx_state TX_COMMITTED}])]
    (is (true? (cassandra/committed? "txid" {:cass-nodes "localhost"})))))

(deftest committed-when-aborted-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/stub [{:tx_state TX_ABORTED}])]
    (is (false? (cassandra/committed? "txid" {:cass-nodes "localhost"})))))

(deftest committed-when-no-state-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/stub [])]
    (is (false? (cassandra/committed? "txid" {:cass-nodes "localhost"})))))

(deftest committed-when-read-fail-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ _ _]
                                         (throw (ex-info "fail" {}))))]
    (is (thrown? clojure.lang.ExceptionInfo
                 (cassandra/committed? "txid" {:cass-nodes "localhost"})))))
