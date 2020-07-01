(ns scalardl.cassandra-test
  (:require [clojure.test :refer :all]
            [qbits.alia :as alia]
            [scalardl.cassandra :as cassandra]
            [spy.core :as spy]))

(def TX_COMMITTED 3)

(deftest check-tx-state-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/stub [{:tx_state TX_COMMITTED}])]
    (is (true? (cassandra/check-tx-state? "txid"
                                          {:cass-nodes "localhost"})))))

(deftest check-tx-state-no-state-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/stub [])]
    (is (false? (cassandra/check-tx-state? "txid"
                                           {:cass-nodes "localhost"})))))

(deftest check-tx-state-fail-test
  (with-redefs [alia/cluster (spy/stub "cluster")
                alia/connect (spy/stub "session")
                alia/execute (spy/mock (fn [_ _ _]
                                         (throw (ex-info "fail" {}))))]
    (is (thrown? clojure.lang.ExceptionInfo
                 (cassandra/check-tx-state? "txid"
                                            {:cass-nodes "localhost"})))))
