(ns scalardb.db.cluster-db.log-selector-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.core :as k8s]
            [scalardb.db.cluster-db.alloydb :as alloydb]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]
            [scalardb.db.cluster-db.db2 :as db2]
            [scalardb.db.cluster-db.managed :as managed]
            [scalardb.db.cluster-db.mariadb :as mariadb]
            [scalardb.db.cluster-db.mysql :as mysql]
            [scalardb.db.cluster-db.oracle :as oracle]
            [scalardb.db.cluster-db.postgres :as postgres]
            [scalardb.db.cluster-db.sqlserver :as sqlserver]
            [scalardb.db.cluster-db.tidb :as tidb]
            [scalardb.db.cluster-db.yugabytedb :as yugabytedb]))

(deftest log-selector-test
  (doseq [[backend-name backend expected]
          [[:postgres
            (postgres/gen-cluster-db)
            {"app.kubernetes.io/instance" "postgresql-scalardb-cluster"}]
           [:alloydb
            (alloydb/gen-cluster-db)
            {"alloydbomni.internal.dbadmin.goog/dbcluster"
             "alloydb-scalardb-cluster"}]
           [:yugabytedb
            (yugabytedb/gen-cluster-db)
            {:release "yugabytedb-scalardb-cluster"}]
           [:mysql
            (mysql/gen-cluster-db)
            {"app.kubernetes.io/instance" "mysql-scalardb-cluster"}]
           [:mariadb
            (mariadb/gen-cluster-db)
            {"app.kubernetes.io/instance" "mariadb-scalardb-cluster"}]
           [:tidb
            (tidb/gen-cluster-db)
            {"app.kubernetes.io/instance" "tidb-scalardb-cluster"}]
           [:sqlserver
            (sqlserver/gen-cluster-db)
            {:release "sqlserver-scalardb-cluster"}]
           [:oracle
            (oracle/gen-cluster-db)
            {:app "oracle-scalardb-cluster"}]
           [:db2
            (db2/gen-cluster-db)
            {:app "db2-scalardb-cluster"}]]]
    (testing (name backend-name)
      (is (satisfies? cluster-db/ClusterDbLogs backend))
      (is (= expected (cluster-db/log-selector backend)))
      (testing "is a valid label selector"
        (is (string? (k8s/label-selector (cluster-db/log-selector backend))))))))

(deftest managed-db-has-no-logs-test
  (testing "an externally-provisioned backend has no pods to collect"
    (is (not (satisfies? cluster-db/ClusterDbLogs
                         (managed/->ClusterDbManaged {}))))))
