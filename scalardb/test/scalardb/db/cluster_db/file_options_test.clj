(ns scalardb.db.cluster-db.file-options-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.chaos-mesh.file-io :as file-io]
            [scalardb.db.cluster-db.alloydb :as alloydb]
            [scalardb.db.cluster-db.cassandra :as cassandra]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]
            [scalardb.db.cluster-db.db2 :as db2]
            [scalardb.db.cluster-db.mariadb :as mariadb]
            [scalardb.db.cluster-db.mysql :as mysql]
            [scalardb.db.cluster-db.oracle :as oracle]
            [scalardb.db.cluster-db.postgres :as postgres]
            [scalardb.db.cluster-db.sqlserver :as sqlserver]
            [scalardb.db.cluster-db.tidb :as tidb]
            [scalardb.db.cluster-db.yugabytedb :as yugabytedb]))

(deftest file-io-options-test
  (doseq [[backend-name backend expected]
          [[:cassandra
            (cassandra/gen-cluster-db)
            {:volume-path "/bitnami/cassandra"
             :file-path "/bitnami/cassandra/**/*"
             :pod-selector {"app.kubernetes.io/instance"
                            "cassandra-scalardb-cluster"
                            "app.kubernetes.io/name" "cassandra"}
             :container-names ["cassandra"]}]
           [:postgres
            (postgres/gen-cluster-db)
            {:volume-path "/bitnami/postgresql"
             :file-path "/bitnami/postgresql/data/**/*"
             :pod-selector {"app.kubernetes.io/instance"
                            "postgresql-scalardb-cluster"
                            "app.kubernetes.io/component" "primary"}
             :container-names ["postgresql"]}]
           [:alloydb
            (alloydb/gen-cluster-db)
            {:volume-path "/mnt/disks/pgsql"
             :file-path "/mnt/disks/pgsql/data/**/*"
             :pod-selector
             {"alloydbomni.internal.dbadmin.goog/dbcluster"
              "alloydb-scalardb-cluster"
              "alloydbomni.internal.dbadmin.goog/task-type" "database"}
             :container-names ["database"]}]
           [:yugabytedb
            (yugabytedb/gen-cluster-db)
            {:volume-path "/mnt/disk0"
             :file-path "/mnt/disk0/**/*"
             :pod-selector {:app "yb-tserver"
                            :release "yugabytedb-scalardb-cluster"}
             :container-names ["yb-tserver"]}]
           [:mysql
            (mysql/gen-cluster-db)
            {:volume-path "/bitnami/mysql"
             :file-path "/bitnami/mysql/data/**/*"
             :pod-selector {"app.kubernetes.io/instance"
                            "mysql-scalardb-cluster"
                            "app.kubernetes.io/component" "primary"}
             :container-names ["mysql"]}]
           [:mariadb
            (mariadb/gen-cluster-db)
            {:volume-path "/bitnami/mariadb"
             :file-path "/bitnami/mariadb/data/**/*"
             :pod-selector {"app.kubernetes.io/instance"
                            "mariadb-scalardb-cluster"
                            "app.kubernetes.io/component" "primary"}
             :container-names ["mariadb"]}]
           [:tidb
            (tidb/gen-cluster-db)
            {:volume-path "/var/lib/tikv"
             :file-path "/var/lib/tikv/**/*"
             :pod-selector {"app.kubernetes.io/instance"
                            "tidb-scalardb-cluster"
                            "app.kubernetes.io/component" "tikv"}
             :container-names ["tikv"]}]
           [:sqlserver
            (sqlserver/gen-cluster-db)
            {:volume-path "/var/opt/mssql"
             :file-path "/var/opt/mssql/**/*"
             :pod-selector {:app "sqlserver-scalardb-cluster-mssqlserver-2022"
                            :release "sqlserver-scalardb-cluster"}
             :container-names ["mssqlserver-2022"]}]
           [:oracle
            (oracle/gen-cluster-db)
            {:volume-path "/opt/oracle/oradata"
             :file-path "/opt/oracle/oradata/**/*"
             :pod-selector {:app "oracle-scalardb-cluster"}
             :container-names ["oracle"]}]
           [:db2
            (db2/gen-cluster-db)
            {:volume-path "/database"
             :file-path "/database/**/*"
             :pod-selector {:app "db2-scalardb-cluster"}
             :container-names ["db2"]}]]]
    (testing (name backend-name)
      (is (satisfies? cluster-db/ClusterDbFileOptions backend))
      (is (= expected (cluster-db/file-io-options backend)))
      ;; The real validator checks path containment, the container names and
      ;; the pod selector, so a typo fails here instead of after a full
      ;; cluster provision in a live run.
      (is (map? (#'file-io/validate-config
                 (cluster-db/file-io-options backend)))))))
