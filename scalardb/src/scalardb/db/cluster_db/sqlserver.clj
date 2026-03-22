(ns scalardb.db.cluster-db.sqlserver
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const SQLSERVER_NAME "sqlserver-scalardb-cluster")
(def ^:private ^:const SQLSERVER_USER "sa")
(def ^:private ^:const SQLSERVER_PASSWORD "Str0ng!Pass")

(defrecord ClusterDbSqlServer []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:sqlserver://sqlserver-scalardb-cluster-mssqlserver-2022.default.svc.cluster.local:1433;encrypt=true;trustServerCertificate=true")

  (get-username [_] SQLSERVER_USER)

  (get-password [_] SQLSERVER_PASSWORD)

  (install! [_]
    (c/exec :helm :repo :add
            "simcube"
            "https://simcubeltd.github.io/simcube-helm-charts"))

  (configure! [_])

  (start! [_]
    (c/exec :helm :install SQLSERVER_NAME "simcube/mssqlserver-2022"
            :--set "image.repository=mcr.microsoft.com/mssql/server"
            :--set "image.tag=2022-latest"
            :--set "acceptEula.value=Y"
            :--set (str "sapassword=" SQLSERVER_PASSWORD)
            :--set "persistence.enabled=true"
            :--set "service.type=LoadBalancer"
            :--version "1.2.3"))

  (wipe! [_]
    (doseq [cmd [[:helm :uninstall SQLSERVER_NAME
                  :--timeout "3m0s" :--ignore-not-found]
                 [:kubectl :delete
                  :pvc "sqlserver-scalardb-cluster-mssqlserver-2022-data"
                  "--timeout=180s" "--ignore-not-found=true"]]]
      (try (apply c/exec cmd)
           (catch Exception e (warn e "Failed to exec:" cmd)))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip SQLSERVER_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:sqlserver://"
                           ip
                           ":1433;encrypt=true;trustServerCertificate=true"))
        (.setProperty "scalar.db.username" SQLSERVER_USER)
        (.setProperty "scalar.db.password" SQLSERVER_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbSqlServer))
