(ns scalardb.db.cluster-db.sqlserver
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
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

  (install! [_ test]
    (helm/repo-add! test
                    "simcube"
                    "https://simcubeltd.github.io/simcube-helm-charts"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install! test {:release SQLSERVER_NAME
                         :chart "simcube/mssqlserver-2022"
                         :version "1.2.3"
                         :set {:image.repository "mcr.microsoft.com/mssql/server"
                               :image.tag "2022-latest"
                               :acceptEula.value "Y"
                               :sapassword SQLSERVER_PASSWORD
                               :persistence.enabled true
                               :service.type "LoadBalancer"}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release SQLSERVER_NAME
                                         :timeout WIPE_TIMEOUT
                                         :ignore-not-found? true})
                 #(k8s/kubectl! test :delete
                                :pvc "sqlserver-scalardb-cluster-mssqlserver-2022-data"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test SQLSERVER_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:sqlserver://"
                           ip
                           ":1433;encrypt=true;trustServerCertificate=true"))
        (.setProperty "scalar.db.username" SQLSERVER_USER)
        (.setProperty "scalar.db.password" SQLSERVER_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbSqlServer))
