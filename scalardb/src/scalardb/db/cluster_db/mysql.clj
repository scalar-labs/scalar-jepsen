(ns scalardb.db.cluster-db.mysql
  (:require [jepsen.control :as c]
            [scalardb.core :as scalar]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const MYSQL_NAME "mysql-scalardb-cluster")
(def ^:private ^:const MYSQL_USER "root")
(def ^:private ^:const MYSQL_PASSWORD "mysql")

(defrecord ClusterDbMySql []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    (str "jdbc:mysql://mysql-scalardb-cluster.default.svc.cluster.local:3306/"
         scalar/KEYSPACE))

  (get-username [_] MYSQL_USER)

  (get-password [_] MYSQL_PASSWORD)

  (install! [_]
    (c/exec :helm :repo :add "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_])

  (start! [_]
    (c/exec :helm :install MYSQL_NAME "bitnami/mysql"
            :--set (str "auth.rootPassword=" MYSQL_PASSWORD)
            :--set (str "auth.database=" scalar/KEYSPACE)
            :--set "primary.persistence.enabled=true"
            ;; Need an external IP for storage APIs
            :--set "primary.service.type=LoadBalancer"
            ;; Use legacy images
            :--set "image.repository=bitnamilegacy/mysql"
            :--set "volumePermissions.image.repository=bitnamilegacy/os-shell"
            :--set "metrics.image.repository=bitnamilegacy/mysqld-exporter"
            :--set "global.security.allowInsecureImages=true"
            :--version "14.0.3"))

  (wipe! [_]
    (doseq [cmd [[:helm :uninstall MYSQL_NAME]
                 [:kubectl :delete :pvc "data-mysql-scalardb-cluster-0"]]]
      (try (apply c/exec cmd) (catch Exception _ nil))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip MYSQL_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mysql://" ip ":3306/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" MYSQL_USER)
        (.setProperty "scalar.db.password" MYSQL_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbMySql))
