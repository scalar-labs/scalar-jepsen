(ns scalardb.db.cluster-db.mysql
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.core :as scalar]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
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

  (install! [_ test]
    (helm/repo-add! test "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install! test {:release MYSQL_NAME
                         :chart "bitnami/mysql"
                         :version "14.0.3"
                         :set {:auth.rootPassword MYSQL_PASSWORD
                               :auth.database scalar/KEYSPACE
                               :primary.persistence.enabled true
                               :primary.service.type "LoadBalancer"
                               :image.repository "bitnamilegacy/mysql"
                               :volumePermissions.image.repository "bitnamilegacy/os-shell"
                               :metrics.image.repository "bitnamilegacy/mysqld-exporter"
                               :global.security.allowInsecureImages true}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release MYSQL_NAME
                                         :timeout WIPE_TIMEOUT
                                         :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :pvc "data-mysql-scalardb-cluster-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test MYSQL_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mysql://" ip ":3306/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" MYSQL_USER)
        (.setProperty "scalar.db.password" MYSQL_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbMySql))
