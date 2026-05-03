(ns scalardb.db.cluster-db.mariadb
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.core :as scalar]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const MARIADB_NAME "mariadb-scalardb-cluster")
(def ^:private ^:const MARIADB_USER "root")
(def ^:private ^:const MARIADB_PASSWORD "mariadb")

(defrecord ClusterDbMariaDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    (str "jdbc:mariadb://mariadb-scalardb-cluster.default.svc.cluster.local:3306/"
         scalar/KEYSPACE))

  (get-username [_] MARIADB_USER)

  (get-password [_] MARIADB_PASSWORD)

  (install! [_ test]
    (helm/repo-add! test "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install! test {:release MARIADB_NAME
                         :chart "bitnami/mariadb"
                         :version "24.1.1"
                         :set {:auth.rootPassword MARIADB_PASSWORD
                               :auth.database scalar/KEYSPACE
                               :primary.persistence.enabled true
                               :primary.service.type "LoadBalancer"
                               :image.repository "bitnamilegacy/mariadb"
                               :volumePermissions.image.repository "bitnamilegacy/os-shell"
                               :metrics.image.repository "bitnamilegacy/mysqld-exporter"
                               :global.security.allowInsecureImages true}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release MARIADB_NAME
                                       :timeout WIPE_TIMEOUT
                                       :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :pvc "data-mariadb-scalardb-cluster-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test MARIADB_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mariadb://" ip ":3306/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" MARIADB_USER)
        (.setProperty "scalar.db.password" MARIADB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbMariaDb))
