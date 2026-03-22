(ns scalardb.db.cluster-db.mariadb
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.control :as c]
            [scalardb.core :as scalar]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
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

  (install! [_]
    (c/exec :helm :repo :add "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_])

  (start! [_]
    (c/exec :helm :install MARIADB_NAME "bitnami/mariadb"
            :--set (str "auth.rootPassword=" MARIADB_PASSWORD)
            :--set (str "auth.database=" scalar/KEYSPACE)
            :--set "primary.persistence.enabled=true"
            ;; Need an external IP for storage APIs
            :--set "primary.service.type=LoadBalancer"
            ;; Use legacy images
            :--set "image.repository=bitnamilegacy/mariadb"
            :--set "volumePermissions.image.repository=bitnamilegacy/os-shell"
            :--set "metrics.image.repository=bitnamilegacy/mysqld-exporter"
            :--set "global.security.allowInsecureImages=true"
            :--version "24.1.1"))

  (wipe! [_]
    (doseq [cmd [[:helm :uninstall MARIADB_NAME
                  :--timeout "3m0s" :--ignore-not-found]
                 [:kubectl :delete :pvc "data-mariadb-scalardb-cluster-0"
                  "--timeout=180s" "--ignore-not-found=true"]]]
      (try (apply c/exec cmd)
           (catch Exception e (warn e "Failed to exec:" cmd)))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip MARIADB_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mariadb://" ip ":3306/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" MARIADB_USER)
        (.setProperty "scalar.db.password" MARIADB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbMariaDb))
