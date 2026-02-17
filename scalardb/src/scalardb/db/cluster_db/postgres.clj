(ns scalardb.db.cluster-db.postgres
  (:require [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const POSTGRESQL_NAME "postgresql-scalardb-cluster")
(def ^:private ^:const POSTGRESQL_USER "postgres")
(def ^:private ^:const POSTGRESQL_PASSWORD "postgres")

(defrecord ClusterDbPostgres []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:postgresql://postgresql-scalardb-cluster.default.svc.cluster.local:5432/postgres")

  (get-username [_] POSTGRESQL_USER)

  (get-password [_] POSTGRESQL_PASSWORD)

  (install! [_]
    (c/exec :helm :repo :add "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_])

  (start! [_]
    (c/exec :helm :install POSTGRESQL_NAME "bitnami/postgresql"
            :--set (str "auth.postgresPassword=" POSTGRESQL_PASSWORD)
            :--set "primary.persistence.enabled=true"
            ;; Need an external IP for storage APIs
            :--set "service.type=LoadBalancer"
            :--set "primary.service.type=LoadBalancer"
            ;; Use legacy images
            :--set "image.repository=bitnamilegacy/postgresql"
            :--set "volumePermissions.image.repository=bitnamilegacy/os-shell"
            :--set "metrics.image.repository=bitnamilegacy/postgres-exporter"
            :--set "global.security.allowInsecureImages=true"
            :--version "16.7.0"))

  (wipe! [_]
    (doseq [cmd [[:helm :uninstall POSTGRESQL_NAME]
                 [:kubectl :delete
                  :pvc
                  :-l "app.kubernetes.io/instance=postgresql-scalardb-cluster"]]]
      (try (apply c/exec cmd) (catch Exception _ nil))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip POSTGRESQL_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5432/postgres"))
        (.setProperty "scalar.db.username" POSTGRESQL_USER)
        (.setProperty "scalar.db.password" POSTGRESQL_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbPostgres))
