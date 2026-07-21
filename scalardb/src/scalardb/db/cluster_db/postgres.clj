(ns scalardb.db.cluster-db.postgres
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
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

  (get-lb-service-name [_] POSTGRESQL_NAME)

  (install! [_ test]
    (helm/repo-add! test "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install! test {:release POSTGRESQL_NAME
                         :chart "bitnami/postgresql"
                         :version "16.7.0"
                         :set {:auth.postgresPassword POSTGRESQL_PASSWORD
                               :primary.persistence.enabled true
                               :service.type "LoadBalancer"
                               :primary.service.type "LoadBalancer"
                               :image.repository "bitnamilegacy/postgresql"
                               :volumePermissions.image.repository "bitnamilegacy/os-shell"
                               :metrics.image.repository "bitnamilegacy/postgres-exporter"
                               :global.security.allowInsecureImages true}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release POSTGRESQL_NAME
                                         :timeout WIPE_TIMEOUT
                                         :ignore-not-found? true})
                 #(k8s/kubectl! test :delete
                                :pvc
                                :-l "app.kubernetes.io/instance=postgresql-scalardb-cluster"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test POSTGRESQL_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5432/postgres"))
        (.setProperty "scalar.db.username" POSTGRESQL_USER)
        (.setProperty "scalar.db.password" POSTGRESQL_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbPostgres))
