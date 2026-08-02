(ns scalardb.db.cluster-db.cassandra
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const CASSANDRA_NAME "cassandra-scalardb-cluster")
(def ^:private ^:const CASSANDRA_USER "cassandra")
(def ^:private ^:const CASSANDRA_PASSWORD "cassandra")
(def ^:private ^:const CASSANDRA_REPLICA_COUNT 3)
(def ^:private ^:const CASSANDRA_CHART_VERSION "12.3.10")
(def ^:private ^:const CASSANDRA_IMAGE
  "docker.io/bitnamilegacy/cassandra:5.0.5-debian-12-r7")
(def ^:private ^:const PREPARE_CONFIG_COMMAND
  (str "cp -R /opt/bitnami/cassandra/conf.default/. /work/ && "
       "sed -i '/^commitlog_sync_period:/d' /work/cassandra.yaml"))

(defn- internal-contact-points
  []
  (->> (range CASSANDRA_REPLICA_COUNT)
       (map #(str CASSANDRA_NAME "-" % "." CASSANDRA_NAME
                  "-headless.default.svc.cluster.local"))
       (str/join ",")))

(defrecord ClusterDbCassandra []
  ClusterDb
  (get-storage-type [_] "cassandra")

  (get-contact-points [_] (internal-contact-points))

  (get-username [_] CASSANDRA_USER)

  (get-password [_] CASSANDRA_PASSWORD)

  (install! [_ test]
    (helm/repo-add! test "bitnami" "https://charts.bitnami.com/bitnami"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install!
     test
     {:release CASSANDRA_NAME
      :chart "bitnami/cassandra"
      :version CASSANDRA_CHART_VERSION
      :wait? true
      :timeout "600s"
      :set {:dbUser.user CASSANDRA_USER
            :dbUser.password CASSANDRA_PASSWORD
            :replicaCount CASSANDRA_REPLICA_COUNT
            :persistence.enabled true
            :service.type "LoadBalancer"
            :jvm.maxHeapSize "1024M"
            :jvm.newHeapSize "256M"
            :resourcesPreset "none"
            :resources.requests.cpu "250m"
            :resources.requests.memory "1Gi"
            :resources.limits.cpu "1"
            :resources.limits.memory "2Gi"
            "extraEnvVars[0].name" "CASSANDRA_CFG_YAML_COMMITLOG_SYNC"
            "extraEnvVars[0].value" "batch"
            "initContainers[0].name" "prepare-cassandra-config"
            "initContainers[0].image" CASSANDRA_IMAGE
            "initContainers[0].command[0]" "bash"
            "initContainers[0].command[1]" "-ec"
            "initContainers[0].args[0]" PREPARE_CONFIG_COMMAND
            "initContainers[0].volumeMounts[0].name" "empty-dir"
            "initContainers[0].volumeMounts[0].mountPath" "/work"
            "initContainers[0].volumeMounts[0].subPath" "app-default-conf-dir"
            "extraVolumeMounts[0].name" "empty-dir"
            "extraVolumeMounts[0].mountPath" "/opt/bitnami/cassandra/conf.default"
            "extraVolumeMounts[0].subPath" "app-default-conf-dir"
            :image.repository "bitnamilegacy/cassandra"
            :volumePermissions.image.repository "bitnamilegacy/os-shell"
            :metrics.image.repository "bitnamilegacy/cassandra-exporter"
            :global.security.allowInsecureImages true}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release CASSANDRA_NAME
                                         :timeout WIPE_TIMEOUT
                                         :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :pvc
                                :-l (str "app.kubernetes.io/instance="
                                         CASSANDRA_NAME)
                                :--timeout WIPE_TIMEOUT
                                "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test CASSANDRA_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "cassandra")
        (.setProperty "scalar.db.contact_points" ip)
        (.setProperty "scalar.db.username" CASSANDRA_USER)
        (.setProperty "scalar.db.password" CASSANDRA_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbCassandra))
