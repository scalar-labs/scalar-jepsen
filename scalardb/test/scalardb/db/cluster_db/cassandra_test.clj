(ns scalardb.db.cluster-db.cassandra-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :as cluster]
            [scalardb.db.cluster-db.cassandra :as cassandra]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]))

(deftest connection-settings-test
  (let [db (cassandra/gen-cluster-db)]
    (is (= "cassandra" (cluster-db/get-storage-type db)))
    (is (= (str "cassandra-scalardb-cluster-0."
                "cassandra-scalardb-cluster-headless.default.svc.cluster.local,"
                "cassandra-scalardb-cluster-1."
                "cassandra-scalardb-cluster-headless.default.svc.cluster.local,"
                "cassandra-scalardb-cluster-2."
                "cassandra-scalardb-cluster-headless.default.svc.cluster.local")
           (cluster-db/get-contact-points db)))
    (is (= "cassandra" (cluster-db/get-username db)))
    (is (= "cassandra" (cluster-db/get-password db)))))

(deftest start-settings-test
  (let [install-opts (atom nil)
        db (cassandra/gen-cluster-db)]
    (with-redefs [helm/install! (fn [_ opts] (reset! install-opts opts))]
      (cluster-db/start! db {}))
    (is (= "cassandra-scalardb-cluster" (:release @install-opts)))
    (is (= "bitnami/cassandra" (:chart @install-opts)))
    (is (= "12.3.10" (:version @install-opts)))
    (is (true? (:wait? @install-opts)))
    (is (= "600s" (:timeout @install-opts)))
    (is (= 3 (get-in @install-opts [:set :replicaCount])))
    (is (true? (get-in @install-opts [:set :persistence.enabled])))
    (is (= "CASSANDRA_CFG_YAML_COMMITLOG_SYNC"
           (get-in @install-opts [:set "extraEnvVars[0].name"])))
    (is (= "batch"
           (get-in @install-opts [:set "extraEnvVars[0].value"])))
    (is (= "prepare-cassandra-config"
           (get-in @install-opts [:set "initContainers[0].name"])))
    (is (= (str "cp -R /opt/bitnami/cassandra/conf.default/. /work/ && "
                "sed -i '/^commitlog_sync_period:/d' /work/cassandra.yaml")
           (get-in @install-opts [:set "initContainers[0].args[0]"])))
    (is (= "app-default-conf-dir"
           (get-in @install-opts
                   [:set "initContainers[0].volumeMounts[0].subPath"])))
    (is (= "/opt/bitnami/cassandra/conf.default"
           (get-in @install-opts
                   [:set "extraVolumeMounts[0].mountPath"])))
    (is (= "app-default-conf-dir"
           (get-in @install-opts
                   [:set "extraVolumeMounts[0].subPath"])))))

(deftest create-storage-properties-test
  (with-redefs [cluster/get-load-balancer-ip (fn [_ _] "192.0.2.1")]
    (let [properties (cluster-db/create-storage-properties
                      (cassandra/gen-cluster-db)
                      {})]
      (is (= "cassandra"
             (.getProperty properties "scalar.db.storage")))
      (is (= "192.0.2.1"
             (.getProperty properties "scalar.db.contact_points")))
      (is (= "cassandra"
             (.getProperty properties "scalar.db.username")))
      (is (= "cassandra"
             (.getProperty properties "scalar.db.password"))))))
