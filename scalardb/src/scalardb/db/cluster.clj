(ns scalardb.db.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [jepsen
             [control :as c]
             [db :as db]]
            [scalardb.db-extend :as ext]
            [scalardb.nemesis.cluster :as n])
  (:import (java.io File)
           (java.util Properties)))

(def ^:private ^:const CLUSTER_VALUES_YAML "scalardb-cluster-custom-values.yaml")
(def ^:private ^:const DEFAULT_SCALARDB_CLUSTER_VERSION "4.0.0-SNAPSHOT")
(def ^:private ^:const DEFAULT_HELM_CHART_VERSION "1.7.2")
(def ^:private ^:const DEFAULT_CHAOS_MESH_VERSION "2.7.2")

(def ^:private ^:const DEFAULT_CLUSTER_NODE_COUNT 3)
(def ^:private ^:const DEFAULT_CASSANDRA_REPLICA_COUNT 3)

(def ^:private ^:const TIMEOUT_SEC 600)
(def ^:private ^:const INTERVAL_SEC 10)

(def ^:private ^:const CLUSTER_NODE_NAME "scalardb-cluster-node")
(def ^:private ^:const CASSANDRA_NODE_NAME "cassandra-scalardb-cluster")

(def ^:private ^:const CLUSTER_VALUES
  {:envoy {:enabled true
           :service {:type "LoadBalancer"}}

   :scalardbCluster
   {:image {:repository "ghcr.io/scalar-labs/scalardb-cluster-node"
            :tag (or (some-> (env :scalardb-cluster-version) not-empty)
                     DEFAULT_SCALARDB_CLUSTER_VERSION)}

    ;; Storage configurations will be set later
    :scalardbClusterNodeProperties
    (str/join "\n"
              ;; ScalarDB Cluster configurations
              ["scalar.db.cluster.membership.type=KUBERNETES"
               "scalar.db.cluster.membership.kubernetes.endpoint.namespace_name=${env:SCALAR_DB_CLUSTER_MEMBERSHIP_KUBERNETES_ENDPOINT_NAMESPACE_NAME}"
               "scalar.db.cluster.membership.kubernetes.endpoint.name=${env:SCALAR_DB_CLUSTER_MEMBERSHIP_KUBERNETES_ENDPOINT_NAME}"
               ""
               ;; Set to true to include transaction metadata in the records
               "scalar.db.consensus_commit.include_metadata.enabled=true"])

    :imagePullSecrets [{:name "scalardb-ghcr-secret"}]}})

(defn- update-cluster-values
  [test values]
  (let [path [:scalardbCluster :scalardbClusterNodeProperties]
        [storage contact-points user-pass]
        (case (:db-type test)
          :cluster           ["jdbc"
                              "jdbc:postgresql://postgresql-scalardb-cluster.default.svc.cluster.local:5432/postgres"
                              "postgres"]
          :cluster-cassandra ["cassandra"
                              (->> (map #(str "cassandra-scalardb-cluster-"
                                              %
                                              ".cassandra-scalardb-cluster-headless.default.svc.cluster.local")
                                        (range DEFAULT_CASSANDRA_REPLICA_COUNT))
                                   (str/join ","))
                              "cassandra"]
          (throw (ex-info "Unsupported DB type" {:db-type (:db-type test)})))
        isolation-level (-> test
                            :isolation-level
                            name
                            str/upper-case
                            (str/replace #"-" "_"))
        new-db-props (-> values
                         (get-in path)
                         (str
                          ;; storage
                          "\nscalar.db.storage="
                          storage
                          "\nscalar.db.contact_points="
                          contact-points
                          "\nscalar.db.username="
                          user-pass
                          "\nscalar.db.password="
                          user-pass
                          ;; isolation level
                          "\nscalar.db.consensus_commit.isolation_level="
                          isolation-level
                          ;; one phase commit
                          (when (:enable-one-phase-commit test)
                            "\nscalar.db.consensus_commit.one_phase_commit.enabled=true")
                          ;; group commit - set hard-coded configurations for now
                          (when (:enable-group-commit test)
                            (str/join
                             "\n"
                             ["\nscalar.db.consensus_commit.group_commit.enabled=true"
                              "scalar.db.consensus_commit.coordinator.group_commit.slot_capacity=4"
                              "scalar.db.consensus_commit.coordinator.group_commit.old_group_abort_timeout_millis=15000"
                              "scalar.db.consensus_commit.coordinator.group_commit.delayed_slot_move_timeout_millis=400"
                              "scalar.db.consensus_commit.coordinator.group_commit.metrics_monitor_log_enabled=true"]))))]
    (assoc-in values path new-db-props)))

(defn- install!
  "Install prerequisites.
  You should already have installed kind (or similar tool), kubectl and helm."
  []
  ;; postgre
  (c/exec :helm :repo :add "bitnami" "https://charts.bitnami.com/bitnami")
  ;; ScalarDB cluster
  (c/exec :helm :repo :add
          "scalar-labs" "https://scalar-labs.github.io/helm-charts")
  ;; Chaos mesh
  (c/exec :helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org")

  (c/exec :helm :repo :update))

(defn- configure!
  [test]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (try
      (c/exec :kubectl :delete :secret "scalardb-ghcr-secret")
      ;; ignore the failure when the secret doesn't exist
      (catch Exception _))
    (c/exec :kubectl :create :secret :docker-registry "scalardb-ghcr-secret"
            "--docker-server=ghcr.io"
            (str "--docker-username=" (:docker-username test))
            (str "--docker-password=" (:docker-access-token test))))

  ;; Chaos Mesh
  (try
    (c/exec :kubectl :get :namespaces "chaos-mesh")
    (catch Exception _
      (c/exec :kubectl :create :ns "chaos-mesh"))))

(defn- start!
  [test]
  ;; postgre or cassandra
  (case (:db-type test)
    :cluster           (c/exec
                        :helm :install "postgresql-scalardb-cluster" "bitnami/postgresql"
                        :--set "auth.postgresPassword=postgres"
                        :--set "primary.persistence.enabled=true"
                         ;; Need an external IP for storage APIs
                        :--set "service.type=LoadBalancer"
                        :--set "primary.service.type=LoadBalancer"
                        ;; Use legacy images
                        :--set "image.repository=bitnamilegacy/postgresql"
                        :--set "volumePermissions.image.repository=bitnamilegacy/os-shell"
                        :--set "metrics.image.repository=bitnamilegacy/postgres-exporter"
                        :--set "global.security.allowInsecureImages=true"
                        :--version "16.7.0")
    :cluster-cassandra (c/exec
                        :helm :install "cassandra-scalardb-cluster" "bitnami/cassandra"
                        :--set "dbUser.user=cassandra"
                        :--set "dbUser.user=cassandra"
                        :--set "dbUser.password=cassandra"
                        :--set (str "replicaCount=" DEFAULT_CASSANDRA_REPLICA_COUNT)
                        ;; TODO: config cassandra.yaml for commitlog_sync
                        :--set "persistence.enabled=true"
                        ;; Need an external IP for storage APIs
                        :--set "service.type=LoadBalancer"
                        :--set "primary.service.type=LoadBalancer")
    (throw (ex-info "Unsupported DB type" {:db-type (:db-type test)})))

  ;; ScalarDB Cluster
  (let [chart-version (or (some-> (env :helm-chart-version) not-empty)
                          DEFAULT_HELM_CHART_VERSION)]
    (info "helm chart version: " chart-version)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (->> CLUSTER_VALUES
           (update-cluster-values test)
           yaml/generate-string
           (spit CLUSTER_VALUES_YAML))
      (c/exec :helm :install "scalardb-cluster" "scalar-labs/scalardb-cluster"
              :-f CLUSTER_VALUES_YAML
              :--version chart-version
              :-n "default")
      (-> CLUSTER_VALUES_YAML File. .delete)))

  ;; Chaos mesh
  (c/exec :helm :install "chaos-mesh" "chaos-mesh/chaos-mesh"
          :-n "chaos-mesh"
          :--version DEFAULT_CHAOS_MESH_VERSION))

(defn- wipe!
  [test]
  ;; ignore errors because these files or pods might not exist
  (try
    (info "wiping old logs...")
    (binding [c/*dir* (System/getProperty "user.dir")]
      (some->> (-> (c/exec :ls) (str/split #"\s+"))
               (filter #(re-matches #"scalardb-cluster-node-.*\.log" %))
               seq
               (apply c/exec :rm :-f)))
    (catch Exception _))
  (info "wiping the pods...")
  (try
    (c/exec :helm :uninstall
            (case (:db-type test)
              :cluster :postgresql-scalardb-cluster
              :cluster-cassandra :cassandra-scalardb-cluster))
    (catch Exception _))
  (try
    (c/exec :kubectl :delete :pvc :-l
            (str "app.kubernetes.io/instance="
                 (case (:db-type test)
                   :cluster "postgresql-scalardb-cluster"
                   :cluster-cassandra "cassandra-scalardb-cluster")))
    (catch Exception _))
  (try
    (c/exec :helm :uninstall :scalardb-cluster)
    (catch Exception _))
  (try
    (c/exec :helm :uninstall :chaos-mesh :-n "chaos-mesh")
    (catch Exception _)))

(defn- get-pod-list
  [name]
  (->> (c/exec :kubectl :get :pod)
       str/split-lines
       (filter #(str/starts-with? % name))
       (filter #(str/includes? % "Running"))
       (map #(first (str/split % #"\s+")))))

(defn- get-logs
  [_test]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [pods (get-pod-list CLUSTER_NODE_NAME)
          logs (map #(str c/*dir* \/ % ".log") pods)]
      (mapv #(spit %1 (c/exec :kubectl :logs %2)) logs pods)
      logs)))

(defn get-load-balancer-ip
  "Get the IP of the load balancer"
  []
  (->> (c/exec :kubectl :get :svc)
       str/split-lines
       (filter #(str/includes? % "scalardb-cluster-envoy"))
       (filter #(str/includes? % "LoadBalancer"))
       (map #(nth (str/split % #"\s+") 3))
       first))

(defn get-postgres-ip
  "Get the IP of the Postgres"
  []
  (->> (c/exec :kubectl :get :svc)
       str/split-lines
       (filter #(str/includes? % "postgresql-scalardb-cluster"))
       (filter #(str/includes? % "LoadBalancer"))
       (map #(nth (str/split % #"\s+") 3))
       first))

(defn get-cassandra-ip
  "Get one IP of the Cassandra nodes"
  []
  (->> (c/exec :kubectl :get :svc)
       str/split-lines
       (filter #(str/includes? % "cassandra-scalardb-cluster"))
       (filter #(str/includes? % "LoadBalancer"))
       (map #(nth (str/split % #"\s+") 3))
       first))

(defn- running-pods?
  "Check if nodes are running."
  [test prefix num]
  (try
    (info "DEBUG:" (-> test :nodes first (c/on (c/exec :kubectl :describe :pod "cassandra-scalardb-cluster-0"))))
    (info "DEBUG:" (-> test :nodes first (c/on (c/exec :kubectl :describe :pod "cassandra-scalardb-cluster-1"))))
    (info "DEBUG:" (-> test :nodes first (c/on (c/exec :kubectl :describe :pod "cassandra-scalardb-cluster-2"))))
    (catch Exception _ nil))
  (-> test
      :nodes
      first
      (c/on (get-pod-list prefix))
      count
      (= num)))

(defn- cluster-nodes-ready?
  [test]
  (and (running-pods? test CLUSTER_NODE_NAME DEFAULT_CLUSTER_NODE_COUNT)
       (try
         (c/on (-> test :nodes first)
               (->> (get-pod-list CLUSTER_NODE_NAME)
                    (mapv #(c/exec :kubectl :wait
                                   "--for=condition=Ready"
                                   "--timeout=120s"
                                   (str "pod/" %)))))
         true
         (catch Exception e
           (warn e "An error occurred")
           false))))

(defn- cassandra-nodes-ready?
  [test]
  (or (not= (:db-type test) :cluster-cassandra)
      (and (running-pods? test
                          CASSANDRA_NODE_NAME
                          DEFAULT_CASSANDRA_REPLICA_COUNT)
           (try
             (c/on (-> test :nodes first)
                   (->> (get-pod-list CASSANDRA_NODE_NAME)
                        (mapv #(c/exec :kubectl :wait
                                       "--for=condition=Ready"
                                       "--timeout=120s"
                                       (str "pod/" %)))))
             true
             (catch Exception e
               (warn (.getMessage e))
               false)))))

(defn- wait-for-recovery
  "Wait for the node bootstrapping."
  ([test]
   (wait-for-recovery TIMEOUT_SEC INTERVAL_SEC test))
  ([timeout-sec interval-sec test]
   (when-not (and (cassandra-nodes-ready? test) (cluster-nodes-ready? test))
     (Thread/sleep (* interval-sec 1000))
     (if (>= timeout-sec interval-sec)
       (wait-for-recovery (- timeout-sec interval-sec) interval-sec test)
       (throw (ex-info "Timed out waiting for pods"
                       {:causes "Some pod couldn't start"}))))))

(defn db
  "Setup ScalarDB Cluster."
  []
  (reify
    db/DB
    (setup! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! test))
      (install!)
      (configure! test)
      (start! test)
      ;; wait for the pods
      (wait-for-recovery test))

    (teardown! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! test)))

    db/Primary
    (primaries [_ test] (:nodes test))
    (setup-primary! [_ _ _])

    db/Pause
    (pause! [_ _ _]
      (n/apply-pod-fault-exp :pause))
    (resume! [_ _ _]
      (n/delete-pod-fault-exp))

    db/Kill
    (start! [_ _ _]
      (n/delete-pod-fault-exp))
    (kill! [_ _ _]
      (n/apply-pod-fault-exp :kill))

    db/LogFiles
    (log-files [_ test _]
      (get-logs test))))

(defrecord ExtCluster []
  ext/DbExtension
  (live-nodes [_ test] (running-pods? test
                                      CLUSTER_NODE_NAME
                                      DEFAULT_CLUSTER_NODE_COUNT))
  (wait-for-recovery [_ test] (wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [node (-> test :nodes first)
              ip (c/on node (get-load-balancer-ip))]
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.transaction_manager" "cluster")
                 (.setProperty "scalar.db.contact_points"
                               (str "indirect:" ip))
                 (.setProperty "scalar.db.sql.connection_mode" "cluster")
                 (.setProperty "scalar.db.sql.cluster_mode.contact_points"
                               (str "indirect:" ip)))
               (ext/set-common-properties test)))))
  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          db-type (:db-type test)
          [storage contact-points user-pass]
          (c/on node (case db-type
                       :cluster ["jdbc"
                                 (str "jdbc:postgresql://"
                                      (get-postgres-ip)
                                      ":5432/postgres")
                                 "postgres"]
                       :cluster-cassandra ["cassandra"
                                           (get-cassandra-ip)
                                           "cassandra"]
                       (throw (ex-info "Unsupported DB type" {:db-type db-type}))))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" storage)
        (.setProperty "scalar.db.contact_points" contact-points)
        (.setProperty "scalar.db.username" user-pass)
        (.setProperty "scalar.db.password" user-pass)))))

(defn gen-db
  [faults admin]
  (when (seq admin)
    (warn "The admin operations are ignored: " admin))
  [(ext/extend-db (db) (->ExtCluster)) (n/nemesis-package db 60 faults) 1])
