(ns scalardb.db.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [jepsen
             [control :as c]
             [db :as db]]
            [scalardb.core :as scalar]
            [scalardb.db-extend :as ext]
            [scalardb.nemesis.cluster :as n])
  (:import (java.io File)
           (java.util Properties)))

(def ^:private ^:const CLUSTER_VALUES_YAML "scalardb-cluster-custom-values.yaml")
(def ^:private ^:const DEFAULT_SCALARDB_CLUSTER_VERSION "4.0.0-SNAPSHOT")
(def ^:private ^:const DEFAULT_HELM_CHART_VERSION "1.7.2")
(def ^:private ^:const DEFAULT_CHAOS_MESH_VERSION "2.7.2")

(def ^:private ^:const TIMEOUT_SEC 600)
(def ^:private ^:const INTERVAL_SEC 10)

(def ^:private ^:const POSTGRESQL_NAME "postgresql-scalardb-cluster")
(def ^:private ^:const MYSQL_NAME "mysql-scalardb-cluster")

(def ^:private ^:const CLUSTER_NAME "scalardb-cluster")
(def ^:private ^:const CLUSTER2_NAME (str CLUSTER_NAME "-2"))
(def ^:private ^:const CLUSTER_NODE_NAME (str CLUSTER_NAME "-node"))
(def ^:private ^:const CLUSTER2_NODE_NAME (str CLUSTER2_NAME "-node"))

(def ^:private ^:const CLUSTER_VALUES
  {:envoy {:enabled true
           :service {:type "LoadBalancer"}}

   :scalardbCluster
   {:image {:repository "ghcr.io/scalar-labs/scalardb-cluster-node"
            :tag (or (some-> (env :scalardb-cluster-version) not-empty)
                     DEFAULT_SCALARDB_CLUSTER_VERSION)}

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

(defn- throw-unsupported-db-error
  [db-type]
  (throw (ex-info "Unsupported DB for ScalarDB Cluster test" {:db db-type})))

(defn- update-cluster-values
  [test db-type values]
  (let [path [:scalardbCluster :scalardbClusterNodeProperties]
        [storage-type contact-points username password]
        (case db-type
          :postgres
          ["jdbc"
           "jdbc:postgresql://postgresql-scalardb-cluster.default.svc.cluster.local:5432/postgres"
           "postgres"
           "postgres"]
          :mysql
          ["jdbc"
           (str "jdbc:mysql://mysql-scalardb-cluster.default.svc.cluster.local:3306/" scalar/KEYSPACE)
           "root"
           "mysql"]
          (throw (ex-info "Unsupported DB for ScalarDB Cluster" {:db (:db-type test)})))
        new-db-props (-> values
                         (get-in path)
                         (str
                          ;; Storage configurations
                          "\nscalar.db.storage=" storage-type
                          "\nscalar.db.contact_points=" contact-points
                          "\nscalar.db.username=" username
                          "\nscalar.db.password=" password
                          ;; isolation level
                          "\nscalar.db.consensus_commit.isolation_level="
                          (-> test
                              :isolation-level
                              name
                              str/upper-case
                              (str/replace #"-" "_"))
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

(defn- need-two-clusters?
  [test]
  (str/includes? (:name test) "2pc"))

(defn- install!
  "Install prerequisites.
  You should already have installed kind (or similar tool), kubectl and helm."
  [db-type]
  (case db-type
    :postgres (c/exec :helm :repo :add
                      "bitnami" "https://charts.bitnami.com/bitnami")
    :mysql (c/exec :helm :repo :add
                   "bitnami" "https://charts.bitnami.com/bitnami")
    (throw-unsupported-db-error db-type))
  ;; ScalarDB cluster
  (c/exec :helm :repo :add
          "scalar-labs" "https://scalar-labs.github.io/helm-charts")
  ;; Chaos mesh
  (c/exec :helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org")

  (c/exec :helm :repo :update))

(defn- configure!
  []
  (when (and (seq (env :docker-username)) (seq (env :docker-access-token)))
    (binding [c/*dir* (System/getProperty "user.dir")]
      (try
        (c/exec :kubectl :delete :secret "scalardb-ghcr-secret")
        ;; ignore the failure when the secret doesn't exist
        (catch Exception _))
      (c/exec :kubectl :create :secret :docker-registry "scalardb-ghcr-secret"
              "--docker-server=ghcr.io"
              (str "--docker-username=" (env :docker-username))
              (str "--docker-password=" (env :docker-access-token)))))

  ;; Chaos Mesh
  (try
    (c/exec :kubectl :get :namespaces "chaos-mesh")
    (catch Exception _
      (c/exec :kubectl :create :ns "chaos-mesh"))))

(defn- start!
  [test db-type]
  (case db-type
    :postgres (c/exec
               :helm :install POSTGRESQL_NAME "bitnami/postgresql"
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
    :mysql (c/exec
            :helm :install MYSQL_NAME "bitnami/mysql"
            :--set "auth.rootPassword=mysql"
            :--set (str "auth.database=" scalar/KEYSPACE)
            :--set "primary.persistence.enabled=true"
            ;; READ-COMMITTED isolation level is ok for ScalarDB
            :--set "primary.extraFlags=--transaction-isolation=READ-COMMITTED"
            ;; Need an external IP for storage APIs
            :--set "primary.service.type=LoadBalancer"
            ;; Use legacy images
            :--set "image.repository=bitnamilegacy/mysql"
            :--set "volumePermissions.image.repository=bitnamilegacy/os-shell"
            :--set "metrics.image.repository=bitnamilegacy/mysqld-exporter"
            :--set "global.security.allowInsecureImages=true"
            :--version "14.0.3")
    (throw-unsupported-db-error db-type))

  ;; ScalarDB Cluster
  (let [chart-version (or (some-> (env :helm-chart-version) not-empty)
                          DEFAULT_HELM_CHART_VERSION)]
    (info "helm chart version: " chart-version)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (->> CLUSTER_VALUES
           (update-cluster-values test db-type)
           yaml/generate-string
           (spit CLUSTER_VALUES_YAML))
      (mapv #(c/exec :helm :install % "scalar-labs/scalardb-cluster"
                     :-f CLUSTER_VALUES_YAML
                     :--version chart-version
                     :-n "default")
            (if (need-two-clusters? test)
              [CLUSTER_NAME CLUSTER2_NAME]
              [CLUSTER_NAME]))
      (-> CLUSTER_VALUES_YAML File. .delete)))

  ;; Chaos mesh
  (c/exec :helm :install "chaos-mesh" "chaos-mesh/chaos-mesh"
          :-n "chaos-mesh"
          :--version DEFAULT_CHAOS_MESH_VERSION))

(defn- wipe!
  []
  (info "wiping old logs...")
  (binding [c/*dir* (System/getProperty "user.dir")]
    (try
      (some->> (-> (c/exec :ls) (str/split #"\s+"))
               (filter #(re-matches #"scalardb-cluster-.*\.log" %))
               seq
               (apply c/exec :rm :-f))
      (catch Exception _ nil)))
  (info "wiping the pods...")
  (doseq [cmd [[:helm :uninstall POSTGRESQL_NAME]
               [:helm :uninstall MYSQL_NAME]
               [:kubectl :delete
                :pvc :-l "app.kubernetes.io/instance=postgresql-scalardb-cluster"]
               [:kubectl :delete :pvc "data-mysql-scalardb-cluster-0"]
               [:helm :uninstall CLUSTER_NAME]
               [:helm :uninstall CLUSTER2_NAME]
               [:helm :uninstall "chaos-mesh" :-n "chaos-mesh"]]]
    (try (apply c/exec cmd) (catch Exception _ nil))))

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
    (let [pods (concat (get-pod-list CLUSTER_NODE_NAME)
                       (get-pod-list CLUSTER2_NODE_NAME))
          logs (map #(str c/*dir* \/ % ".log") pods)]
      (mapv #(spit %1 (c/exec :kubectl :logs %2)) logs pods)
      logs)))

(defn- get-load-balancer-ip
  "Get the IP of the load balancer"
  [prefix]
  (->> (c/exec :kubectl :get :svc)
       str/split-lines
       (filter #(str/includes? % prefix))
       (filter #(str/includes? % "LoadBalancer"))
       (map #(nth (str/split % #"\s+") 3))
       first))

(defn- running-pods?
  "Check a live node."
  [test]
  (-> test
      :nodes
      first
      (c/on (concat (get-pod-list CLUSTER_NODE_NAME)
                    (get-pod-list CLUSTER2_NODE_NAME)))
      count
      (= (if (need-two-clusters? test) 6 3))))

(defn- cluster-nodes-ready?
  [test]
  (and (running-pods? test)
       (try
         (c/on (-> test :nodes first)
               (->> (concat (get-pod-list CLUSTER_NODE_NAME)
                            (get-pod-list CLUSTER2_NODE_NAME))
                    (mapv #(c/exec :kubectl :wait
                                   "--for=condition=Ready"
                                   "--timeout=120s"
                                   (str "pod/" %)))))
         true
         (catch Exception e
           (warn e "An error occurred")
           false))))

(defn- wait-for-recovery
  "Wait for the node bootstrapping."
  ([test]
   (wait-for-recovery TIMEOUT_SEC INTERVAL_SEC test))
  ([timeout-sec interval-sec test]
   (when-not (cluster-nodes-ready? test)
     (Thread/sleep (* interval-sec 1000))
     (if (>= timeout-sec interval-sec)
       (wait-for-recovery (- timeout-sec interval-sec) interval-sec test)
       (throw (ex-info "Timed out waiting for pods"
                       {:causes "Some pod couldn't start"}))))))

(defn db
  "Setup ScalarDB Cluster."
  [db-type]
  (reify
    db/DB
    (setup! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe!))
      (install! db-type)
      (configure!)
      (start! test db-type)
      ;; wait for the pods
      (wait-for-recovery test))

    (teardown! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe!)))

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

(defrecord ExtCluster [db-type]
  ext/DbExtension
  (live-nodes [_ test] (running-pods? test))
  (wait-for-recovery [_ test] (wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [node (-> test :nodes first)
              ip (c/on node (get-load-balancer-ip (str CLUSTER_NAME "-envoy")))
              ip2 (c/on node (get-load-balancer-ip (str CLUSTER2_NAME "-envoy")))
              create-fn
              (fn [ip]
                (->> (doto (Properties.)
                       (.setProperty "scalar.db.transaction_manager" "cluster")
                       (.setProperty "scalar.db.contact_points" (str "indirect:" ip)))
                     (ext/set-common-properties test)))]
          (if (need-two-clusters? test)
            (mapv create-fn [ip ip2])
            (create-fn ip)))))
  (create-storage-properties [_ _]
    (let [node (-> test :nodes first)]
      (case db-type
        :postgres (let [ip (c/on node (get-load-balancer-ip POSTGRESQL_NAME))]
                    (doto (Properties.)
                      (.setProperty "scalar.db.storage" "jdbc")
                      (.setProperty "scalar.db.contact_points"
                                    (str "jdbc:postgresql://" ip ":5432/postgres"))
                      (.setProperty "scalar.db.username" "postgres")
                      (.setProperty "scalar.db.password" "postgres")))
        :mysql (let [ip (c/on node (get-load-balancer-ip MYSQL_NAME))]
                 (doto (Properties.)
                   (.setProperty "scalar.db.storage" "jdbc")
                   (.setProperty "scalar.db.contact_points"
                                 (str "jdbc:mysql://" ip ":3306/" scalar/KEYSPACE))
                   (.setProperty "scalar.db.username" "root")
                   (.setProperty "scalar.db.password" "mysql")))
        (throw-unsupported-db-error db-type)))))

(defn gen-db
  [faults admin db-type]
  (when (seq admin)
    (warn "The admin operations are ignored: " admin))
  (let [db (ext/extend-db (db db-type) (->ExtCluster db-type))]
    [db (n/nemesis-package db 60 faults) 1]))
