(ns scalardb.db.cluster
  (:require [cheshire.core :as cheshire]
            [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [jepsen
             [control :as c]
             [db :as db]]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]
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

(defn- update-cluster-values
  [test backend-db values]
  (let [path [:scalardbCluster :scalardbClusterNodeProperties]
        storage-type (cluster-db/get-storage-type backend-db)
        contact-points (cluster-db/get-contact-points backend-db)
        username (cluster-db/get-username backend-db)
        password (cluster-db/get-password backend-db)
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
                          "\nscalar.db.jdbc.isolation_level="
                          (-> test
                              :jdbc-isolation-level
                              name
                              str/upper-case
                              (str/replace #"-" "_"))
                          ;; connection pool min idle
                          "\nscalar.db.jdbc.connection_pool.min_idle=0"
                          "\nscalar.db.jdbc.table_metadata.connection_pool.min_idle=0"
                          "\nscalar.db.jdbc.admin.connection_pool.min_idle=0"
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
  [backend-db]
  ;; Cluster backend DB
  (cluster-db/install! backend-db)

  ;; ScalarDB Cluster
  (c/exec :helm :repo :add
          "scalar-labs" "https://scalar-labs.github.io/helm-charts")
  ;; Chaos mesh
  (c/exec :helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org")

  (c/exec :helm :repo :update))

(defn- configure!
  [backend-db]
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

  (cluster-db/configure! backend-db)

  ;; Chaos Mesh
  (try
    (c/exec :kubectl :get :namespaces "chaos-mesh")
    (catch Exception _
      (c/exec :kubectl :create :ns "chaos-mesh"))))

(defn- start!
  [test backend-db]
  ;; Cluster backend DB
  (cluster-db/start! backend-db)

  ;; ScalarDB Cluster
  (let [chart-version (or (some-> (env :helm-chart-version) not-empty)
                          DEFAULT_HELM_CHART_VERSION)]
    (info "helm chart version: " chart-version)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (->> CLUSTER_VALUES
           (update-cluster-values test backend-db)
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
  [backend-db]
  (info "wiping old logs...")
  (binding [c/*dir* (System/getProperty "user.dir")]
    (try
      (some->> (-> (c/exec :ls) (str/split #"\s+"))
               (filter #(re-matches #"scalardb-cluster-.*\.log" %))
               seq
               (apply c/exec :rm :-f))
      (catch Exception _ nil)))
  (info "wiping the pods...")
  (cluster-db/wipe! backend-db)
  (doseq [cmd [[:helm :uninstall CLUSTER_NAME]
               [:helm :uninstall CLUSTER2_NAME]
               [:helm :uninstall "chaos-mesh" :-n "chaos-mesh"]]]
    (try (apply c/exec cmd) (catch Exception _ nil))))

(defn- get-pod-list
  "Get the pod list whose name starts with prefix"
  [prefix]
  (let [pods (-> (c/exec :kubectl :get :pod :-o :json)
                 (cheshire/parse-string true))]
    (->> pods
         :items
         (filter #(str/starts-with? (get-in % [:metadata :name]) prefix))
         (filter #(= "Running" (get-in % [:status :phase])))
         (map #(get-in % [:metadata :name])))))

(defn- get-logs
  [_test]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [pods (concat (get-pod-list CLUSTER_NODE_NAME)
                       (get-pod-list CLUSTER2_NODE_NAME))
          logs (map #(str c/*dir* \/ % ".log") pods)]
      (mapv #(spit %1 (c/exec :kubectl :logs %2)) logs pods)
      logs)))

(defn get-load-balancer-ip
  "Get the external IP of a LoadBalancer service whose name includes prefix"
  [prefix]
  (let [svc (-> (c/exec :kubectl :get :svc :-o :json)
                (cheshire/parse-string true))]
    (->> svc
         :items
         (filter #(str/includes? (get-in % [:metadata :name]) prefix))
         (filter #(= "LoadBalancer" (get-in % [:spec :type])))
         (map #(get-in % [:status :loadBalancer :ingress 0 :ip]))
         (remove nil?)
         first)))

(defn get-k8s-node-ip
  "Get the internal IP of a Kubernetes node"
  []
  (let [nodes (-> (c/exec :kubectl :get :nodes :-o :json)
                  (cheshire/parse-string true))]
    (->> nodes
         :items
         (mapcat #(get-in % [:status :addresses]))
         (filter #(= "InternalIP" (:type %)))
         (map :address)
         first)))

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
  [backend-db]
  (reify
    db/DB
    (setup! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! backend-db))
      (install! backend-db)
      (configure! backend-db)
      (start! test backend-db)
      ;; wait for the pods
      (wait-for-recovery test))

    (teardown! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! backend-db)))

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

(defrecord ExtCluster [backend-db]
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
                (let [client-side-optimizations-enabled (str (:enable-cluster-client-side-optimizations test))]
                  (doto (Properties.)
                    (.setProperty "scalar.db.transaction_manager" "cluster")
                    (.setProperty "scalar.db.contact_points" (str "indirect:" ip))
                    (.setProperty "scalar.db.cluster.client.piggyback_begin.enabled" client-side-optimizations-enabled)
                    (.setProperty "scalar.db.cluster.client.write_buffering.enabled" client-side-optimizations-enabled))))]
          (if (need-two-clusters? test)
            (mapv create-fn [ip ip2])
            (create-fn ip)))))
  (create-storage-properties [_ test]
    (cluster-db/create-storage-properties backend-db test)))

(def ^:private dbtype->gen-var
  {:postgres  'scalardb.db.cluster-db.postgres/gen-cluster-db
   :mysql     'scalardb.db.cluster-db.mysql/gen-cluster-db
   :mariadb   'scalardb.db.cluster-db.mariadb/gen-cluster-db
   :tidb      'scalardb.db.cluster-db.tidb/gen-cluster-db
   :sqlserver 'scalardb.db.cluster-db.sqlserver/gen-cluster-db
   :oracle    'scalardb.db.cluster-db.oracle/gen-cluster-db})

(defn- cluster-backend-db
  [db-type]
  (if-let [v (get dbtype->gen-var db-type)]
    ((requiring-resolve v))
    (throw (ex-info "Unsupported DB for ScalarDB Cluster test" {:db db-type}))))

(defn gen-db
  [faults admin db-type]
  (when (seq admin)
    (warn "The admin operations are ignored: " admin))
  (let [backend-db (cluster-backend-db db-type)
        db (ext/extend-db (db backend-db) (->ExtCluster backend-db))]
    [db (n/nemesis-package db 60 faults) 1]))
