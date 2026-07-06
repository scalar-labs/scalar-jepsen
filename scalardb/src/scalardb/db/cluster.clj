(ns scalardb.db.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [jepsen.db :as db]
            [jepsen.k8s.chaos-mesh.core :as cm]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [jepsen.store :as store]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]
            [scalardb.db-extend :as ext])
  (:import (java.io File)
           (java.util Properties)))

(def ^:private ^:const CLUSTER_VALUES_YAML "scalardb-cluster-custom-values.yaml")
(def ^:private ^:const DEFAULT_SCALARDB_CLUSTER_VERSION "4.0.0-SNAPSHOT")
(def ^:private ^:const DEFAULT_HELM_CHART_VERSION "1.7.2")

(def ^:const WIPE_TIMEOUT "180s")
(def ^:private ^:const TIMEOUT_SEC 600)
(def ^:private ^:const INTERVAL_SEC 10)

(def ^:private ^:const CLUSTER_NAME "scalardb-cluster")
(def ^:private ^:const CLUSTER2_NAME (str CLUSTER_NAME "-2"))
(def ^:private ^:const NODE_SELECTOR "app.kubernetes.io/app=scalardb-cluster")

(def ^:private ^:const LB_SCHEME_ANNOTATION
  "service.beta.kubernetes.io/aws-load-balancer-scheme")

(def ^:private ^:const CLUSTER_VALUES
  {:envoy {:enabled true
           :service {:type "LoadBalancer"}}

   :scalardbCluster
   {:image {:repository "ghcr.io/scalar-labs/scalardb-cluster-node"
            :tag (or (some-> (env :scalardb-cluster-version) not-empty)
                     DEFAULT_SCALARDB_CLUSTER_VERSION)}

    :scalardbClusterNodeProperties
    (str/join "\n"
              ["scalar.db.cluster.membership.type=KUBERNETES"
               "scalar.db.cluster.membership.kubernetes.endpoint.namespace_name=${env:SCALAR_DB_CLUSTER_MEMBERSHIP_KUBERNETES_ENDPOINT_NAMESPACE_NAME}"
               "scalar.db.cluster.membership.kubernetes.endpoint.name=${env:SCALAR_DB_CLUSTER_MEMBERSHIP_KUBERNETES_ENDPOINT_NAME}"
               ""
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
                          "\nscalar.db.storage=" storage-type
                          "\nscalar.db.contact_points=" contact-points
                          "\nscalar.db.username=" username
                          "\nscalar.db.password=" password
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
                          "\nscalar.db.jdbc.connection_pool.min_idle=0"
                          "\nscalar.db.jdbc.table_metadata.connection_pool.min_idle=0"
                          "\nscalar.db.jdbc.admin.connection_pool.min_idle=0"
                          (when (:enable-one-phase-commit test)
                            "\nscalar.db.consensus_commit.one_phase_commit.enabled=true")
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

(defn- expose-loadbalancers!
  "When --lb-internet-facing is set, annotate every LoadBalancer service in the
  namespace so the cloud provisions it internet-facing. This covers both the
  Envoy entry point and each backend DB's service: the checker reaches the
  backend directly via the ScalarDB storage API, so the backend must be
  reachable from outside the cloud network too. Backends exposed via NodePort +
  node IP (e.g. Db2, Oracle) are not LoadBalancers and need separate handling.
  The annotation is ignored by providers that don't recognize it (e.g. MetalLB)."
  [test]
  (when (:lb-internet-facing test)
    (doseq [name (->> (k8s/services test {})
                      :items
                      (filter #(= "LoadBalancer" (get-in % [:spec :type])))
                      (map #(get-in % [:metadata :name])))]
      (info "exposing LoadBalancer as internet-facing:" name)
      (k8s/kubectl! test :annotate :svc name
                    (str LB_SCHEME_ANNOTATION "=internet-facing")
                    :--overwrite))))

(defn- install!
  "Install prerequisites.
  You should already have installed kind (or similar tool), kubectl and helm."
  [test backend-db]
  (cluster-db/install! backend-db test)
  (helm/repo-add! test "scalar-labs" "https://scalar-labs.github.io/helm-charts")
  (helm/repo-update! test))

(defn- configure!
  [test backend-db]
  (when (and (seq (env :docker-username)) (seq (env :docker-access-token)))
    (try
      (k8s/kubectl! test :delete :secret "scalardb-ghcr-secret")
      (catch Exception _))
    (k8s/kubectl! test :create :secret :docker-registry "scalardb-ghcr-secret"
                  "--docker-server=ghcr.io"
                  (str "--docker-username=" (env :docker-username))
                  (str "--docker-password=" (env :docker-access-token))))
  (cluster-db/configure! backend-db test))

(defn- start!
  [test backend-db]
  (cluster-db/start! backend-db test)

  (let [chart-version (or (some-> (env :helm-chart-version) not-empty)
                          DEFAULT_HELM_CHART_VERSION)]
    (info "helm chart version:" chart-version)
    (->> CLUSTER_VALUES
         (update-cluster-values test backend-db)
         yaml/generate-string
         (spit CLUSTER_VALUES_YAML))
    (doseq [name (if (need-two-clusters? test)
                   [CLUSTER_NAME CLUSTER2_NAME]
                   [CLUSTER_NAME])]
      (helm/install! test {:release name
                           :chart "scalar-labs/scalardb-cluster"
                           :values [CLUSTER_VALUES_YAML]
                           :version chart-version
                           :namespace "default"}))
    (.delete (File. CLUSTER_VALUES_YAML)))

  (cm/setup! test {})

  ;; Expose the Envoy and backend DB LoadBalancers to outside the cloud network
  ;; when requested (e.g. a Jepsen control running outside the VPC on EKS).
  (expose-loadbalancers! test))

(defn- wipe!
  [test backend-db]
  (info "wiping old logs...")
  (doseq [f (.listFiles (File. (System/getProperty "user.dir")))]
    (when (and (.isFile f)
               (re-matches #"scalardb-cluster-.*\.log" (.getName f)))
      (.delete f)))
  (info "wiping the pods...")
  (cluster-db/wipe! backend-db test)
  (doseq [release [CLUSTER_NAME CLUSTER2_NAME]]
    (try (helm/uninstall! test {:release release
                                :timeout WIPE_TIMEOUT
                                :ignore-not-found? true})
         (catch Exception e (warn e "Failed to uninstall:" release))))
  (try (cm/wipe! test {})
       (catch Exception e (warn e "Failed to wipe Chaos Mesh"))))

(defn- get-pod-list
  "Get names of running ScalarDB Cluster node pods across all releases."
  [test]
  (->> (k8s/pods test {:selector NODE_SELECTOR})
       :items
       (filter #(= "Running" (get-in % [:status :phase])))
       (map #(get-in % [:metadata :name]))))

(defn- get-logs
  "Collect ScalarDB Cluster pod logs into the test's store directory directly."
  [test]
  (k8s/collect-logs! test {:selector NODE_SELECTOR
                           :output-dir (store/path! test "pods")}))

(defn- find-load-balancer-ip
  [test prefix]
  (->> (k8s/services test {})
       :items
       (filter #(str/includes? (get-in % [:metadata :name]) prefix))
       (filter #(= "LoadBalancer" (get-in % [:spec :type])))
       (keep #(let [ingress (get-in % [:status :loadBalancer :ingress 0])]
                (or (:ip ingress) (:hostname ingress))))
       first))

(defn get-load-balancer-ip
  "Get the external address of a LoadBalancer service whose name includes
  prefix. Returns the ingress IP when present, otherwise the DNS hostname
  (clouds like AWS expose LoadBalancers via a hostname rather than an IP).
  Cloud providers provision the LoadBalancer asynchronously, so poll until an
  address appears rather than returning nil the instant after service creation."
  ([test prefix] (get-load-balancer-ip test prefix TIMEOUT_SEC INTERVAL_SEC))
  ([test prefix timeout-sec interval-sec]
   (or (find-load-balancer-ip test prefix)
       (if (<= timeout-sec 0)
         (throw (ex-info "Timed out waiting for LoadBalancer address"
                         {:prefix prefix}))
         (do
           (info "waiting for LoadBalancer address for" prefix "...")
           (Thread/sleep (* interval-sec 1000))
           (recur test prefix (- timeout-sec interval-sec) interval-sec))))))

(defn get-k8s-node-ip
  "Get the IP of a Kubernetes node used to reach NodePort-exposed backends
  (e.g. Db2, Oracle). Prefer the node's ExternalIP (its public IP) when the node
  reports one, so a control outside the cloud network (e.g. on EKS/GKE) can
  reach the NodePort; fall back to the InternalIP, which is what an in-VPC or
  local (kind) control uses. kind nodes report no ExternalIP, so they keep using
  the InternalIP."
  [test]
  (let [addresses (->> (k8s/nodes test)
                       :items
                       (mapcat #(get-in % [:status :addresses])))
        by-type (fn [t] (->> addresses
                             (filter #(= t (:type %)))
                             (map :address)
                             first))]
    (or (by-type "ExternalIP") (by-type "InternalIP"))))

(defn- running-pods?
  "Check a live node."
  [test]
  (= (count (get-pod-list test))
     (if (need-two-clusters? test) 6 3)))

(defn- cluster-nodes-ready?
  [test]
  (and (running-pods? test)
       (try
         (k8s/wait! test {:resource "pod"
                          :selector NODE_SELECTOR
                          :for "condition=Ready"
                          :timeout "120s"})
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
        (wipe! test backend-db))
      (install! test backend-db)
      (configure! test backend-db)
      (start! test backend-db)
      (wait-for-recovery test))

    (teardown! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! test backend-db)))

    db/Primary
    (primaries [_ test] (:nodes test))
    (setup-primary! [_ _ _])

    db/Kill
    (kill! [_ _test _node])
    (start! [_ _test _node])

    db/Pause
    (pause! [_ _test _node])
    (resume! [_ _test _node])

    db/LogFiles
    (log-files [_ test _]
      ;; Collect pod logs into store/ ourselves and return [] so jepsen's
      ;; snarf-logs! doesn't try to fetch them over the dummy SSH connection.
      (get-logs test)
      [])))

(defrecord ExtCluster [backend-db]
  ext/DbExtension
  (live-nodes [_ test] (running-pods? test))
  (wait-for-recovery [_ test] (wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [create-fn
              (fn [ip]
                (let [client-side-optimizations-enabled (str (:enable-cluster-client-side-optimizations test))]
                  (doto (Properties.)
                    (.setProperty "scalar.db.transaction_manager" "cluster")
                    (.setProperty "scalar.db.contact_points" (str "indirect:" ip))
                    (.setProperty "scalar.db.cluster.client.piggyback_begin.enabled" client-side-optimizations-enabled)
                    (.setProperty "scalar.db.cluster.client.write_buffering.enabled" client-side-optimizations-enabled))))]
          (if (need-two-clusters? test)
            (mapv (comp create-fn #(get-load-balancer-ip test (str % "-envoy")))
                  [CLUSTER_NAME CLUSTER2_NAME])
            (create-fn (get-load-balancer-ip test (str CLUSTER_NAME "-envoy")))))))
  (create-storage-properties [_ test]
    (cluster-db/create-storage-properties backend-db test)))

(def ^:private dbtype->gen-var
  {:postgres  'scalardb.db.cluster-db.postgres/gen-cluster-db
   :alloydb   'scalardb.db.cluster-db.alloydb/gen-cluster-db
   :yugabytedb  'scalardb.db.cluster-db.yugabytedb/gen-cluster-db
   :mysql     'scalardb.db.cluster-db.mysql/gen-cluster-db
   :mariadb   'scalardb.db.cluster-db.mariadb/gen-cluster-db
   :tidb      'scalardb.db.cluster-db.tidb/gen-cluster-db
   :sqlserver 'scalardb.db.cluster-db.sqlserver/gen-cluster-db
   :oracle    'scalardb.db.cluster-db.oracle/gen-cluster-db
   :db2       'scalardb.db.cluster-db.db2/gen-cluster-db
   :managed   'scalardb.db.cluster-db.managed/gen-cluster-db})

(def ^:private managed-db-types
  "DB types whose backend is provisioned outside the test (e.g., AWS Aurora).
  Their constructors receive the test options so they can read the user-supplied
  --managed-db-config YAML file."
  #{:managed})

(defn- cluster-backend-db
  [db-type opts]
  (if-let [v (get dbtype->gen-var db-type)]
    (let [f (requiring-resolve v)]
      (if (managed-db-types db-type) (f opts) (f)))
    (throw (ex-info "Unsupported DB for ScalarDB Cluster test" {:db db-type}))))

(defn gen-db
  [faults admin db-type & [opts]]
  (when (seq admin)
    (warn "The admin operations are ignored: " admin))
  (let [backend-db (cluster-backend-db db-type opts)
        db (ext/extend-db (db backend-db) (->ExtCluster backend-db))
        nemesis (cm/nemesis-package db 60 faults)]
    [db nemesis 1]))
