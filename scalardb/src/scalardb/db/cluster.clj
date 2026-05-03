(ns scalardb.db.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [environ.core :refer [env]]
            [jepsen.db :as db]
            [jepsen.k8s.chaos-mesh.core :as cm]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
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

  (cm/setup! test {}))

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
  "Get the pod list whose name starts with prefix"
  [test prefix]
  (->> (k8s/pods test {})
       :items
       (filter #(str/starts-with? (get-in % [:metadata :name]) prefix))
       (filter #(= "Running" (get-in % [:status :phase])))
       (map #(get-in % [:metadata :name]))))

(defn- get-logs
  [test]
  (let [dir (System/getProperty "user.dir")
        pods (concat (get-pod-list test CLUSTER_NODE_NAME)
                     (get-pod-list test CLUSTER2_NODE_NAME))
        logs (mapv #(str dir "/" % ".log") pods)]
    (mapv #(spit %1 (k8s/pod-logs! test {:pod %2})) logs pods)
    logs))

(defn get-load-balancer-ip
  "Get the external IP of a LoadBalancer service whose name includes prefix"
  [test prefix]
  (->> (k8s/services test {})
       :items
       (filter #(str/includes? (get-in % [:metadata :name]) prefix))
       (filter #(= "LoadBalancer" (get-in % [:spec :type])))
       (map #(get-in % [:status :loadBalancer :ingress 0 :ip]))
       (remove nil?)
       first))

(defn get-k8s-node-ip
  "Get the internal IP of a Kubernetes node"
  [test]
  (->> (k8s/nodes test)
       :items
       (mapcat #(get-in % [:status :addresses]))
       (filter #(= "InternalIP" (:type %)))
       (map :address)
       first))

(defn- running-pods?
  "Check a live node."
  [test]
  (= (count (concat (get-pod-list test CLUSTER_NODE_NAME)
                    (get-pod-list test CLUSTER2_NODE_NAME)))
     (if (need-two-clusters? test) 6 3)))

(defn- cluster-nodes-ready?
  [test]
  (and (running-pods? test)
       (try
         (->> (concat (get-pod-list test CLUSTER_NODE_NAME)
                      (get-pod-list test CLUSTER2_NODE_NAME))
              (mapv #(k8s/wait! test {:resource (str "pod/" %)
                                      :for "condition=Ready"
                                      :timeout "120s"})))
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
      (get-logs test))))

(defrecord ExtCluster [backend-db]
  ext/DbExtension
  (live-nodes [_ test] (running-pods? test))
  (wait-for-recovery [_ test] (wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [ip (get-load-balancer-ip test (str CLUSTER_NAME "-envoy"))
              ip2 (get-load-balancer-ip test (str CLUSTER2_NAME "-envoy"))
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
