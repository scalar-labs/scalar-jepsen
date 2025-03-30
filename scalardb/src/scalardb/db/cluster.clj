(ns scalardb.db.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [error info]]
            [jepsen
             [control :as c]
             [db :as db]]
            [jepsen.nemesis.combined :as jn])
  (:import (java.io File)))

(def ^:private ^:const DEFAULT_VERSION "3.14.0")
(def ^:private ^:const CLUSTER_VALUES_YAML "scalardb-cluster-custom-values.yaml")
(def ^:private ^:const DEFAULT_CHAOS_MESH_VERSION "2.7.1")

(def ^:private ^:const TIMEOUT_SEC 600)
(def ^:private ^:const INTERVAL_SEC 10)

(def ^:private ^:const CLUSTER_NODE_NAME "scalardb-cluster-node")

(def ^:private ^:const POD_FAULT_YAML "./pod-fault.yaml")

(defn- install!
  "Install prerequisites. You should already have installed minikube, kubectl and helm."
  []
  ;; postgre
  (c/exec :helm :repo :add "bitnami" "https://charts.bitnami.com/bitnami")
  ;; ScalarDB cluster
  (c/exec :helm :repo :add
          "scalar-labs" "https://scalar-labs.github.io/helm-charts")
  ;; Chaos mesh
  (c/exec :helm :repo :add "chaos-mesh" "https://charts.chaos-mesh.org"))

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
            (str "--docker-password=" (:docker-access-token test)))
    (try
      (c/exec :kubectl :delete :secret "scalardb-credentials-secret")
      ;; ignore the failure when the secret doesn't exist
      (catch Exception _))
    (c/exec :kubectl :create :secret :generic "scalardb-credentials-secret"
            "--from-literal=SCALAR_DB_CLUSTER_POSTGRES_USERNAME=postgres"
            "--from-literal=SCALAR_DB_CLUSTER_POSTGRES_PASSWORD=postgres"
            :-n "default"))

  ;; Chaos Mesh
  (try
    (c/exec :kubectl :get :namespaces "chaos-mesh")
    (catch Exception _
      (c/exec :kubectl :create :ns "chaos-mesh"))))

(defn- start!
  []
  ;; postgre
  (c/exec
   :helm :install "postgresql-scalardb-cluster" "bitnami/postgresql"
   :--set "auth.postgresPassword=postgres"
   :--set "primary.persistence.enabled=false"
   ;; Need an external IP for storage APIs
   :--set "service.type=LoadBalancer"
   :--set "primary.service.type=LoadBalancer")

  ;; ScalarDB cluster
  (let [chart-version (->> (c/exec :helm :search
                                   :repo "scalar-labs/scalardb-cluster" :-l)
                           str/split-lines
                           (filter #(str/includes? % DEFAULT_VERSION))
                           (map #(nth (str/split % #"\s+") 1))
                           (sort #(compare %2 %1))
                           first)]
    (info "helm chart version:" chart-version)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (c/exec :helm :install "scalardb-cluster" "scalar-labs/scalardb-cluster"
              :-f CLUSTER_VALUES_YAML
              :--version chart-version
              :-n "default")))

  ;; Chaos mesh
  (c/exec :helm :install "chaos-mesh" "chaos-mesh/chaos-mesh"
          :-n "chaos-mesh"
          :--version DEFAULT_CHAOS_MESH_VERSION))

(defn- wipe!
  []
  (try
    (info "wiping old logs...")
    (binding [c/*dir* (System/getProperty "user.dir")]
      (some->> (-> (c/exec :ls) (str/split #"\s+"))
               (filter #(re-matches #"scalardb-cluster-node-.*\.log" %))
               seq
               (apply c/exec :rm :-f)))
    (info "wiping the pods...")
    (c/exec :helm :uninstall :scalardb-cluster :postgresql-scalardb-cluster)
    (c/exec :helm :uninstall :chaos-mesh :-n "chaos-mesh")
    (catch Exception _ nil)))

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
  "Get the IP of the load balancer"
  []
  (->> (c/exec :kubectl :get :svc)
       str/split-lines
       (filter #(str/includes? % "postgresql-scalardb-cluster"))
       (filter #(str/includes? % "LoadBalancer"))
       (map #(nth (str/split % #"\s+") 3))
       first))

(defn running-pods?
  "Check a live node."
  [_test]
  (-> test
      :nodes
      first
      (c/on (get-pod-list CLUSTER_NODE_NAME))
      count
      ;; TODO: check the number of pods
      (= 3)))

(defn wait-for-recovery
  "Wait for the node bootstrapping."
  ([test]
   (wait-for-recovery TIMEOUT_SEC INTERVAL_SEC test))
  ([timeout-sec interval-sec test]
   (when (not (running-pods? test))
     (Thread/sleep (* interval-sec 1000))
     (if (>= timeout-sec interval-sec)
       (wait-for-recovery (- timeout-sec interval-sec) interval-sec test)
       (throw (ex-info "Timed out waiting for pods"
                       {:causes "Some pod couldn't start"}))))))

(defn- apply-pod-fault-exp
  [pod-fault]
  (if (contains? #{:pause :kill} pod-fault)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (let [targets (->> (get-pod-list CLUSTER_NODE_NAME)
                         shuffle
                         (take (inc (rand-int 3))))
            action (case pod-fault
                     :pause "pod-failure"
                     :kill "pod-kill")
            base-spec {:action action
                       :mode "all"
                       :selector {:pods {"default" targets}}}
            spec (if (= pod-fault :pause)
                   (assoc base-spec :duration "60s")
                   base-spec)]
        (info "Try" action "nodes:" targets)
        (->> (yaml/generate-string
              {:apiVersion "chaos-mesh.org/v1alpha1"
               :kind "PodChaos"
               :metadata {:name action :namespace "chaos-mesh"}
               :spec spec})
             (spit POD_FAULT_YAML))
        (info "DEBUG:" (slurp POD_FAULT_YAML))
        (c/exec :kubectl :apply :-f POD_FAULT_YAML)
        targets))
    (error "Unexpected pod-fault type")))

(defn- delete-chaos-exp
  "Delete Chaos experiment if it exists."
  [yaml-path]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [file (File. yaml-path)]
      (when (.exists file)
        (c/exec :kubectl :delete :-f yaml-path)
        (.delete file)))))

(defn db
  "Setup ScalarDB cluster."
  []
  (reify
    db/DB
    (setup! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe!))
      (install!)
      (configure! test)
      (start!)
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
      (apply-pod-fault-exp :pause))
    (resume! [_ _ _]
      (delete-chaos-exp POD_FAULT_YAML))

    db/Kill
    (start! [_ _ _]
      (delete-chaos-exp POD_FAULT_YAML))
    (kill! [_ _ _]
      (apply-pod-fault-exp :kill))

    db/LogFiles
    (log-files [_ test _]
      (get-logs test))))

(defn nemesis-package
  "Nemeses for ScalarDB cluster"
  [db interval faults]
  (let [opts {:db db
              :interval interval
              :faults (set faults)
              :partition {:targets [:one]}
              :kill {:targets [:one]}
              :pause {:targets [:one]}}]
    (jn/compose-packages [(jn/db-package opts)])))
