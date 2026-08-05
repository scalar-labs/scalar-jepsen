(ns scalardb.db.cluster-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.core :as k8s]
            [jepsen.store :as store]
            [scalardb.db.cluster :as cluster]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]))

(def file-io-options
  {:volume-path "/var/lib/database"
   :file-path "/var/lib/database/**/*"
   :pod-selector {:app "database"}
   :container-names ["database"]})

(def backend-with-file-options
  (reify
    cluster-db/ClusterDbFileOptions
    (file-io-options [_] file-io-options)))

(def backend-with-logs
  (reify
    cluster-db/ClusterDbLogs
    (log-selector [_] {:app "database"})))

(defn- collected-logs
  "Runs get-logs with a mocked k8s, returning the log requests it made."
  [backend-db]
  (let [requests (atom [])]
    (with-redefs [store/path! (fn [_ & dirs] (str/join "/" dirs))
                  k8s/collect-logs! (fn [_ opts]
                                      (swap! requests conj opts))]
      (#'cluster/get-logs {} backend-db))
    @requests))

(def cluster-logs
  {:selector "app.kubernetes.io/app=scalardb-cluster" :output-dir "pods"})

(def chaos-mesh-logs
  {:namespace "chaos-mesh" :output-dir "pods/chaos-mesh"})

(deftest get-logs-test
  (testing "collects the cluster node, backend DB and Chaos Mesh logs"
    (is (= [cluster-logs
            {:selector {:app "database"} :output-dir "pods"}
            chaos-mesh-logs]
           (collected-logs backend-with-logs))))

  (testing "skips the backend DB logs for a backend without pods"
    (is (= [cluster-logs chaos-mesh-logs]
           (collected-logs (Object.)))))

  (testing "keeps collecting when one of the requests fails"
    (with-redefs [store/path! (fn [_ & dirs] (str/join "/" dirs))
                  k8s/collect-logs! (fn [_ _] (throw (ex-info "boom" {})))]
      (is (nil? (#'cluster/get-logs {} backend-with-logs))))))

(deftest chaos-mesh-values-test
  (testing "chaos-daemon uses the container runtime of the test cluster"
    ;; The chart defaults to Docker, which makes every fault that enters the
    ;; pod's namespaces fail to apply on a containerd cluster.
    (is (= {:set {:chaosDaemon.runtime "containerd"
                  :chaosDaemon.socketPath "/run/containerd/containerd.sock"}}
           @#'cluster/CHAOS_MESH_VALUES))))

(deftest nemesis-options-test
  (testing "adds backend-specific options for the file-io nemesis"
    (is (= {:file-io file-io-options}
           (#'cluster/nemesis-options backend-with-file-options
                                      :postgres
                                      [:file-io]))))

  (testing "does not require file options for other nemeses"
    (is (= {}
           (#'cluster/nemesis-options (Object.) :managed [:kill]))))

  (testing "rejects file I/O faults for a backend without file options"
    (let [exception (try
                      (#'cluster/nemesis-options
                       (Object.) :managed [:file-io])
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (= "Backend does not support the file-io nemesis"
             (ex-message exception)))
      (is (= {:db :managed} (ex-data exception))))))
