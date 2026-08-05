(ns scalardb.db.cluster-test
  (:require [clojure.test :refer [deftest is testing]]
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

(defn- collected-selectors
  "Runs get-logs with a mocked k8s, returning the selectors it collected."
  [backend-db]
  (let [selectors (atom [])]
    (with-redefs [store/path! (fn [_ dir] dir)
                  k8s/collect-logs! (fn [_ opts]
                                      (swap! selectors conj opts))]
      (#'cluster/get-logs {} backend-db))
    @selectors))

(deftest get-logs-test
  (testing "collects the cluster node logs and the backend DB logs"
    (is (= [{:selector "app.kubernetes.io/app=scalardb-cluster"
             :output-dir "pods"}
            {:selector {:app "database"} :output-dir "pods"}]
           (collected-selectors backend-with-logs))))

  (testing "collects only the cluster node logs for a backend without pods"
    (is (= [{:selector "app.kubernetes.io/app=scalardb-cluster"
             :output-dir "pods"}]
           (collected-selectors (Object.))))))

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
