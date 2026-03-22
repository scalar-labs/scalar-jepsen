(ns scalardb.db.cluster-db.oracle
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-k8s-node-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const ORACLE_NAME "oracle-scalardb-cluster")
(def ^:private ^:const ORACLE_MANIFEST_YAML "oracle-free-jepsen.yaml")
(def ^:private ^:const ORACLE_USER "system")
(def ^:private ^:const ORACLE_PASSWORD "Str0ng!Pass")

(defrecord ClusterDbOracle []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:oracle:thin:@oracle-scalardb-cluster.default.svc.cluster.local:1521/FREEPDB1")

  (get-username [_] ORACLE_USER)

  (get-password [_] ORACLE_PASSWORD)

  (install! [_]
    (c/upload ORACLE_MANIFEST_YAML "/tmp"))

  (configure! [_]
    (binding [c/*dir* (System/getProperty "user.dir")]
      (try
        (c/exec :kubectl :delete :secret "oracle-scalardb-cluster-secret")
        ;; ignore the failure when the secret doesn't exist
        (catch Exception _))
      (c/exec :kubectl :create :secret :generic "oracle-scalardb-cluster-secret"
              (str "--from-literal=ORACLE_PASSWORD=" ORACLE_PASSWORD))))

  (start! [_]
    (c/exec :kubectl :apply :-f (str "/tmp/" ORACLE_MANIFEST_YAML))
    (c/exec :kubectl :wait
            "--for=condition=Ready"
            "pod"
            :-l (str "app=" ORACLE_NAME)
            "--timeout=300s"))

  (wipe! [_]
    (doseq [cmd [[:kubectl :delete :-f (str "/tmp/" ORACLE_MANIFEST_YAML)
                  "--timeout=180s" "--ignore-not-found=true"]
                 [:kubectl :delete :pvc "data-oracle-scalardb-cluster-0"
                  "--timeout=180s" "--ignore-not-found=true"]]]
      (try (apply c/exec cmd)
           (catch Exception e (warn e "Failed to exec:" cmd)))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-k8s-node-ip))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:oracle:thin:@" ip ":31521/FREEPDB1"))
        (.setProperty "scalar.db.username" ORACLE_USER)
        (.setProperty "scalar.db.password" ORACLE_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbOracle))
