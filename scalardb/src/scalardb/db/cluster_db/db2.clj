(ns scalardb.db.cluster-db.db2
  (:require [jepsen.control :as c]
            [scalardb.core :as scalar]
            [scalardb.db.cluster :refer [get-k8s-node-ip WIPE_TIMEOUT]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const DB2_NAME "db2-scalardb-cluster")
(def ^:private ^:const DB2_MANIFEST_YAML "db2-jepsen.yaml")
(def ^:private ^:const DB2_USER "db2inst1")
(def ^:private ^:const DB2_PASSWORD "Str0ng!Pass")

(defrecord ClusterDbDb2 []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    (str "jdbc:db2://db2-scalardb-cluster.default.svc.cluster.local:50000/"
         scalar/KEYSPACE))

  (get-username [_] DB2_USER)

  (get-password [_] DB2_PASSWORD)

  (install! [_]
    (c/upload DB2_MANIFEST_YAML "/tmp"))

  (configure! [_]
    (binding [c/*dir* (System/getProperty "user.dir")]
      (try
        (c/exec :kubectl :delete :secret "db2-scalardb-cluster-secret")
        ;; ignore the failure when the secret doesn't exist
        (catch Exception _))
      (c/exec :kubectl :create :secret :generic "db2-scalardb-cluster-secret"
              (str "--from-literal=DB2INST1_PASSWORD=" DB2_PASSWORD))))

  (start! [_]
    (c/exec :kubectl :apply :-f (str "/tmp/" DB2_MANIFEST_YAML))
    (c/exec :kubectl :wait
            "--for=condition=Ready"
            "pod"
            :-l (str "app=" DB2_NAME)
            "--timeout=600s"))

  (wipe! [_]
    (doseq [cmd [[:kubectl :delete :-f (str "/tmp/" DB2_MANIFEST_YAML)
                  :--timeout WIPE_TIMEOUT "--ignore-not-found=true"]
                 [:kubectl :delete :pvc "data-db2-scalardb-cluster-0"
                  :--timeout WIPE_TIMEOUT "--ignore-not-found=true"]]]
      (try (apply c/exec cmd) (catch Exception _ nil))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-k8s-node-ip))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:db2://" ip ":31500/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" DB2_USER)
        (.setProperty "scalar.db.password" DB2_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbDb2))
