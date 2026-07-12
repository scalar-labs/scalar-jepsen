(ns scalardb.db.cluster-db.oracle
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [scalardb.db.cluster :refer [get-k8s-node-ip WIPE_TIMEOUT]]
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

  ;; Oracle is reached via NodePort + node IP, not a LoadBalancer.
  (get-lb-service-name [_] nil)

  (install! [_ _])

  (configure! [_ test]
    (try
      (k8s/kubectl! test :delete :secret "oracle-scalardb-cluster-secret")
      (catch Exception _))
    (k8s/kubectl! test :create :secret :generic "oracle-scalardb-cluster-secret"
                  (str "--from-literal=ORACLE_PASSWORD=" ORACLE_PASSWORD)))

  (start! [_ test]
    (k8s/kubectl! test :apply :-f ORACLE_MANIFEST_YAML)
    (k8s/wait! test {:resource "pod"
                     :selector {"app" ORACLE_NAME}
                     :for "condition=Ready"}))

  (wipe! [_ test]
    (doseq [cmd [#(k8s/kubectl! test :delete :-f ORACLE_MANIFEST_YAML
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(k8s/kubectl! test :delete :pvc "data-oracle-scalardb-cluster-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-k8s-node-ip test)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:oracle:thin:@" ip ":31521/FREEPDB1"))
        (.setProperty "scalar.db.username" ORACLE_USER)
        (.setProperty "scalar.db.password" ORACLE_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbOracle))
