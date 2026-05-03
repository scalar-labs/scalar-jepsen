(ns scalardb.db.cluster-db.db2
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
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

  (install! [_ _])

  (configure! [_ test]
    (try
      (k8s/kubectl! test :delete :secret "db2-scalardb-cluster-secret")
      (catch Exception _))
    (k8s/kubectl! test :create :secret :generic "db2-scalardb-cluster-secret"
                  (str "--from-literal=DB2INST1_PASSWORD=" DB2_PASSWORD)))

  (start! [_ test]
    (k8s/kubectl! test :apply :-f DB2_MANIFEST_YAML)
    (k8s/wait! test {:resource "pod"
                     :selector {"app" DB2_NAME}
                     :for "condition=Ready"
                     :timeout "600s"}))

  (wipe! [_ test]
    (doseq [cmd [#(k8s/kubectl! test :delete :-f DB2_MANIFEST_YAML
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(k8s/kubectl! test :delete :pvc "data-db2-scalardb-cluster-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd) (catch Exception _ nil))))

  (create-storage-properties [_ test]
    (let [ip (get-k8s-node-ip test)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:db2://" ip ":31500/" scalar/KEYSPACE))
        (.setProperty "scalar.db.username" DB2_USER)
        (.setProperty "scalar.db.password" DB2_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbDb2))
