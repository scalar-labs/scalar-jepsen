(ns scalardb.db.cluster-db.alloydb
  (:require [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const CERT_MANAGER_NAMESPACE "cert-manager")
(def ^:private ^:const ALLOYDB_OPERATOR_NAMESPACE "alloydb-omni-system")
(def ^:private ^:const ALLOYDB_OPERATOR_NAME "alloydbomni-operator")
(def ^:private ^:const ALLOYDB_OPERATOR_VERSION "1.6.2")
(def ^:private ^:const ALLOYDB_MANIFEST_YAML "alloydb-cluster.yaml")
(def ^:private ^:const ALLOYDB_NAME "alloydb-scalardb-cluster")
(def ^:private ^:const ALLOYDB_USER "postgres")
(def ^:private ^:const ALLOYDB_PASSWORD "postgres")

(defrecord ClusterDbAlloyDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:postgresql://al-alloydb-scalardb-cluster-rw-elb.default.svc.cluster.local:5432/postgres")

  (get-username [_] ALLOYDB_USER)

  (get-password [_] ALLOYDB_PASSWORD)

  (install! [_]
    (binding [c/*dir* (System/getProperty "user.dir")]
      (c/upload ALLOYDB_MANIFEST_YAML "/tmp")
      ;; set up cert-manager for AlloyDB Operator
      (c/exec :helm :repo :add "jetstack" "https://charts.jetstack.io")
      (c/exec :helm :install "cert-manager" "jetstack/cert-manager"
              :--namespace CERT_MANAGER_NAMESPACE :--create-namespace
              :--set "installCRDs=true" :--version "v1.19.4")
      ;; set up operator
      (c/exec :curl :-O
              (str "https://storage.googleapis.com/alloydb-omni-operator/"
                   ALLOYDB_OPERATOR_VERSION
                   "/alloydbomni-operator-" ALLOYDB_OPERATOR_VERSION ".tgz"))
      (c/exec :helm :install ALLOYDB_OPERATOR_NAME
              (str "alloydbomni-operator-" ALLOYDB_OPERATOR_VERSION ".tgz")
              :--namespace ALLOYDB_OPERATOR_NAMESPACE :--create-namespace
              :--atomic :--timeout "5m")))

  (configure! [_]
    (try
      (c/exec :kubectl :delete :secret "db-pw-alloydb-scalardb-cluster")
        ;; ignore the failure when the secret doesn't exist
      (catch Exception _))
    (c/exec :kubectl :create :secret :generic "db-pw-alloydb-scalardb-cluster"
            (str "--from-literal=alloydb-scalardb-cluster=" ALLOYDB_PASSWORD)))

  (start! [_]
    (c/exec :kubectl :apply :-f (str "/tmp/" ALLOYDB_MANIFEST_YAML))
    (c/exec :kubectl :wait
            "--for=condition=Provisioned"
            (str "dbcluster/" ALLOYDB_NAME)
            "--timeout=300s"))

  (wipe! [_]
    (binding [c/*dir* (System/getProperty "user.dir")]
      (doseq [cmd [[:kubectl :delete :-f (str "/tmp/" ALLOYDB_MANIFEST_YAML)]
                   [:helm :uninstall "cert-manager"
                    :--namespace CERT_MANAGER_NAMESPACE]
                   [:kubectl :delete :namespace CERT_MANAGER_NAMESPACE]
                   [:helm :uninstall ALLOYDB_OPERATOR_NAME
                    :--namespace ALLOYDB_OPERATOR_NAMESPACE]
                   [:kubectl :delete :namespace ALLOYDB_OPERATOR_NAMESPACE]
                   [:rm :-f "alloydbomni-operator-*.tgz"]]]
        (try (apply c/exec cmd) (catch Exception _ nil)))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip (str "al-" ALLOYDB_NAME)))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5432/postgres"))
        (.setProperty "scalar.db.username" ALLOYDB_USER)
        (.setProperty "scalar.db.password" ALLOYDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbAlloyDb))
