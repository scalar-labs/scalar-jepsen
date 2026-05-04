(ns scalardb.db.cluster-db.alloydb
  (:require [clojure.java.shell :refer [sh]]
            [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.io File)
           (java.util Properties)))

(def ^:private ^:const CERT_MANAGER_NAMESPACE "cert-manager")
(def ^:private ^:const CERT_MANAGER_VERSION "v1.19.4")

(def ^:private ^:const ALLOYDB_OPERATOR_NAMESPACE "alloydb-omni-system")
(def ^:private ^:const ALLOYDB_OPERATOR_NAME "alloydbomni-operator")
(def ^:private ^:const ALLOYDB_OPERATOR_VERSION "1.6.2")
(def ^:private ^:const ALLOYDB_MANIFEST_YAML "alloydb-cluster.yaml")
(def ^:private ^:const ALLOYDB_NAME "alloydb-scalardb-cluster")
(def ^:private ^:const ALLOYDB_USER "postgres")
(def ^:private ^:const ALLOYDB_PASSWORD "postgres")

(defn- operator-tgz []
  (str "alloydbomni-operator-" ALLOYDB_OPERATOR_VERSION ".tgz"))

(defrecord ClusterDbAlloyDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:postgresql://al-alloydb-scalardb-cluster-rw-elb.default.svc.cluster.local:5432/postgres")

  (get-username [_] ALLOYDB_USER)

  (get-password [_] ALLOYDB_PASSWORD)

  (install! [_ test]
    (helm/repo-add! test "jetstack" "https://charts.jetstack.io")
    (helm/repo-update! test)
    (helm/install! test {:release "cert-manager"
                         :chart "jetstack/cert-manager"
                         :namespace CERT_MANAGER_NAMESPACE
                         :version CERT_MANAGER_VERSION
                         :set {:installCRDs true}})
    (let [url (str "https://storage.googleapis.com/alloydb-omni-operator/"
                   ALLOYDB_OPERATOR_VERSION "/" (operator-tgz))
          {:keys [exit err]} (sh "curl" "-O" url)]
      (when-not (zero? exit)
        (throw (ex-info "curl failed" {:url url :exit exit :err err}))))
    (helm/install! test {:release ALLOYDB_OPERATOR_NAME
                         :chart (operator-tgz)
                         :namespace ALLOYDB_OPERATOR_NAMESPACE
                         :timeout "5m"}))

  (configure! [_ test]
    (try
      (k8s/kubectl! test :delete :secret (str "db-pw-" ALLOYDB_NAME))
      (catch Exception _))
    (k8s/kubectl! test :create :secret :generic (str "db-pw-" ALLOYDB_NAME)
                  (str "--from-literal=" ALLOYDB_NAME \= ALLOYDB_PASSWORD)))

  (start! [_ test]
    (k8s/kubectl! test :apply :-f ALLOYDB_MANIFEST_YAML)
    (k8s/wait! test {:resource (str "dbcluster/" ALLOYDB_NAME)
                     :for "condition=Provisioned"}))

  (wipe! [_ test]
    (doseq [cmd [#(k8s/patch! test "dbcluster" ALLOYDB_NAME
                              "{\"spec\":{\"isDeleted\":true}}")
                 #(k8s/kubectl! test :delete :-f ALLOYDB_MANIFEST_YAML
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(helm/uninstall! test {:release "cert-manager"
                                        :namespace CERT_MANAGER_NAMESPACE
                                        :timeout WIPE_TIMEOUT
                                        :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :namespace CERT_MANAGER_NAMESPACE
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(helm/uninstall! test {:release ALLOYDB_OPERATOR_NAME
                                        :namespace ALLOYDB_OPERATOR_NAMESPACE
                                        :timeout WIPE_TIMEOUT
                                        :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :namespace ALLOYDB_OPERATOR_NAMESPACE
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(.delete (File. (operator-tgz)))]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test (str "al-" ALLOYDB_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5432/postgres"))
        (.setProperty "scalar.db.username" ALLOYDB_USER)
        (.setProperty "scalar.db.password" ALLOYDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbAlloyDb))
