(ns scalardb.db.cluster-db.tidb
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const TIDB_NAME "tidb-scalardb-cluster-tidb")
(def ^:private ^:const TIDB_OPERATOR_NAME "tidb-operator")
(def ^:private ^:const TIDB_USER "root")
(def ^:private ^:const TIDB_PASSWORD "")
(def ^:private ^:const TIDB_CRD_URL "https://raw.githubusercontent.com/pingcap/tidb-operator/v1.6.5/manifests/crd.yaml")
(def ^:private ^:const TIDB_MANIFEST_YAML "tidb-cluster.yaml")

(defrecord ClusterDbTiDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:mysql://tidb-scalardb-cluster-tidb.default.svc.cluster.local:4000/mysql")

  (get-username [_] TIDB_USER)

  (get-password [_] TIDB_PASSWORD)

  (install! [_ test]
    (helm/repo-add! test "pingcap" "https://charts.pingcap.org/")
    (k8s/kubectl! test :create :namespace "tidb-admin")
    (k8s/kubectl! test :create :-f TIDB_CRD_URL)
    (helm/install! test {:release TIDB_OPERATOR_NAME
                         :chart "pingcap/tidb-operator"
                         :namespace "tidb-admin"
                         :version "v1.6.5"}))

  (configure! [_ _])

  (start! [_ test]
    (k8s/kubectl! test :apply :-f TIDB_MANIFEST_YAML))

  (wipe! [_ test]
    (doseq [cmd [#(k8s/kubectl! test :delete :-f TIDB_MANIFEST_YAML
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(k8s/kubectl! test :delete :pvc "pd-tidb-scalardb-cluster-pd-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(k8s/kubectl! test :delete :pvc "tikv-tidb-scalardb-cluster-tikv-0"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(helm/uninstall! test {:release TIDB_OPERATOR_NAME
                                        :namespace "tidb-admin"
                                        :timeout WIPE_TIMEOUT
                                        :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :namespace "tidb-admin"
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")
                 #(k8s/kubectl! test :delete :-f TIDB_CRD_URL
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test TIDB_NAME)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mysql://" ip ":4000/mysql"))
        (.setProperty "scalar.db.username" TIDB_USER)
        (.setProperty "scalar.db.password" TIDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbTiDb))
