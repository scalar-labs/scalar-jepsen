(ns scalardb.db.cluster-db.tidb
  (:require [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const TIDB_NAME "tidb-scalardb-cluster-tidb")
(def ^:private ^:const TIDB_OPERATOR_NAME "tidb-operator")
(def ^:private ^:const TIDB_USER "root")
(def ^:private ^:const TIDB_PASSWORD "") ;; empty
(def ^:private ^:const TIDB_CRD_URL "https://raw.githubusercontent.com/pingcap/tidb-operator/v1.6.5/manifests/crd.yaml")
(def ^:private ^:const TIDB_MANIFEST_YAML "tidb-cluster.yaml")

(defrecord ClusterDbTiDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:mysql://tidb-scalardb-cluster-tidb.default.svc.cluster.local:4000/mysql")

  (get-username [_] TIDB_USER)

  (get-password [_] TIDB_PASSWORD)

  (install! [_]
    (c/exec :helm :repo :add "pingcap" "https://charts.pingcap.org/")
    (c/exec :kubectl :create :namespace "tidb-admin")
    (c/exec :kubectl :create :-f TIDB_CRD_URL)
    (c/exec :helm :install
            TIDB_OPERATOR_NAME "pingcap/tidb-operator"
            :--namespace "tidb-admin"
            :--version "v1.6.5")
    (c/upload TIDB_MANIFEST_YAML "/tmp"))

  (configure! [_])

  (start! [_]
    (c/exec :kubectl :apply :-f (str "/tmp/" TIDB_MANIFEST_YAML)))

  (wipe! [_]
    (doseq [cmd [[:kubectl :delete :-f (str "/tmp/" TIDB_MANIFEST_YAML)]
                 [:kubectl :delete :pvc "pd-tidb-scalardb-cluster-pd-0"]
                 [:kubectl :delete :pvc "tikv-tidb-scalardb-cluster-tikv-0"]
                 [:helm :uninstall TIDB_OPERATOR_NAME :--namespace "tidb-admin"]
                 [:kubectl :delete :namespace "tidb-admin"]
                 [:kubectl :delete :-f TIDB_CRD_URL]]]
      (try (apply c/exec cmd) (catch Exception _ nil))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip TIDB_NAME))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:mysql://" ip ":4000/mysql"))
        (.setProperty "scalar.db.username" TIDB_USER)
        (.setProperty "scalar.db.password" TIDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbTiDb))
