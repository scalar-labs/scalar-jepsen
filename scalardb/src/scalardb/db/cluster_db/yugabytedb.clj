(ns scalardb.db.cluster-db.yugabytedb
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.control :as c]
            [scalardb.db.cluster :refer [get-load-balancer-ip]]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private ^:const YUGABYTEDB_NAME "yugabytedb-scalardb-cluster")
(def ^:private ^:const YUGABYTEDB_LB_SERVICE "yb-tserver-service")
(def ^:private ^:const YUGABYTEDB_USER "yugabyte")
(def ^:private ^:const YUGABYTEDB_PASSWORD "")

(defrecord ClusterDbYugabyteDb []
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_]
    "jdbc:postgresql://yb-tservers.default.svc.cluster.local:5433/yugabyte")

  (get-username [_] YUGABYTEDB_USER)

  (get-password [_] YUGABYTEDB_PASSWORD)

  (install! [_]
    (c/exec :helm :repo :add "yugabytedb" "https://charts.yugabyte.com"))

  (configure! [_])

  (start! [_]
    (c/exec :helm :install YUGABYTEDB_NAME "yugabytedb/yugabyte"
            :--set "replicas.master=1"
            :--set "replicas.tserver=1"
            :--set "replicas.totalMasters=1"
            :--set "resource.master.requests.cpu=0.5"
            :--set "resource.master.requests.memory=512Mi"
            :--set "resource.master.limits.cpu=0.5"
            :--set "resource.master.limits.memory=512Mi"
            :--set "resource.tserver.requests.cpu=0.5"
            :--set "resource.tserver.requests.memory=1Gi"
            :--set "resource.tserver.limits.cpu=0.5"
            :--set "resource.tserver.limits.memory=1Gi"
            :--set "storage.master.count=1"
            :--set "storage.master.size=1Gi"
            :--set "storage.tserver.count=1"
            :--set "storage.tserver.size=1Gi"
            :--set "enableLoadBalancer=true"
            :--version "2024.2.8"))

  (wipe! [_]
    (doseq [cmd [[:helm :uninstall YUGABYTEDB_NAME
                  :--timeout "3m0s" :--ignore-not-found]
                 [:kubectl :delete :pvc :-l (str "release=" YUGABYTEDB_NAME)
                  "--timeout=180s" "--ignore-not-found=true"]]]
      (try (apply c/exec cmd)
           (catch Exception e (warn e "Failed to exec:" cmd)))))

  (create-storage-properties [_ test]
    (let [node (-> test :nodes first)
          ip (c/on node (get-load-balancer-ip YUGABYTEDB_LB_SERVICE))]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:postgresql://" ip ":5433/yugabyte"))
        (.setProperty "scalar.db.username" YUGABYTEDB_USER)
        (.setProperty "scalar.db.password" YUGABYTEDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbYugabyteDb))
