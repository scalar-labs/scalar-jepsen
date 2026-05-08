(ns scalardb.db.cluster-db.yugabytedb
  (:require [clojure.tools.logging :refer [warn]]
            [jepsen.k8s.core :as k8s]
            [jepsen.k8s.helm :as helm]
            [scalardb.db.cluster :refer [get-load-balancer-ip WIPE_TIMEOUT]]
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
    "jdbc:yugabytedb://yb-tservers.default.svc.cluster.local:5433/yugabyte")

  (get-username [_] YUGABYTEDB_USER)

  (get-password [_] YUGABYTEDB_PASSWORD)

  (install! [_ test]
    (helm/repo-add! test "yugabytedb" "https://charts.yugabyte.com"))

  (configure! [_ _])

  (start! [_ test]
    (helm/install! test {:release YUGABYTEDB_NAME
                         :chart "yugabytedb/yugabyte"
                         :version "2024.2.8"
                         :set {:replicas.master 1
                               :replicas.tserver 1
                               :replicas.totalMasters 1
                               :resource.master.requests.cpu 0.5
                               :resource.master.requests.memory "512Mi"
                               :resource.master.limits.cpu 0.5
                               :resource.master.limits.memory "512Mi"
                               :resource.tserver.requests.cpu 0.5
                               :resource.tserver.requests.memory "1Gi"
                               :resource.tserver.limits.cpu 0.5
                               :resource.tserver.limits.memory "1Gi"
                               :storage.master.count 1
                               :storage.master.size "1Gi"
                               :storage.tserver.count 1
                               :storage.tserver.size "1Gi"
                               :enableLoadBalancer true}}))

  (wipe! [_ test]
    (doseq [cmd [#(helm/uninstall! test {:release YUGABYTEDB_NAME
                                         :timeout WIPE_TIMEOUT
                                         :ignore-not-found? true})
                 #(k8s/kubectl! test :delete :pvc :-l (str "release=" YUGABYTEDB_NAME)
                                :--timeout WIPE_TIMEOUT "--ignore-not-found=true")]]
      (try (cmd)
           (catch Exception e (warn e "Failed to exec wipe command")))))

  (create-storage-properties [_ test]
    (let [ip (get-load-balancer-ip test YUGABYTEDB_LB_SERVICE)]
      (doto (Properties.)
        (.setProperty "scalar.db.storage" "jdbc")
        (.setProperty "scalar.db.contact_points"
                      (str "jdbc:yugabytedb://" ip ":5433/yugabyte"))
        (.setProperty "scalar.db.username" YUGABYTEDB_USER)
        (.setProperty "scalar.db.password" YUGABYTEDB_PASSWORD)))))

(defn gen-cluster-db [] (->ClusterDbYugabyteDb))
