(ns scalardb.nemesis.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [error info]]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as n]]
            [jepsen.nemesis.combined :as jn])
  (:import (java.io File)))

(def ^:private ^:const POD_FAULT_YAML "./pod-fault.yaml")
(def ^:private ^:const PARTITION_YAML "./partition.yaml")

(defn get-pod-list
  "Get all pods."
  []
  (->> (c/exec :kubectl :get :pod)
       str/split-lines
       (filter #(str/includes? % "scalardb"))
       (map #(-> % (str/split #"\s+") first))))

(defn apply-pod-fault-exp
  [pod-fault]
  (if (contains? #{:pause :kill} pod-fault)
    (binding [c/*dir* (System/getProperty "user.dir")]
      (let [targets (->> (get-pod-list)
                         ;; choose envoy or cluster nodes
                         (filter #(str/starts-with? % "scalardb-"))
                         shuffle
                         (take (inc (rand-int 3))))
            action (case pod-fault
                     :pause "pod-failure"
                     :kill "pod-kill")
            base-spec {:action action
                       :mode "all"
                       :selector {:pods {"default" targets}}}
            spec (if (= pod-fault :pause)
                   (assoc base-spec :duration "60s")
                   base-spec)]
        (info "Try" action "nodes:" targets)
        (->> (yaml/generate-string
              {:apiVersion "chaos-mesh.org/v1alpha1"
               :kind "PodChaos"
               :metadata {:name action :namespace "chaos-mesh"}
               :spec spec})
             (spit POD_FAULT_YAML))
        (info "DEBUG:" (slurp POD_FAULT_YAML))
        (c/exec :kubectl :apply :-f POD_FAULT_YAML)
        targets))
    (error "Unexpected pod-fault type")))

(defn apply-partition-exp
  [grudge]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [remain (->> (get-pod-list) (remove (set grudge)))]
      (->> (yaml/generate-string
            {:apiVersion "chaos-mesh.org/v1alpha1"
             :kind "NetworkChaos"
             :metadata {:name "partition" :namespace "chaos-mesh"}
             :spec {:action "partition"
                    :mode "all"
                    :selector {:pods {"default" remain}}
                    :direction "both"
                    :target {:mode "all"
                             :selector {:pods {"default" grudge}}}}})
           (spit PARTITION_YAML))
      (info "DEBUG:" (slurp PARTITION_YAML))
      (c/exec :kubectl :apply :-f PARTITION_YAML))))

(defn- delete-chaos-exp
  "Delete Chaos experiment if it exists."
  [yaml-path]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [file (File. yaml-path)]
      (when (.exists file)
        (c/exec :kubectl :delete :-f yaml-path)
        (.delete file)))))

(defn delete-pod-fault-exp
  []
  (delete-chaos-exp POD_FAULT_YAML))

(defn delete-partition-exp
  []
  (delete-chaos-exp PARTITION_YAML))

(defn- partitioner
  "Partitioner for Chaos Mesh."
  []
  (reify n/Nemesis
    (setup! [this test]
      (c/on (-> test :nodes first) delete-partition-exp)
      this)

    (invoke! [_ test op]
      (case (:f op)
        :start (c/on (-> test :nodes first)
                     (let [grudge (->> (get-pod-list)
                                       shuffle
                                       (take (inc (rand-int 3))))]
                       (apply-partition-exp grudge)
                       (assoc op :value [:isolated grudge])))
        :stop  (do (c/on (-> test :nodes first) (delete-partition-exp))
                   (assoc op :value :network-healed))))

    (teardown! [_ _]
      (c/on (-> test :nodes first) delete-partition-exp))))

(defn- partition-package
  "Almost copied from jepsen.nemesis.combined/partition-package.
  Replace partition-nemesis for Chaos Mesh."
  [opts]
  (let [needed? ((:faults opts) :partition)
        db      (:db opts)
        start (fn start [_ _]
                {:type  :info
                 :f     :start-partition
                 ;; Target the k8s host. Pods will be choosen.
                 :value :one})
        stop  {:type :info, :f :stop-partition, :value nil}
        gen   (->> (gen/flip-flop start (repeat stop))
                   (gen/stagger (:interval opts jn/default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (jn/partition-nemesis db (partitioner))
     :perf            #{{:name  "partition"
                         :start #{:start-partition}
                         :stop  #{:stop-partition}
                         :color "#E9DCA0"}}}))

(defn nemesis-package
  "Nemeses for ScalarDB cluster"
  [db interval faults]
  (let [opts {:db db
              :interval interval
              :faults (set faults)
              :partition {:targets [:one]}
              :kill {:targets [:one]}
              :pause {:targets [:one]}}]
    (jn/compose-packages [(partition-package opts)
                          (jn/db-package opts)])))
