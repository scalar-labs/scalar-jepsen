(ns scalardb.nemesis.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [error info]]
            [jepsen
             [control :as c]
             [nemesis :as n]
             [net :as net]]
            [jepsen.nemesis.combined :as jn]
            [jepsen.net :as net])
  (:import (java.io File)))

(def ^:private ^:const POD_FAULT_YAML "./pod-fault.yaml")
(def ^:private ^:const PARTITION_YAML "./partition.yaml")
(def ^:private ^:const PACKET_FAULT_YAML "./packet-fault.yaml")

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

(defn- apply-partition-exp
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

(defn- apply-packet-fault-exp
  [targets behaviour]
  (binding [c/*dir* (System/getProperty "user.dir")]
    (let [[action params] (first behaviour)
          get-percent-fn #(-> % :percent name (str/replace "%" ""))
          get-correlation-fn #(-> % :correlation name (str/replace "%" ""))
          base {:apiVersion "chaos-mesh.org/v1alpha1"
                :kind "NetworkChaos"
                :metadata {:name action :namespace "chaos-mesh"}
                :spec {:action action
                       :mode "all"
                       :selector {:pods {"default" targets}}}}
          fault-spec (case action
                       :delay {:latency (-> params :time name)
                               :jitter (-> params :jitter name)
                               :correlation (get-percent-fn params)}
                       :loss {:loss (get-percent-fn params)
                              :correlation (get-correlation-fn params)}
                       :corrupt {:corrupt (get-percent-fn params)
                                 :correlation (get-correlation-fn params)}
                       :duplicate {:duplicate (get-percent-fn params)
                                   :correlation (get-correlation-fn params)}
                       ;; TODO: check how to enable this fault
                       ;:reorder {:reorder (get-percent-fn params)
                       ;          :correlation (get-correlation-fn params)}
                       :rate {:rate (-> params :rate name)})]
      (->> (assoc-in base [:spec action] fault-spec)
           yaml/generate-string
           (spit PACKET_FAULT_YAML))
      (info "DEBUG:" (slurp PACKET_FAULT_YAML))
      (c/exec :kubectl :apply :-f PACKET_FAULT_YAML))))

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

(defn delete-packet-fault-exp
  []
  (delete-chaos-exp PACKET_FAULT_YAML))

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
  "Replace partition-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/partition-package opts)
         :nemesis (jn/partition-nemesis (:db opts) (partitioner))))

(defn- packet-nemesis
  "A nemesis to disrupt packets with Chaos Mesh."
  []
  (reify
    n/Reflection
    (fs [_this]
      [:start-packet  :stop-packet])

    n/Nemesis
    (setup! [this test]
      (c/on (-> test :nodes first) delete-packet-fault-exp)
      this)

    (invoke! [_ test {:keys [f value] :as op}]
      (c/on (-> test :nodes first)
            (let [result (case f
                           :start-packet (let [[_ behavior] value
                                               targets (->> (get-pod-list)
                                                            shuffle
                                                            (take (inc (rand-int 7))))]
                                           (apply-packet-fault-exp targets behavior))
                           :stop-packet  (delete-packet-fault-exp))]
              (assoc op :value result))))

    (teardown! [_ test]
      (c/on (-> test :nodes first) delete-packet-fault-exp))))

(defn- packet-package
  "Replace packet-nemesis for Chaos Mesh."
  [opts]
  (assoc (jn/packet-package opts)
         :nemesis (packet-nemesis)))

(defn nemesis-package
  "Nemeses for ScalarDB cluster"
  [db interval faults]
  (let [opts {:db db
              :interval interval
              :faults (set faults)
              :partition {:targets [:one]}
              :packet {:targets [:one]
                       :behaviors (reduce (fn [acc [k v]] (conj acc {k v}))
                                          []
                                          net/all-packet-behaviors)}
              :kill {:targets [:one]}
              :pause {:targets [:one]}}]
    (jn/compose-packages [(partition-package opts)
                          (packet-package opts)
                          (jn/db-package opts)])))
