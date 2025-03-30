(ns scalardb.nemesis.cluster
  (:require [clj-yaml.core :as yaml]
            [clojure.string :as str]
            [clojure.tools.logging :refer [error info]]
            [jepsen.control :as c]
            [jepsen.nemesis.combined :as jn])
  (:import (java.io File)))

(def ^:private ^:const POD_FAULT_YAML "./pod-fault.yaml")

(defn get-pod-list
  "Get all pods"
  []
  (->> (c/exec :kubectl :get :pod)
       str/split-lines
       (map #(first (str/split % #"\s+")))))

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

(defn nemesis-package
  "Nemeses for ScalarDB cluster"
  [db interval faults]
  (let [opts {:db db
              :interval interval
              :faults (set faults)
              :partition {:targets [:one]}
              :kill {:targets [:one]}
              :pause {:targets [:one]}}]
    (jn/compose-packages [(jn/db-package opts)])))
