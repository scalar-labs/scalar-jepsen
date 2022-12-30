(ns cassandra.runner
  (:gen-class)
  (:require [clojure.string :as str]
            [cassandra
             [batch      :as batch]
             [conductors :as conductors]
             [core       :as cassandra]
             [counter    :as counter]
             [lwt        :as lwt]
             [nemesis    :as cn]]
            [cassandra.collections.map :as map]
            [cassandra.collections.set :as set]
            [jepsen
             [core    :as jepsen]
             [cli     :as cli]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.nemesis.time :as nt]))

(def link-to-tarball "https://archive.apache.org/dist/cassandra/3.11.6/apache-cassandra-3.11.6-bin.tar.gz")

(def workload-names
  "A map of workload names"
  {"batch"   :batch
   "map"     :map
   "set"     :set
   "counter" :counter
   "lwt"     :lwt})

(def workloads
  "A map of workload to test constructors."
  {:batch   batch/workload
   :map     map/workload
   :set     set/workload
   :counter counter/workload
   :lwt     lwt/workload})

(def nemeses
  {"none"      []
   "flush"     [:flush]
   "partition" [:partition]
   "clock"     [:clock]
   "crash"     [:crash]
   "mix"       [:crash :partition :clock]})

(def joinings
  {"none"         {:name ""                 :bootstrap false :decommission false}
   "bootstrap"    {:name "-bootstrap"       :bootstrap true  :decommission false}
   "decommission" {:name "-decommissioning" :bootstrap false :decommission true}
   "rejoin"       {:name "-rejoining"       :bootstrap true  :decommission true}})

(def clocks
  {"none"   {:name ""              :bump false :strobe false}
   "bump"   {:name "-clock-bump"   :bump true  :strobe false}
   "strobe" {:name "-clock-strobe" :bump false :strobe true}
   "drift"  {:name "-clock-drift"  :bump true  :strobe true}})

(def test-opt-spec
  [(cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-names)])

(def cassandra-opt-spec
  [(cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [[]]
                     nemeses)

   (cli/repeated-opt nil "--join NAME" "Which node joinings to use"
                     [{:name "" :bootstrap false :decommission false}]
                     joinings)

   (cli/repeated-opt nil "--clock NAME" "Which clock-drift to use"
                     [{:name "" :bump false :strobe false}]
                     clocks)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--cassandra-dir CASSANDRA_DIRECTORY" "Cassandra directory on DB node"
    :default "/root/cassandra"]

   (cli/tarball-opt link-to-tarball)])

(defn combine-nemesis
  "Combine nemesis options with bootstrapper and decommissioner"
  [opts nemesis joining clock]
  (-> opts
      (assoc :suffix (str (:name nemesis) (:name joining) (:name clock)))
      (assoc :join joining)
      (assoc :clock clock)
      (assoc :decommissioned
             (if (:bootstrap joining)
               (atom #{(last (:nodes opts))})
               (atom #{})))
      (assoc :nemesis nemesis)))
      ;(assoc :nemesis
      ;       (jn/compose
      ;        (conj {#{:start :stop} (:nemesis (eval nemesis))}
      ;              (when (:decommission joining)
      ;                {#{:decommission} (conductors/decommissioner)})
      ;              (when (:bootstrap joining)
      ;                {#{:bootstrap} (conductors/bootstrapper)})
      ;              (when (or (:bump clock) (:strobe clock))
      ;                {#{:reset :bump :strobe} (nt/clock-nemesis)}))))))

(defn cassandra-test
  [opts]
  (let [workload-key (:workload opts)
        workload ((workload-key workloads) opts)
        nemesis (cn/nemesis-package {:faults    (:nemesis opts)
                                     :partition {:targets [:one
                                                           :primaries
                                                           :majority
                                                           :majorities-ring
                                                           :minority-third]}})]
    (merge tests/noop-test
           opts
           {:name (str "cassandra-" (name workload-key)
                       (when seq (:nemesis opts)
                         (str "-" (str/join "-" (map name (:nemesis opts))))))
            :client (:client workload)
            :db (cassandra/db)
            :pure-generators true
            :generator (gen/phases
                        (->> (:generator workload)
                             gen/mix
                             (gen/nemesis
                              (gen/phases
                               (gen/sleep 5)
                               (:generator nemesis)))
                             (gen/time-limit (:time-limit opts)))
                        (gen/nemesis (:final-generator nemesis))
                        (gen/clients (:final-generator workload)))
            :nemesis (:nemesis nemesis)
            :checker (:checker workload)
            :decommissioned (atom #{})})))

(defn test-cmd
  []
  {"test" {:opt-spec (->> test-opt-spec
                          (into cassandra-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage (cli/test-usage)
           :run (fn [{:keys [options]}]
                  (doseq [_ (range (:test-count options))
                          workload (:workload options)
                          nemesis (:nemesis options)]
                    (let [test (-> options
                                   (assoc :workload workload :nemesis nemesis)
                                   cassandra-test
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (cli/run! (merge (cli/serve-cmd)
                   (test-cmd))
            args))
