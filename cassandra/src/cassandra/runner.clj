(ns cassandra.runner
  (:gen-class)
  (:require [clojure.string :as str]
            [cassandra
             [batch      :as batch]
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
             [tests :as tests]]))

(def link-to-tarball "https://archive.apache.org/dist/cassandra/3.11.14/apache-cassandra-3.11.14-bin.tar.gz")

(def workload-keys
  "A map of workload keys"
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
   "partition" [:partition]
   "clock"     [:clock]
   "crash"     [:crash]})

(def admin
  {"none" []
   "join" [:join]
   "flush" [:flush-compact]})

(def test-opt-spec
  [(cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-keys)])

(def cassandra-opt-spec
  [(cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [[]]
                     nemeses)

   (cli/repeated-opt nil "--admin NAME" "Which admin operations to use"
                     [[]]
                     admin)

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--cassandra-dir CASSANDRA_DIRECTORY" "Cassandra directory on DB node"
    :default "/root/cassandra"]

   (cli/tarball-opt link-to-tarball)])

(defn cassandra-test
  [opts]
  (let [target (:target opts)
        workload-key (:workload opts)
        workload ((workload-key workloads) opts)
        faults (:nemesis opts)
        admin (:admin opts)
        db (cassandra/db)
        nemesis (cn/nemesis-package {:db db
                                     :faults faults
                                     :admin admin
                                     :partition {:targets [:one
                                                           :primaries
                                                           :majority
                                                           :majorities-ring
                                                           :minority-third]}})]
    (merge tests/noop-test
           opts
           {:name (-> [target (name workload-key)]
                      (into (map name faults))
                      (into (map name admin))
                      (->> (remove nil?) (str/join "-")))
            :client (:client workload)
            :db db
            :pure-generators true
            :generator (gen/phases
                        (->> (:generator workload)
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
                          nemesis (:nemesis options)
                          admin (:admin options)]
                    (let [test (-> options
                                   (assoc :target "cassandra"
                                          :workload workload
                                          :nemesis nemesis
                                          :admin admin)
                                   cassandra-test
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (cli/run! (merge (cli/serve-cmd)
                   (test-cmd))
            args))
