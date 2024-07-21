(ns scalardb.runner
  (:gen-class)
  (:require [cassandra.core :as cassandra]
            [cassandra.nemesis :as cn]
            [cassandra.runner :as cr]
            [clojure.tools.logging :refer [warn]]
            [clojure.string :as string]
            [jepsen
             [core :as jepsen]
             [cli :as cli]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.nemesis [combined :as jn]]
            [scalardb
             [core :refer [INITIAL_TABLE_ID]]
             [transfer]
             [transfer-append]
             [elle-append]
             [elle-write-read]
             [transfer-2pc]
             [transfer-append-2pc]
             [elle-append-2pc]
             [elle-write-read-2pc]
             [db-extend :refer [extend-db]]]
            [scalardb.db
             [postgres :as postgres]]))

(def db-keys
  "The map of test DBs."
  {"cassandra" :cassandra
   "postgres" :postgres})

(defn- gen-db
  "Returns [extended-db constructed-nemesis num-max-nodes]."
  [db-key faults admin]
  (case db-key
    :cassandra (let [db (extend-db (cassandra/db) :cassandra)
                     ;; replace :kill nemesis with :crash for Cassandra
                     faults (mapv #(if (= % :kill) :crash %) faults)]
                 (when-not (every? #(some? (get cr/nemeses (name %))) faults)
                   (throw
                    (ex-info
                     (str "Invalid nemesis for Cassandra: " faults) {})))
                 [db
                  (cn/nemesis-package
                   {:db db
                    :faults faults
                    :admin admin
                    :partition {:targets [:one
                                          :primaries
                                          :majority
                                          :majorities-ring
                                          :minority-third]}})
                  Integer/MAX_VALUE])
    :postgres (let [db (extend-db (postgres/db) :postgres)]
                (when (seq admin)
                  (warn "The admin operations are ignored:" admin))
                [db
                 (jn/nemesis-package
                  {:db db
                   :interval 60
                   :faults faults
                   :partition {:targets [:one]}
                   :kill {:targets [:one]}
                   :pause {:targets [:one]}})
                 1])
    (throw (ex-info "Unsupported DB" {:db db-key}))))

(def workload-keys
  "A map of test workload keys."
  {"transfer"            :transfer
   "transfer-append"     :transfer-append
   "elle-append"         :elle-append
   "elle-write-read"     :elle-write-read
   "transfer-2pc"        :transfer-2pc
   "transfer-append-2pc" :transfer-append-2pc
   "elle-append-2pc"     :elle-append-2pc
   "elle-write-read-2pc" :elle-write-read-2pc})

(def workloads
  "A map of workload to test constructors."
  {:transfer            scalardb.transfer/workload
   :transfer-append     scalardb.transfer-append/workload
   :elle-append         scalardb.elle-append/workload
   :elle-write-read     scalardb.elle-write-read/workload
   :transfer-2pc        scalardb.transfer-2pc/workload
   :transfer-append-2pc scalardb.transfer-append-2pc/workload
   :elle-append-2pc     scalardb.elle-append-2pc/workload
   :elle-write-read-2pc scalardb.elle-write-read-2pc/workload})

(def nemeses
  "A map of nemeses."
  {"none" []
   "partition" [:partition]
   "packet"    [:packet]
   "clock"     [:clock]
   "crash"     [:kill]
   "pause"     [:pause]})

(def test-opt-spec
  [(cli/repeated-opt nil "--db NAME" "DB(s) on which the test is run"
                     [:cassandra] db-keys)

   (cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-keys)

   (cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [[]]
                     nemeses)

   [nil "--isolation-level ISOLATION_LEVEL" "isolation level"
    :default :snapshot
    :parse-fn keyword
    :validate [#{:snapshot :serializable}
               "Should be one of snapshot or serializable"]]

   [nil "--serializable-strategy SERIALIZABLE_STRATEGY"
    "serializable strategy"
    :default :extra-read
    :parse-fn keyword
    :validate [#{:extra-read :extra-write}
               "Should be one of extra-read or extra-write"]]

   (cli/repeated-opt nil "--consistency-model CONSISTENCY_MODEL"
                     "consistency model to be checked"
                     ["snapshot-isolation"])

   [nil "--config-file CONFIG_FILE" "ScalarDB config file"
    :default ""]])

(defn- test-name
  [workload-key faults admin]
  (-> ["scalardb" (name workload-key)]
      (into (map name faults))
      (into (map name admin))
      (->> (remove nil?) (string/join "-"))))

(def ^:private scalardb-opts
  {:storage (atom nil)
   :transaction (atom nil)
   :2pc (atom nil)
   :table-id (atom INITIAL_TABLE_ID)
   :unknown-tx (atom #{})
   :failures (atom 0)
   :decommissioned (atom #{})})

(defn scalardb-test
  [base-opts db-key workload-key faults admin]
  (let [[db nemesis max-nodes] (gen-db db-key faults admin)
        consistency-model (->> base-opts :consistency-model (mapv keyword))
        workload-opts (merge base-opts
                             scalardb-opts
                             {:nodes (vec (take max-nodes (:nodes base-opts)))
                              :consistency-model consistency-model})
        workload ((workload-key workloads) workload-opts)]
    (merge tests/noop-test
           workload-opts
           {:name (test-name workload-key faults admin)
            :client (:client workload)
            :db db
            :pure-generators true
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/nemesis
                              (gen/phases
                               (gen/sleep 5)
                               (:generator nemesis)))
                             (gen/time-limit (:time-limit base-opts)))
                        (gen/nemesis (:final-generator nemesis))
                        (gen/clients (:final-generator workload)))
            :nemesis (:nemesis nemesis)
            :checker (:checker workload)})))

(defn test-cmd
  []
  {"test" {:opt-spec (->> test-opt-spec
                          (into cr/cassandra-opt-spec)
                          (into cr/admin-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage (cli/test-usage)
           :run (fn [{:keys [options]}]
                  (doseq [_ (range (:test-count options))
                          db-key (:db options)
                          workload-key (:workload options)
                          faults (:nemesis options)
                          admin (:admin options)]
                    (let [test (-> options
                                   (scalardb-test db-key
                                                  workload-key
                                                  faults
                                                  admin)
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (cli/run! (test-cmd)
            args))
