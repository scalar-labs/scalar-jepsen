(ns scalardb.runner
  (:gen-class)
  (:require [clojure.string :as string]
            [environ.core :refer [env]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]
             [generator :as gen]
             [tests :as tests]]
            [scalardb
             [core :refer [INITIAL_TABLE_ID]]
             [transfer]
             [transfer-append]
             [elle-append]
             [elle-write-read]
             [transfer-2pc]
             [transfer-append-2pc]
             [elle-append-2pc]
             [elle-write-read-2pc]]
            [clojure.core :as c]))

(def db-types
  "The map of test DBs."
  {"cassandra" :cassandra
   "postgres" :postgres
   "cluster" :cluster
   "cluster-cassandra" :cluster-cassandra})

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
                     [:cassandra] db-types)

   (cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-keys)

   (cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [[]]
                     nemeses)

   [nil "--isolation-level ISOLATION_LEVEL" "isolation level"
    :default :snapshot
    :parse-fn keyword
    :validate [#{:read-committed :snapshot :serializable}
               "Should be one of read-committed, snapshot, or serializable"]]

   (cli/repeated-opt nil "--consistency-model CONSISTENCY_MODEL"
                     "consistency model to be checked"
                     ["snapshot-isolation"])

   [nil "--enable-one-phase-commit" "if set, one-phase commit is enabled"
    :default false]

   [nil "--enable-group-commit" "if set, group commit is enabled"
    :default false]

   [nil "--config-file CONFIG_FILE"
    "ScalarDB config file. When this is given, other configuration options are ignored."
    :default ""]

   [nil "--docker-username DOCKER_USERNAME"
    "Username to pull ScalarDB Cluster node image in ghcr.io."
    :default ""]
   [nil "--docker-access-token DOCKER_ACCESS_TOKEN"
    "Access token to pull ScalarDB Cluster node image in ghcr.io."]])

(defn- test-name
  [workload-key faults admin]
  (-> ["scalardb" (name workload-key)]
      (into (map name faults))
      (into (map name admin))
      (->> (remove nil?) (string/join "-"))))

(def ^:private db->gen-namespace
  {:cassandra          'scalardb.db.cassandra
   :postgres           'scalardb.db.postgres
   :cluster            'scalardb.db.cluster
   :cluster-cassandra  'scalardb.db.cluster})

(defn- load-gen-db-fn
  [db-type]
  (if-let [ns-sym (db->gen-namespace db-type)]
    (do
      (require ns-sym)
      (-> ns-sym str (symbol "gen-db") resolve))
    (throw (ex-info "Unsupported DB" {:db db-type}))))

(defn- gen-db
  "Returns [extended-db constructed-nemesis num-max-nodes]."
  [db-type faults admin]
  ((load-gen-db-fn db-type) faults admin))

(defn- gen-test-opt-spec
  []
  (let [specs (atom (into test-opt-spec cli/test-opt-spec))]
    (when (= (env :cassandra?) "true")
      (binding [*ns* *ns*] (require 'cassandra.runner))
      (swap! specs into @(resolve 'cassandra.runner/cassandra-opt-spec))
      (swap! specs into @(resolve 'cassandra.runner/admin-opt-spec)))
    @specs))

(def ^:private scalardb-opts
  {:storage (atom nil)
   :transaction (atom nil)
   :2pc (atom nil)
   :table-id (atom INITIAL_TABLE_ID)
   :unknown-tx (atom #{})
   :failures (atom 0)
   :decommissioned (atom #{})})

(defn scalardb-test
  [base-opts db-type workload-key faults admin]
  (let [[db nemesis max-nodes] (gen-db db-type faults admin)
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
            :db-type db-type
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
  {"test" {:opt-spec (gen-test-opt-spec)
           :opt-fn (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage (cli/test-usage)
           :run (fn [{:keys [options]}]
                  (doseq [_ (range (:test-count options))
                          db-type (:db options)
                          workload-key (:workload options)
                          faults (:nemesis options)
                          admin (or (:admin options) [[]])]
                    (let [test (-> options
                                   (scalardb-test db-type
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
