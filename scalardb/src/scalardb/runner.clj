(ns scalardb.runner
  (:gen-class)
  (:require [cassandra.runner :as car]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardb
             [core :refer [INITIAL_TABLE_ID]]
             [transfer]
             [transfer-append]
             [elle-append]
             [elle-write-read]
             [transfer-2pc]
             [transfer-append-2pc]
             [elle-append-2pc]
             [elle-write-read-2pc]]))

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

(def test-opt-spec
  [(cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-keys)

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

   [nil "--use-null-tx-metadata" "Use null transaction metadata to test with existing tables."
    :default false]])

(defn test-cmd
  []
  {"test" {:opt-spec (->> test-opt-spec
                          (into car/cassandra-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage (cli/test-usage)
           :run (fn [{:keys [options]}]
                  (with-redefs [car/workloads workloads]
                    (doseq [_ (range (:test-count options))
                            workload (:workload options)
                            nemesis (:nemesis options)
                            admin (:admin options)]
                      (let [test (-> options
                                     (assoc :target "scalardb"
                                            :workload workload
                                            :nemesis nemesis
                                            :admin admin
                                            :storage (atom nil)
                                            :transaction (atom nil)
                                            :2pc (atom nil)
                                            :table-id (atom INITIAL_TABLE_ID)
                                            :unknown-tx (atom #{})
                                            :failures (atom 0))
                                     (update :consistency-model
                                             (fn [ms] (mapv keyword ms)))
                                     car/cassandra-test
                                     jepsen/run!)]
                        (when-not (:valid? (:results test))
                          (System/exit 1))))))}})

(defn -main
  [& args]
  (cli/run! (test-cmd)
            args))
