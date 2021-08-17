(ns scalardb.runner
  (:gen-class)
  (:require [cassandra
             [core :as cassandra]
             [runner :as car]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardb.transfer]
            [scalardb.transfer_append]
            [scalardb.elle_append]
            [scalardb.elle_write_read]
            [scalardb.transfer_2pc]
            [scalardb.transfer_append_2pc]
            [scalardb.elle_append_2pc]
            [scalardb.elle_write_read_2pc]))

(def tests
  "A map of test names to test constructors."
  {"transfer"        scalardb.transfer/transfer-test
   "transfer-append" scalardb.transfer-append/transfer-append-test
   "elle-append"     scalardb.elle-append/elle-append-test
   "elle-write-read" scalardb.elle-write-read/elle-write-read-test
   "transfer-2pc" scalardb.transfer-2pc/transfer-2pc-test
   "transfer-append-2pc" scalardb.transfer-append-2pc/transfer-append-2pc-test
   "elle-append-2pc" scalardb.elle-append-2pc/elle-append-2pc-test
   "elle-write-read-2pc" scalardb.elle-write-read-2pc/elle-write-read-2pc-test})

(def test-opt-spec
  [(cli/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

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

   [nil "--consistency-model CONSISTENCY_MODEL"
    "consistency model to be checked"
    :default :snapshot-isolation
    :parse-fn keyword]])

(defn test-cmd
  []
  {"test" {:opt-spec (->> test-opt-spec
                          (into car/cassandra-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn   (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage    (cli/test-usage)
           :run      (fn [{:keys [options]}]
                       (doseq [_ (range (:test-count options))
                               test-fn (:test options)
                               nemesis (:nemesis options)
                               joining (:join options)
                               clock (:clock options)]
                         (let [test (-> options
                                        (car/combine-nemesis nemesis joining clock)
                                        (assoc :db (cassandra/db))
                                        (assoc :pure-generators true)
                                        (dissoc :test)
                                        test-fn
                                        jepsen/run!)]
                           (when-not (:valid? (:results test))
                             (System/exit 1)))))}})

(defn -main
  [& args]
  (cli/run! (test-cmd)
            args))
