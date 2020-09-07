(ns scalardb.runner
  (:gen-class)
  (:require [cassandra
             [core :as cassandra]
             [runner :as car]
             [nemesis :as can]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardb.transfer]
            [scalardb.transfer_append]
            [scalardb.elle_append]))

(def tests
  "A map of test names to test constructors."
  {"transfer"        scalardb.transfer/transfer-test
   "transfer-append" scalardb.transfer-append/transfer-append-test
   "elle-append" scalardb.elle-append/elle-append-test})

(def test-opt-spec
  [(cli/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   [nil "--isolation-level ISOLATION_LEVEL" "isolation level"
    :default :snapshot
    :parse-fn keyword
    :validate [#{:snapshot :serializable}
               "Should be one of snapshot or serializable"]]

   [nil "--serializable-strategy SERIALIZABLE_STRATEGY"
    "serializable strategy"
    :default :extra-write
    :parse-fn keyword
    :validate [#{:extra-write :extra-read}
               "Should be one of extra-write or extra-read"]]])

(defn test-cmd
  []
  {"test" {:opt-spec (->> test-opt-spec
                          (into car/cassandra-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn   (fn [parsed] (-> parsed cli/test-opt-fn))
           :usage    (cli/test-usage)
           :run      (fn [{:keys [options]}]
                       (doseq [i (range (:test-count options))
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
