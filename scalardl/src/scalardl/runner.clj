(ns scalardl.runner
  (:gen-class)
  (:require [clojure.string :as cstr]
            [cassandra
             [nemesis :as can]
             [runner :as car]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardl
             [nemesis :as nemesis]
             [cas]
             [transfer]]))

(def tests
  "A map of test names to test constructors."
  {"cas"   scalardl.cas/cas-test
   "transfer" scalardl.transfer/transfer-test})

(def nemeses
  {"crash" `(nemesis/crash)})

(def opt-spec
  [(cli/repeated-opt nil "--test NAME" "Test(s) to run" [] tests)

   (cli/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                     [`(can/none)]
                     (merge car/nemeses nemeses))

   (cli/repeated-opt nil "--join NAME" "Which node joinings to use"
                     [{:name "" :bootstrap false :decommission false}]
                     car/joinings)

   (cli/repeated-opt nil "--clock NAME" "Which clock-drift to use"
                     [{:name "" :bump false :strobe false}]
                     car/clocks)

   [nil "--cass-nodes CASSANDRA_NODES"
    "Comma-separated list of cassandra hostnames."
    :default ["n1" "n2" "n3"]
    :parse-fn #(cstr/split % #",\s*")]
   [nil "--servers DL_SERVERS"
    "Comma-separated list of server hostnames."
    :default ["n4" "n5"]
    :parse-fn #(cstr/split % #",\s*")]

   [nil "--cert CERTIFICATE" "a certificate file for DL"
    :default "resources/client.pem"]
   [nil "--client-key CLIENT_KEY" "a private key for DL client"
    :default "resources/client-key.pem"]
   [nil "--server-key SERVER_KEY" "a private key for DL server"
    :default "resources/server-key.pem"]
   [nil "--ledger-tarball LEDGER_TARBALL" "DL server bin files"
    :default "resources/ledger.tar"]

   [nil "--cassandra-dir CASSANDRA_DIRECTORY" "Cassandra directory on DB node"
    :default "/root/cassandra"]

   [nil "--rf REPLICATION_FACTOR" "Replication factor"
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   (cli/tarball-opt "https://archive.apache.org/dist/cassandra/3.11.4/apache-cassandra-3.11.4-bin.tar.gz")])

(defn- parse-nodes
  "Returns options with `nodes` which has cass-nodes and servers"
  [parsed]
  (let [options (:options parsed)
        all-nodes (into (:cass-nodes options) (:servers options))]
    (assoc parsed :options (-> options
                               (dissoc :node :nodes-file)
                               (assoc :nodes all-nodes)))))

(defn- modify-decommissioned-node
  "Avoid starting Cassandra on a DL server"
  [opts joining]
  (let [rf (:rf opts)
        cass-nodes (:cass-nodes opts)
        bootstrap? (and (:bootstrap joining) (< rf (count cass-nodes)))
        decommissioned (if bootstrap? #{(last cass-nodes)} #{})]
    (assoc opts :decommissioned (atom decommissioned))))

(defn test-cmd
  []
  {"test" {:opt-spec (into cli/test-opt-spec opt-spec)
           :opt-fn   (fn [parsed] (-> parsed parse-nodes cli/test-opt-fn))
           :usage    (cli/test-usage)
           :run      (fn [{:keys [options]}]
                       (doseq [i (range (:test-count options))
                               test-fn (:test options)
                               nemesis (:nemesis options)
                               joining (:join options)
                               clock (:clock options)]
                         (let [test (-> options
                                        (car/combine-nemesis nemesis joining clock)
                                        (modify-decommissioned-node joining)
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
