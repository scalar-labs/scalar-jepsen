(ns scalardl.runner
  (:gen-class)
  (:require [clojure.string :as cstr]
            [cassandra
             [core :as cassandra]
             [runner :as car]]
            [jepsen
             [core :as jepsen]
             [cli :as cli]]
            [scalardl
             [core :as dl]
             [cas]
             [transfer]]))

(def workload-keys
  "A map of test names to test constructors."
  {"cas"      :cas
   "transfer" :transfer})

(def workloads
  "A map of workload to test constructors."
  {:cas      scalardl.cas/workload
   :transfer scalardl.transfer/workload})

(def opt-spec
  [(cli/repeated-opt nil "--workload NAME" "Test(s) to run" [] workload-keys)

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
    :default "resources/ledger.tar"]])

(defn- parse-nodes
  "Returns options with `nodes` which has cass-nodes and servers"
  [parsed]
  (let [options (:options parsed)
        all-nodes (into (:cass-nodes options) (:servers options))]
    (assoc parsed :options (-> options
                               (dissoc :node :nodes-file)
                               (assoc :nodes all-nodes)))))

(defn test-cmd
  []
  {"test" {:opt-spec (->> opt-spec
                          (into car/cassandra-opt-spec)
                          (into cli/test-opt-spec))
           :opt-fn   (fn [parsed] (-> parsed parse-nodes cli/test-opt-fn))
           :usage    (cli/test-usage)
           :run (fn [{:keys [options]}]
                  (with-redefs [cassandra/db dl/db
                                car/workloads workloads]
                    (doseq [_ (range (:test-count options))
                            workload (:workload options)
                            nemesis (:nemesis options)
                            admin (:admin options)]
                      (let [test (-> options
                                     (assoc :target "scalardl"
                                            :workload workload
                                            :nemesis nemesis
                                            :admin admin
                                            :unknown-tx (atom #{})
                                            :failures (atom 0))
                                     car/cassandra-test
                                     jepsen/run!)]
                        (when-not (:valid? (:results test))
                          (System/exit 1))))))}})

(defn -main
  [& args]
  (cli/run! (test-cmd)
            args))
