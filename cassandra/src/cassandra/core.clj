(ns cassandra.core
  (:require [clojure.java.jmx :as jmx]
            [clojure.set :as set]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [db :as db]
             [util :as util :refer [meh timeout]]
             [control :as c :refer [| lit]]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.control
             [net :as cn]
             [util :as cu]]
            [jepsen.os.debian :as debian]
            [qbits.alia :as alia]
            [qbits.hayt.dsl.clause :refer :all]
            [qbits.hayt.dsl.statement :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core Session)
           (com.datastax.driver.core Cluster)
           (com.datastax.driver.core Metadata)
           (com.datastax.driver.core Host)
           (com.datastax.driver.core WriteType)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                TransportException
                                                WriteTimeoutException
                                                UnavailableException)
           (com.datastax.driver.core.schemabuilder SchemaBuilder)
           (com.datastax.driver.core.schemabuilder TableOptions)
           (com.datastax.driver.core.policies RetryPolicy
                                              RetryPolicy$RetryDecision)
           (java.net InetAddress)
           (java.util.concurrent TimeUnit)))

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn cassandra-log
  [test]
  (str (:cassandra-dir test) "/logs/system.log"))

(defn- disable-hints?
  "Returns true if Jepsen tests should run without hints"
  []
  (not (System/getenv "JEPSEN_DISABLE_HINTS")))

(defn dns-resolve
  "Gets the address of a hostname"
  [hostname]
  (.getHostAddress (InetAddress/getByName hostname)))

(defn dns-hostnames
  "Gets the list of hostnames"
  [test addrs]
  (let [names (:nodes test)
        ordered (map dns-resolve names)]
    (set (map (fn [addr]
                (->> (.indexOf ordered addr)
                     (get names)))
              addrs))))

(defn- get-shuffled-nodes
  [test]
  (-> test :nodes set (set/difference @(:decommissioned test)) shuffle))

(defn- get-jmx-status
  [node attr]
  (try
    (jmx/with-connection {:host node :port 7199}
      (jmx/read "org.apache.cassandra.db:type=StorageService"
                attr))
    (catch Exception e
      (info "Couldn't get status from node" node))))

(defn live-nodes
  "Get the list of live nodes from a random node in the cluster"
  [test]
  (->> test get-shuffled-nodes
       (some #(get-jmx-status % :LiveNodes))
       (dns-hostnames test)))

(defn joining-nodes
  "Get the list of joining nodes from a random node in the cluster"
  [test]
  (->> test
       get-shuffled-nodes
       (mapcat #(get-jmx-status % :JoiningNodes))
       set))

(defn seed-nodes
  "Get a list of seed nodes"
  [test]
  (if (= (:rf test) 1)
    (take 1 (:nodes test))
    (take (dec (:rf test)) (:nodes test))))

(defn nodetool
  "Run a nodetool command"
  [test node & args]
  (c/on node (apply c/exec (lit (str (:cassandra-dir test) "/bin/nodetool")) args)))

(defn- install-jdk-with-retry
  []
  (letfn [(step [tries]
            (when (pos? tries)
              (exponential-backoff tries))
            (try
              (c/su (debian/install [:openjdk-8-jre]))
              (catch clojure.lang.ExceptionInfo e
                (debian/update!)
                (if (= tries 7)
                  (throw e)
                  (step (inc tries))))))]
    (step 0)))

(defn install!
  "Installs Cassandra on the given node."
  [node test]
  (let [url (:tarball test)
        local-file (second (re-find #"file://(.+)" url))
        tpath (if local-file "file:///tmp/cassandra.tar.gz" url)]
    (install-jdk-with-retry)
    (info node "installing Cassandra from" url)
    (do (when local-file
          (c/upload local-file "/tmp/cassandra.tar.gz"))
        (cu/install-archive! tpath (:cassandra-dir test)))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "configuring Cassandra")
  (c/su
   (doseq [rep ["\"s/#MAX_HEAP_SIZE=.*/MAX_HEAP_SIZE='1G'/g\"" ; docker memory should be set to around 8G or more
                "\"s/#HEAP_NEWSIZE=.*/HEAP_NEWSIZE='256M'/g\""
                "\"s/LOCAL_JMX=yes/LOCAL_JMX=no/g\""
                (str "'s/# JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     "<public name>\"/JVM_OPTS=\"$JVM_OPTS -Djava.rmi.server.hostname="
                     (name node) "\"/g'")
                (str "'s/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote"
                     ".authenticate=true\"/JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management"
                     ".jmxremote.authenticate=false\"/g'")
                "'/JVM_OPTS=\"$JVM_OPTS -Dcassandra.mv_disable_coordinator_batchlog=.*\"/d'"]]
     (c/exec :sed :-i (lit rep) (str (:cassandra-dir test) "/conf/cassandra-env.sh")))
   (doseq [rep (into ["\"s/cluster_name: .*/cluster_name: 'jepsen'/g\""
                      (str "\"s/seeds: .*/seeds: '"
                           (clojure.string/join "," (seed-nodes test)) "'/g\"")
                      (str "\"s/listen_address: .*/listen_address: " (cn/ip node) "/g\"")
                      (str "\"s/rpc_address: .*/rpc_address: " (cn/ip node) "/g\"")
                      (str "\"s/hinted_handoff_enabled:.*/hinted_handoff_enabled: " (disable-hints?) "/g\"")
                      "\"s/commitlog_sync: .*/commitlog_sync: batch/g\""
                      (str "\"s/# commitlog_sync_batch_window_in_ms: .*/"
                           "commitlog_sync_batch_window_in_ms: 1.0/g\"")
                      "\"s/commitlog_sync_period_in_ms: .*/#/g\""
                      "\"/auto_bootstrap: .*/d\""
                      "\"s/# commitlog_compression.*/commitlog_compression:/g\""
                      "\"s/#hints_compression.*/hints_compression:/g\""
                      (str "\"s/#   - class_name: LZ4Compressor/"
                           "    - class_name: LZ4Compressor/g\"")])]
     (c/exec :sed :-i (lit rep) (str (:cassandra-dir test) "/conf/cassandra.yaml")))
   (c/exec :sed :-i (lit "\"s/INFO/DEBUG/g\"") (str (:cassandra-dir test) "/conf/logback.xml"))))

(defn start!
  "Starts Cassandra."
  [node test]
  (info node "starting Cassandra")
  (c/su
   (c/exec (lit (str (:cassandra-dir test) "/bin/cassandra -R")))))

(defn guarded-start!
  "Guarded start that only starts nodes that have joined the cluster already
  through initial DB lifecycle or a bootstrap. It will not start decommissioned
  nodes."
  [node test]
  (let [decommissioned (:decommissioned test)]
    (when-not (@decommissioned node)
      (start! node test))))

(defn stop!
  "Stops Cassandra."
  [node]
  (info node "stopping Cassandra")
  (c/su
   (meh (c/exec :killall :java))
   (while (.contains (c/exec :ps :-ef) "java")
     (Thread/sleep 100)))
  (info node "has stopped Cassandra"))

(defn wipe!
  "Shuts down Cassandra and wipes data."
  [test node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :rm :-r (str (:cassandra-dir test) "/logs")))
   (meh (c/exec :rm :-r (str (:cassandra-dir test) "/data")))))

(defn wait-turn
  "A node has to wait because Cassandra node can't start when another node is bootstrapping"
  [node {:keys [decommissioned nodes]}]
  (let [indexed-nodes (zipmap nodes (range (count nodes)))
        idx (indexed-nodes node)]
    (when-not (@decommissioned node)
      (Thread/sleep (* 1000 60 idx)))))

(defn db
  "Setup Cassandra."
  []
  (reify db/DB
    (setup! [_ test node]
      (when (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
        (wipe! test node))
      (doto node
        (install! test)
        (configure! test)
        (wait-turn test)
        (guarded-start! test)))

    (teardown! [_ test node]
      (when-not (seq (System/getenv "LEAVE_CLUSTER_RUNNING"))
        (wipe! test node)))

    db/LogFiles
    (log-files [_ _ _]
      [])))

(def add {:type :invoke, :f :add, :value 1})
(def sub {:type :invoke, :f :add, :value -1})
(def r {:type :invoke, :f :read})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
   (gen/once r)))

(defn create-my-keyspace
  [session test {:keys [keyspace]}]
  (alia/execute session (create-keyspace (keyword keyspace)
                                         (if-exists false)
                                         (with {:replication {"class"              "SimpleStrategy"
                                                              "replication_factor" (:rf test)}}))))

(defn create-my-table
  [session {:keys [keyspace table schema compaction-strategy]
            :or {compaction-strategy :SizeTieredCompactionStrategy}}]
  (alia/execute session (use-keyspace (keyword keyspace)))
  (alia/execute session (create-table (keyword table)
                                      (if-exists false)
                                      (column-definitions schema)
                                      (with {:compaction
                                             {:class compaction-strategy}}))))

(defn close-cassandra
  [cluster session]
  (some-> session alia/shutdown (.get 10 TimeUnit/SECONDS))
  (some-> cluster alia/shutdown (.get 10 TimeUnit/SECONDS)))

(defn handle-exception
  [op ^ExceptionInfo e & conditional?]
  (let [ex (:exception (ex-data e))
        exception-class (class ex)]
    (condp = exception-class
      WriteTimeoutException (condp = (.getWriteType ex)
                              WriteType/CAS (assoc op
                                                   :type :info
                                                   :value :write-timed-out)
                              WriteType/BATCH_LOG (assoc op
                                                         :type :info
                                                         :value :write-timed-out)
                              WriteType/SIMPLE (if conditional?
                                                 (assoc op :type :ok)
                                                 (assoc op
                                                        :type :info
                                                        :value :write-timed-out))
                              WriteType/BATCH (assoc op :type :ok)
                              WriteType/COUNTER (assoc op
                                                       :type :info
                                                       :value :write-timed-out)
                              (assoc op :type :fail :error :write-timed-out))
      ReadTimeoutException (assoc op :type :fail :error :read-timed-out)
      TransportException (assoc op :type :fail :error :node-down)
      UnavailableException (assoc op :type :fail :error :unavailable)
      NoHostAvailableException (do
                                 (info "All the servers are down - waiting 2s")
                                 (Thread/sleep 2000)
                                 (assoc op
                                        :type :fail
                                        :error :no-host-available)))))

(defn cassandra-test
  [name opts]
  (merge tests/noop-test
         {:name (str "cassandra-" name)}
         opts))
