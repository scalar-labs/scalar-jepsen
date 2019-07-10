(ns scalardl.core
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [scalardl.util :as util])
  (:import (com.scalar.client.service ClientService)
           (com.scalar.client.config ClientConfig)
           (com.scalar.client.service ClientModule)
           (com.google.inject Guice)
           (java.util Properties)))

(def ^:private ^:const RETRIES 5)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(def ^:private ^:const LOCAL_DIR "/scalar-jepsen/scalardl")
(def ^:private ^:const LEDGER_INSTALL_DIR "/root/ledger")
(def ^:private ^:const LEDGER_EXE "bin/scalar-ledger")
(def ^:private ^:const LEDGER_PROPERTIES (str LEDGER_INSTALL_DIR "/ledger.properties"))
(def ^:private ^:const LEDGER_LOG (str LEDGER_INSTALL_DIR "/scalardl.log"))
(def ^:private ^:const LEDGER_PID (str LEDGER_INSTALL_DIR "/scalardl.pid"))

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn create-client-properties
  [test]
  (doto (Properties.)
    (.setProperty "scalar.ledger.client.server_host" (rand-nth (:servers test)))
    (.setProperty "scalar.ledger.client.cert_holder_id" "jepsen")
    (.setProperty "scalar.ledger.client.cert_path" (:cert test))
    (.setProperty "scalar.ledger.client.private_key_path" (:client-key test))))

(defn prepare-client-service
  [test]
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (if (pos? tries)
      (if-let [injector (some->> test
                                 create-client-properties
                                 ClientConfig.
                                 ClientModule.
                                 vector
                                 Guice/createInjector)]
        (try
          (.getInstance injector ClientService)
          (catch Exception e
            (warn (.getMessage e))))
        (recur (dec tries)))
      (throw (ex-info "Failed to prepare ClientService"
                      {:cause "Failed to prepare ClientService"})))))

(defn try-switch-server!
  [client-service test]
  (if (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (do
      (info "Switch the server to another")
      (.close client-service)
      (reset! (:failures test) 0)
      (prepare-client-service test))
    client-service))

(defn- install-server!
  [node test]
  (info node "installing DL server")
  (c/su (c/exec :rm :-rf LEDGER_INSTALL_DIR))
  (c/su (debian/install [:openjdk-8-jre]))
  (c/upload (:ledger-tarball test) "/tmp/ledger.tar")
  (cu/install-archive! "file:///tmp/ledger.tar" LEDGER_INSTALL_DIR)
  (c/exec :echo (str "scalar.database.contact_points="
                     (clojure.string/join "," (:cass-nodes test)))
          :> LEDGER_PROPERTIES)
  (c/exec :echo "scalar.database.username=cassandra"
          :>> LEDGER_PROPERTIES)
  (c/exec :echo "scalar.database.password=cassandra"
          :>> LEDGER_PROPERTIES))

(defn start-server!
  [node test]
  (info node "starting DL server")
  (cu/start-daemon! {:logfile LEDGER_LOG :pidfile LEDGER_PID :chdir LEDGER_INSTALL_DIR}
                    LEDGER_EXE
                    :-config LEDGER_PROPERTIES))

(defn stop-server!
  [node]
  (info node "tearing down DL server")
  (cu/stop-daemon! LEDGER_PID)
  (c/su (c/exec :rm :-rf LEDGER_INSTALL_DIR)))

(defn db
  []
  (reify db/DB
    (setup! [_ test node]
      (if (util/server? node test)
        (do
          (install-server! node test)
          (info node "waiting for starting C* cluster")
          (Thread/sleep (* 1000 60 (count (:cass-nodes test))))
          (start-server! node test))
        (util/spinup-cassandra! node test)))

    (teardown! [_ test node]
      (if (util/server? node test)
        (stop-server! node)
        (util/teardown-cassandra! node)))

    db/LogFiles
    (log-files [_ test node]
      (if (util/server? node test)
        [LEDGER_LOG]
        ["/root/cassandra/logs/system.log"])))) ;; TODO: to const in cassandra

(defn scalardl-test
  [name opts]
  (merge tests/noop-test
         {:name (str "scalardl-" name)
          :os debian/os
          :db (db)}
         opts))
