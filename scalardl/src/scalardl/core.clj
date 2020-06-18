(ns scalardl.core
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [scalardl
             [cassandra :as cassandra]
             [util :as util]])
  (:import (com.scalar.dl.client.config ClientConfig)
           (com.scalar.dl.client.exception ClientException)
           (com.scalar.dl.client.service ClientService)
           (com.scalar.dl.client.service ClientModule)
           (com.google.inject Guice)
           (java.util Optional)
           (java.util Properties)))

(def ^:const RETRIES 8)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(def ^:private ^:const LEDGER_INSTALL_DIR "/root/ledger")
(def ^:private ^:const LEDGER_EXE "bin/scalar-ledger")
(def ^:private ^:const LEDGER_PROPERTIES (str LEDGER_INSTALL_DIR "/ledger.properties"))
(def ^:private ^:const LEDGER_KEY (str LEDGER_INSTALL_DIR "/server-key.pem"))
(def ^:private ^:const LEDGER_LOG (str LEDGER_INSTALL_DIR "/scalardl.log"))
(def ^:private ^:const LEDGER_PID (str LEDGER_INSTALL_DIR "/scalardl.pid"))

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn- retry-when-exception*
  [tries f args fallback]
  (if (pos? tries)
    (let [res (try {:value (apply f args)}
                   (catch Exception e
                     (if (zero? tries)
                       (throw e)
                       {:exception e})))]
      (if-let [e (:exception res)]
        (do
          (warn e)
          (when fallback (fallback))
          (exponential-backoff (- RETRIES tries))
          (recur (dec tries) f args fallback))
        (:value res)))))

(defn retry-when-exception
  [f args & fallback]
  (retry-when-exception* RETRIES f args fallback))

(defn- create-client-properties
  [test]
  (doto (Properties.)
    (.setProperty "scalar.dl.client.server.host" (rand-nth (:servers test)))
    (.setProperty "scalar.dl.client.cert_holder_id" "jepsen")
    (.setProperty "scalar.dl.client.cert_path" (:cert test))
    (.setProperty "scalar.dl.client.private_key_path" (:client-key test))))

(defn prepare-client-service
  [test]
  (retry-when-exception (fn []
                          (if-let [injector (some-> test
                                                    create-client-properties
                                                    ClientConfig.
                                                    ClientModule.
                                                    vector
                                                    Guice/createInjector)]
                            (.getInstance injector ClientService)
                            (throw (ex-info "Failed to get ClientService"
                                            {:cause :injection-failure}))))
                        []))

(defn try-switch-server!
  [client-service test]
  (if (= (swap! (:failures test) inc) NUM_FAILURES_FOR_RECONNECTION)
    (do
      (info "switching the server to another")
      (.close client-service)
      (reset! (:failures test) 0)
      (prepare-client-service test))
    client-service))

(defn register-certificate
  [client-service]
  (retry-when-exception (fn [] (.registerCertificate client-service)) []))

(defn register-contracts
  "Register contracts which have
  {:name contract-name, :class class-name, :path contract-path}"
  [client-service contracts]
  (doseq [c contracts]
    (retry-when-exception (fn [{:keys [name class path]}]
                            (.registerContract client-service
                                               name class path
                                               (Optional/empty)))
                          [c])))

(defn check-tx-committed
  [txid test]
  (info "checking a TX state" txid)
  (retry-when-exception (fn [id t]
                          (if-let [committed (cassandra/check-tx-state id t)]
                            committed
                            (throw (ex-info "Failed to read the TX state"
                                            {:cause :read-state-failure}))))
                        [txid test]))

(defn- create-server-properties
  [test]
  (c/exec :echo (str "scalar.dl.ledger.nonce_txid.enabled=true\n"
                     "scalar.db.contact_points="
                     (clojure.string/join "," (:cass-nodes test)) "\n"
                     "scalar.db.username=cassandra\n"
                     "scalar.db.password=cassandra")
          :> LEDGER_PROPERTIES))

(defn- install-jdk-with-retry
  []
  (retry-when-exception (fn [package]
                          (c/su (debian/install package)))
                        [[:openjdk-8-jre]]
                        debian/update!))

(defn- install-server!
  [node test]
  (info node "installing DL server")
  (c/su (c/exec :rm :-rf LEDGER_INSTALL_DIR))
  (install-jdk-with-retry)
  (c/upload (:ledger-tarball test) "/tmp/ledger.tar")
  (cu/install-archive! "file:///tmp/ledger.tar" LEDGER_INSTALL_DIR)
  (c/upload (:server-key test) LEDGER_KEY)
  (create-server-properties test))

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
        (cassandra/spinup-cassandra! node test)))

    (teardown! [_ test node]
      (if (util/server? node test)
        (stop-server! node)
        (cassandra/teardown-cassandra! node test)))

    db/LogFiles
    (log-files [_ test node]
      (if (util/server? node test)
        [LEDGER_LOG]
        [(cassandra/cassandra-log test)]))))

(defn scalardl-test
  [name opts]
  (merge tests/noop-test
         {:name (str "scalardl-" name)
          :db (db)}
         opts))
