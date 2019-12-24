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
  (:import (com.scalar.client.service ClientService)
           (com.scalar.client.config ClientConfig)
           (com.scalar.client.service ClientModule)
           (com.google.inject Guice)
           (java.util Optional)
           (java.util Properties)))

(def ^:const RETRIES 8)
(def ^:private ^:const NUM_FAILURES_FOR_RECONNECTION 1000)

(def ^:private ^:const LOCAL_DIR "/scalar-jepsen/scalardl")
(def ^:private ^:const LEDGER_INSTALL_DIR "/root/ledger")
(def ^:private ^:const LEDGER_EXE "bin/scalar-ledger")
(def ^:private ^:const LEDGER_PROPERTIES (str LEDGER_INSTALL_DIR "/ledger.properties"))
(def ^:private ^:const LEDGER_KEY (str LEDGER_INSTALL_DIR "/server-key.pem"))
(def ^:private ^:const LEDGER_LOG (str LEDGER_INSTALL_DIR "/scalardl.log"))
(def ^:private ^:const LEDGER_PID (str LEDGER_INSTALL_DIR "/scalardl.pid"))

(defn exponential-backoff
  [r]
  (Thread/sleep (reduce * 1000 (repeat r 2))))

(defn- create-client-properties
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
      (if-let [injector (some-> test
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
      (info "switching the server to another")
      (.close client-service)
      (reset! (:failures test) 0)
      (prepare-client-service test))
    client-service))

(defn register-certificate
  [client-service]
  (let [resp (.registerCertificate client-service)]
    (when-not (util/success? resp)
      (throw (ex-info "Failed to register a certificate"
                      {:cause "Failed to register a certificate"})))))

(defn register-contracts
  "Register contracts which have
  {:name contract-name, :class class-name, :path contract-path}"
  [client-service contracts]
  (doseq [c contracts]
    (let [resp (.registerContract client-service
                                  (:name c) (:class c) (:path c)
                                  (Optional/empty))]
      (when-not (util/success? resp)
        (throw (ex-info "Failed to register a contract"
                        {:cause "Failed to register a contract"
                         :contract c}))))))

(defn check-tx-committed
  [txid test]
  (info "checking a TX state" txid)
  (loop [tries RETRIES]
    (when (< tries RETRIES)
      (exponential-backoff (- RETRIES tries)))
    (let [committed (cassandra/check-tx-state txid test)]
      (if-not (nil? committed)
        committed
        (if (pos? tries)
          (recur (dec tries))
          (warn "Failed to check the TX state" txid))))))

(defn response->result
  [resp op txid test]
  (if (util/success? resp)
    (assoc op :type :ok)
    (if (util/unknown? resp)
      (let [committed (check-tx-committed txid test)]
        (if (nil? committed)
          (assoc op :type :info :error (.getMessage resp)) ;; unknown
          (if committed
            (assoc op :type :ok)
            (assoc op :type :fail :error (.getMessage resp)))))
      (assoc op :type :fail :error (str "status code:" (.getStatus resp)
                                        " error message:" (.getMessage resp))))))

(defn- create-server-properties
  [test]
  (c/exec :echo (str "scalar.ledger.nonce_txid.enabled=true\n"
                     "scalar.database.contact_points="
                     (clojure.string/join "," (:cass-nodes test)) "\n"
                     "scalar.database.username=cassandra\n"
                     "scalar.database.password=cassandra")
          :> LEDGER_PROPERTIES))

(defn- install-jdk-with-retry
  []
  (letfn [(step [tries]
            (when (pos? tries)
              (exponential-backoff tries))
            (try
              (c/su (debian/install [:openjdk-8-jre]))
              (catch clojure.lang.ExceptionInfo e
                (debian/update!)
                (if (= tries RETRIES)
                  (throw e)
                  (step (inc tries))))))]
    (step 0)))

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
