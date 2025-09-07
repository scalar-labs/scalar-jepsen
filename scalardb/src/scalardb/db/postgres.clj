(ns scalardb.db.postgres
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen
             [control :as c]
             [db :as db]
             [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.nemesis.combined :as jn]
            [scalardb.db-extend :as ext])
  (:import (java.util Properties)))

(def ^:private ^:const DEFAULT_VERSION "15")
(def ^:private ^:const TIMEOUT_SEC 600)
(def ^:private ^:const INTERVAL_SEC 10)

(defn- install!
  "Installs PostgreSQL."
  [{:keys [version] :or {version DEFAULT_VERSION}}]
  (let [postgre (keyword (str "postgresql-" version))
        client (keyword (str "postgresql-client-" version))]
    (c/su
     (c/exec :wget
             :--quiet
             :-O
             :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc"
             c/| :apt-key :add :-)
     (debian/install [:lsb-release])
     (let [release (c/exec :lsb_release :-cs)]
       (debian/add-repo! "postgresql"
                         (str "deb http://apt.postgresql.org/pub/repos/apt/ "
                              release "-pgdg main")))
     (debian/install [postgre client])
     (c/su (c/exec :sed :-i
                   (c/lit "\"s/#listen_addresses = 'localhost'/listen_addresses = '*'/g\"")
                   (str "/etc/postgresql/" version "/main/postgresql.conf")))
     (c/su (c/exec :echo
                   (c/lit "host all all 0.0.0.0/0 trust")
                   c/| :tee :-a
                   (str "/etc/postgresql/" version "/main/pg_hba.conf")
                   :> "/dev/null"))
     (c/su (meh (c/exec :service :postgresql :stop)))
     (c/exec "update-rc.d" :postgresql :disable))))

(defn- get-bin-dir
  [version]
  (str "/usr/lib/postgresql/" version "/bin"))

(defn- get-main-dir
  [version]
  (str "/var/lib/postgresql/" version "/main"))

(defn- configure!
  [{:keys [version] :or {version DEFAULT_VERSION}}]
  (c/sudo "postgres"
          (c/exec (str (get-bin-dir version) "/initdb")
                  :-D (get-main-dir version))))

(defn- get-log-path
  [{:keys [version] :or {version DEFAULT_VERSION}}]
  (str "/var/log/postgresql/postgresql-" version "-main.log"))

(defn- start!
  []
  (c/su (c/exec :service :postgresql :start)))

(defn- stop!
  []
  (c/su (meh (c/exec :service :postgresql :stop))))

(defn- wipe!
  [{:keys [version] :or {version DEFAULT_VERSION}}]
  (stop!)
  (c/su (meh (c/exec :rm :-r (get-main-dir version))))
  (c/su (meh (c/exec :rm (get-log-path version)))))

(defn- live-node?
  [test]
  (let [node (-> test :nodes first)]
    (try
      (c/on node (c/sudo "postgres" (c/exec :pg_isready)))
      true
      (catch Exception _
        (info node "is down")
        false))))

(defn- wait-for-recovery
  "Wait for the node bootstrapping."
  ([test]
   (wait-for-recovery TIMEOUT_SEC INTERVAL_SEC test))
  ([timeout-sec interval-sec test]
   (when-not (live-node? test)
     (Thread/sleep (* interval-sec 1000))
     (if (>= timeout-sec interval-sec)
       (wait-for-recovery (- timeout-sec interval-sec) interval-sec test)
       (throw (ex-info "Timed out waiting for the postgres node"
                       {:cause "The node couldn't start"}))))))

(defn db
  "Setup PostgreSQL."
  []
  (reify
    db/DB
    (setup! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! test))
      (install! test)
      (configure! test)
      (start!))

    (teardown! [_ test _]
      (when-not (:leave-db-running? test)
        (wipe! test)))

    db/Primary
    (primaries [_ test] (:nodes test))
    (setup-primary! [_ _ _])

    db/Pause
    (pause! [_ _ _]
      (c/su (c/exec :service :postgresql :stop)))
    (resume! [_ _ _]
      (c/su (c/exec :service :postgresql :start)))

    db/Kill
    (start! [_ _ _]
      (c/su (c/exec :service :postgresql :restart)))
    (kill! [_ _ _]
      (doseq [pattern (shuffle
                       ["postgres -D" ; Main process
                        "main: checkpointer"
                        "main: background writer"
                        "main: walwriter"
                        "main: autovacuum launcher"])]
        (Thread/sleep (rand-int 100))
        (info "Killing" pattern "-" (cu/grepkill! pattern))))

    db/LogFiles
    (log-files [_ test _] [(get-log-path test)])))

(defrecord ExtPostgres []
  ext/DbExtension
  (get-db-type [_] :postgres)
  (live-nodes [_ test] (live-node? test))
  (wait-for-recovery [_ test] (wait-for-recovery test))
  (create-table-opts [_ _] {})
  (create-properties
    [_ test]
    (or (ext/load-config test)
        (let [node (-> test :nodes first)]
          ;; We have only one node in this test
          (->> (doto (Properties.)
                 (.setProperty "scalar.db.storage" "jdbc")
                 (.setProperty "scalar.db.contact_points"
                               (str "jdbc:postgresql://" node ":5432/"))
                 (.setProperty "scalar.db.username" "postgres")
                 (.setProperty "scalar.db.password" "postgres"))
               (ext/set-common-properties test)))))
  (create-storage-properties [this test] (ext/create-properties this test)))

(defn gen-db
  [faults admin]
  (when (seq admin)
    (warn "The admin operations are ignored: " admin))
  [(ext/extend-db (db) (->ExtPostgres))
   (jn/nemesis-package
    {:db db
     :interval 60
     :faults faults
     :partition {:targets [:one]}
     :kill {:targets [:one]}
     :pause {:targets [:one]}})
   1])
