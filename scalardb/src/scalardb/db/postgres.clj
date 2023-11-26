(ns scalardb.db.postgres
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [db :as db]
             [util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def ^:private ^:const DEFAULT_VERSION "15")

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
