(ns scalardb.db.postgre
  (:require [clojure.tools.logging :refer [info]]
            [jepsen
             [control :as c]
             [db :as db]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn- install!
  "Installs PostgreSQL."
  [{:keys [version]}]
  (let [postgre (keyword (str "postgresql-" (or version "15")))
        client (keyword (str "postgresql-client-" (or version "15")))]
    (c/su
     (c/exec :wget :--quiet :-O :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc" c/| :apt-key :add :-)
     (debian/install [:lsb-release])
     (let [release (c/exec :lsb_release :-cs)]
       (debian/add-repo! "postgresql"
                         (str "deb http://apt.postgresql.org/pub/repos/apt/ "
                              release "-pgdg main")))
     (debian/install [postgre client])
     (c/exec :service :postgresql :stop)
     (c/exec "update-rc.d" :postgresql :disable))))

(defn- configure!
  [{:keys [version]}]
  (let [version (or version "15")]
    (c/sudo "postgres"
            (c/exec (str "/usr/lib/postgresql/" version "/bin/initdb")
                    :-D (str "/var/lib/postgresql/" version "/main")))))

(defn- start!
  []
  (c/su (c/exec :service :postgresql :start)))

(defn- log
  [{:keys [version]}]
  (str "/var/log/postgresql/postgresql-" version "-main.log"))

(defn db
  "Setup PostgreSQL."
  []
  (reify
    db/DB
    (setup! [_ test _]
      (install! test)
      (configure! test)
      (start!))

    (teardown! [_ _ _]
      (c/su (c/exec :service :postgresql :stop)))

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
    (log-files [_ test _] [(log test)])))
