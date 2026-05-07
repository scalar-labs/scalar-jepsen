(ns scalardb.db.cluster-db.managed
  "Backend for externally-provisioned managed DBs (e.g., AWS Aurora, Cloud SQL).
  install!/configure!/start!/wipe! are no-ops; connection details come from a
  user-supplied YAML file passed via --managed-db-config.

  YAML schema:
    Either provide 'jdbc-url' (used as-is), or 'subprotocol' + 'host'
    (URL is built as jdbc:<subprotocol>://<host>:<port>/<database>).
    'username' is required. 'password', 'port', and 'database' are optional;
    defaults are provided for known subprotocols (postgresql, mysql)."
  (:require [clj-yaml.core :as yaml]
            [clojure.tools.logging :refer [info warn]]
            [scalardb.core :as scalar]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private subprotocol-defaults
  "Default port and database per JDBC subprotocol. Add entries for new
  managed DB families that follow the jdbc:<subprotocol>://host:port/db form."
  {"postgresql" {:port 5432 :database "postgres"}
   "mysql"      {:port 3306 :database scalar/KEYSPACE}})

(defn- load-config
  [path]
  (when (or (nil? path) (and (string? path) (empty? path)))
    (throw (ex-info "Managed DB backend requires --managed-db-config <file>"
                    {:option "--managed-db-config"})))
  (let [config (-> path slurp yaml/parse-string)]
    (when-not (or (:jdbc-url config)
                  (and (:subprotocol config) (:host config)))
      (throw (ex-info
              "Managed DB config needs 'jdbc-url' or both 'subprotocol' and 'host'"
              {:path path})))
    (when-not (:username config)
      (throw (ex-info "Managed DB config is missing 'username'" {:path path})))
    config))

(defn- build-jdbc-url
  [{:keys [jdbc-url subprotocol host port database]}]
  (or jdbc-url
      (let [defaults (get subprotocol-defaults subprotocol)
            actual-port (or port (:port defaults))
            actual-db (or database (:database defaults))]
        (when-not (and actual-port actual-db)
          (throw (ex-info
                  (str "Managed DB config: cannot derive port/database for "
                       "subprotocol '" subprotocol "'. Provide them explicitly "
                       "or use 'jdbc-url'.")
                  {:subprotocol subprotocol})))
        (str "jdbc:" subprotocol "://" host \: actual-port \/ actual-db))))

(defrecord ClusterDbManaged [config]
  ClusterDb
  (get-storage-type [_] "jdbc")

  (get-contact-points [_] (build-jdbc-url config))

  (get-username [_] (:username config))

  (get-password [_] (or (:password config) ""))

  (install! [_]
    (info "Managed DB; skipping install"))

  (configure! [_]
    (info "Managed DB; skipping configure"))

  (start! [_]
    (info "Managed DB; skipping start"))

  (wipe! [_]
    (warn "Managed DB; skipping wipe."
          " Existing test data is not cleaned up automatically."))

  (create-storage-properties [_ _]
    (doto (Properties.)
      (.setProperty "scalar.db.storage" "jdbc")
      (.setProperty "scalar.db.contact_points" (build-jdbc-url config))
      (.setProperty "scalar.db.username" (str (:username config)))
      (.setProperty "scalar.db.password" (str (or (:password config) ""))))))

(defn gen-cluster-db
  [opts]
  (->ClusterDbManaged (load-config (:managed-db-config opts))))
