(ns scalardb.db.cluster-db.managed
  "Backend for externally-provisioned managed DBs (e.g., AWS Aurora, Aurora
  MySQL, Amazon DynamoDB, Azure Cosmos DB for NoSQL).
  install!/configure!/start!/wipe! are no-ops; connection details come from a
  user-supplied YAML file passed via --managed-db-config.

  YAML schema:
    storage:        Required. One of 'jdbc', 'dynamo', 'cosmos'.
    contact-points: For storage=jdbc, an explicit JDBC URL (alternative to
                    subprotocol+host below). For storage=dynamo, the AWS
                    region. For storage=cosmos, the Cosmos DB URI.
    subprotocol, host, port, database:
                    For storage=jdbc only. If contact-points is not given, the
                    URL is built as jdbc:<subprotocol>://<host>:<port>/<db>.
                    Defaults are provided for postgresql/mysql.
    username:       Required for jdbc and dynamo. Ignored for cosmos.
    password:       Required."
  (:require [clj-yaml.core :as yaml]
            [clojure.tools.logging :refer [info warn]]
            [scalardb.core :as scalar]
            [scalardb.db.cluster-db.cluster-db :refer [ClusterDb]])
  (:import (java.util Properties)))

(def ^:private valid-storages #{"jdbc" "dynamo" "cosmos"})

(def ^:private subprotocol-defaults
  "Default port and database per JDBC subprotocol."
  {"postgresql" {:port 5432 :database "postgres"}
   "mysql"      {:port 3306 :database scalar/KEYSPACE}})

(defn- load-config
  [path]
  (when (or (nil? path) (and (string? path) (empty? path)))
    (throw (ex-info "Managed DB backend requires --managed-db-config <file>"
                    {:option "--managed-db-config"})))
  (let [config (-> path slurp yaml/parse-string)
        {:keys [storage contact-points subprotocol host username password]} config]
    (when-not (valid-storages storage)
      (throw (ex-info (str "Managed DB config 'storage' must be one of "
                           valid-storages)
                      {:path path :storage storage})))
    (case storage
      "jdbc"
      (when-not (or contact-points (and subprotocol host))
        (throw (ex-info
                (str "Managed DB config (jdbc): needs 'contact-points' "
                     "or both 'subprotocol' and 'host'")
                {:path path})))

      ("dynamo" "cosmos")
      (when-not contact-points
        (throw (ex-info
                (str "Managed DB config (" storage
                     "): 'contact-points' is required")
                {:path path}))))
    (when (and (#{"jdbc" "dynamo"} storage) (not username))
      (throw (ex-info (str "Managed DB config (" storage
                           "): 'username' is required")
                      {:path path})))
    (when-not password
      (throw (ex-info "Managed DB config: 'password' is required"
                      {:path path})))
    config))

(defn- resolve-contact-points
  "Returns the value to set as scalar.db.contact_points.
  For jdbc: an explicit URL if provided, otherwise built from parts.
  For dynamo/cosmos: the configured contact-points string (region or URI)."
  [{:keys [storage contact-points subprotocol host port database]}]
  (if (= storage "jdbc")
    (or contact-points
        (let [defaults (get subprotocol-defaults subprotocol)
              actual-port (or port (:port defaults))
              actual-db (or database (:database defaults))]
          (when-not (and actual-port actual-db)
            (throw (ex-info
                    (str "Managed DB config: cannot derive port/database for "
                         "subprotocol '" subprotocol "'. Provide them "
                         "explicitly or use 'contact-points'.")
                    {:subprotocol subprotocol})))
          (str "jdbc:" subprotocol "://" host \: actual-port \/ actual-db)))
    contact-points))

(defrecord ClusterDbManaged [config]
  ClusterDb
  (get-storage-type [_] (:storage config))

  (get-contact-points [_] (resolve-contact-points config))

  (get-username [_] (or (:username config) ""))

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
      (.setProperty "scalar.db.storage" (:storage config))
      (.setProperty "scalar.db.contact_points" (resolve-contact-points config))
      (.setProperty "scalar.db.username" (str (or (:username config) "")))
      (.setProperty "scalar.db.password" (str (:password config))))))

(defn gen-cluster-db
  [opts]
  (->ClusterDbManaged (load-config (:managed-db-config opts))))
