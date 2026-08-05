(ns scalardb.db.cluster-db.cluster-db)

(defprotocol ClusterDb
  (get-storage-type [this])
  (get-contact-points [this])
  (get-username [this])
  (get-password [this])
  (install! [this test])
  (configure! [this test])
  (start! [this test])
  (wipe! [this test])
  (create-storage-properties [this test]))

(defprotocol ClusterDbFileOptions
  (file-io-options [this]
    "Returns the Chaos Mesh file I/O configuration for this backend."))

(defprotocol ClusterDbLogs
  (log-selector [this]
    "Returns a label selector matching this backend's database pods.
    Backends implement this to have their pod logs collected into the store.
    Externally-provisioned backends have no pods and don't implement it."))
