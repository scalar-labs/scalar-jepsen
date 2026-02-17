(ns scalardb.db.cluster-db.cluster-db)

(defprotocol ClusterDb
  (get-storage-type [this])
  (get-contact-points [this])
  (get-username [this])
  (get-password [this])
  (install! [this])
  (configure! [this])
  (start! [this])
  (wipe! [this])
  (create-storage-properties [this test]))
