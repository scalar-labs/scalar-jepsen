(ns scalardb.db.cluster-db.cluster-db)

(defprotocol ClusterDb
  (get-storage-type [this])
  (get-contact-points [this])
  (get-username [this])
  (get-password [this])
  (get-lb-service-name [this])
  (install! [this test])
  (configure! [this test])
  (start! [this test])
  (wipe! [this test])
  (create-storage-properties [this test]))
