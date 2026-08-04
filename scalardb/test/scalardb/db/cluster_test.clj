(ns scalardb.db.cluster-test
  (:require [clojure.test :refer [deftest is testing]]
            [scalardb.db.cluster :as cluster]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]))

(def file-io-options
  {:volume-path "/var/lib/database"
   :file-path "/var/lib/database/**/*"
   :pod-selector {:app "database"}
   :container-names ["database"]})

(def backend-with-file-options
  (reify
    cluster-db/ClusterDbFileOptions
    (file-io-options [_] file-io-options)))

(deftest nemesis-options-test
  (testing "adds backend-specific options for the file-io nemesis"
    (is (= {:file-io file-io-options}
           (#'cluster/nemesis-options backend-with-file-options
                                      :postgres
                                      [:file-io]))))

  (testing "does not require file options for other nemeses"
    (is (= {}
           (#'cluster/nemesis-options (Object.) :managed [:kill]))))

  (testing "rejects file I/O faults for a backend without file options"
    (let [exception (try
                      (#'cluster/nemesis-options
                       (Object.) :managed [:file-io])
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
      (is (= "Backend does not support the file-io nemesis"
             (ex-message exception)))
      (is (= {:db :managed} (ex-data exception))))))
