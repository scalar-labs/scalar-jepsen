(ns scalardb.runner-test
  (:require [clojure.test :refer [deftest is]]
            [scalardb.core :refer [INITIAL_TABLE_ID]]
            [scalardb.runner :as runner]))

(deftest scalardb-opts-test
  (let [opts (#'runner/scalardb-opts)]
    (is (nil? @(:storage opts)))
    (is (nil? @(:transaction opts)))
    (is (= INITIAL_TABLE_ID @(:table-id opts)))
    (is (= #{} @(:unknown-tx opts)))
    (is (zero? @(:failures opts)))
    (is (= #{} @(:decommissioned opts)))))

(deftest scalardb-opts-not-shared-test
  ;; every run wipes the backend DB, so a run must not see another run's state
  (let [opts (#'runner/scalardb-opts)
        other (#'runner/scalardb-opts)]
    (doseq [k [:storage :transaction :table-id :unknown-tx
               :failures :decommissioned]]
      (is (not (identical? (k opts) (k other)))))
    (swap! (:table-id opts) inc)
    (is (= INITIAL_TABLE_ID @(:table-id other)))))
