(ns cassandra.runner-test
  (:require [clojure.test :refer :all]
            [cassandra.runner :as runner]))

(deftest name-test
  (let [opts {:workload :batch
              :nemesis [:crash]
              :admin [:flush-compact]
              :time-limit 60}
        test (runner/cassandra-test opts)]
    (is (= (:name test) "cassandra-batch-crash-flush-compact"))))
