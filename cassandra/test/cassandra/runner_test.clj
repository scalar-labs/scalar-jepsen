(ns cassandra.runner-test
  (:require [clojure.test :refer :all]
            [cassandra.runner :as runner]))

(deftest name-test
  (let [opts {:target "cassandra"
              :workload :batch
              :nemesis [:crash]
              :admin [:flush-compact]
              :time-limit 60}
        test (runner/cassandra-test opts)]
    (is (= "cassandra-batch-crash-flush-compact" (:name test)))))
