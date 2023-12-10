(ns cassandra.runner-test
  (:require [clojure.test :refer [deftest is]]
            [cassandra.runner :as runner]))

(deftest name-test
  (let [opts {:target "cassandra"
              :workload :batch
              :nemesis [:crash]
              :admin [:flush]
              :time-limit 60}
        test (runner/cassandra-test opts)]
    (is (= "cassandra-batch-crash-flush" (:name test)))))
