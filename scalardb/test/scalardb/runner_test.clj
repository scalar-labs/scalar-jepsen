(ns scalardb.runner-test
  (:require [clojure.test :refer [deftest is]]
            [scalardb.runner :as runner]))

(deftest stress-nemesis-option-test
  (is (= [:stress] (get runner/nemeses "stress"))))
