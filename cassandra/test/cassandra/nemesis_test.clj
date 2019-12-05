(ns cassandra.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen
             [control :as c]
             [nemesis :as n]]
            [cassandra.nemesis :as can]
            [spy.core :as spy]))

(deftest safe-mostly-small-nonempty-subset-test
  (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
              :decommissioned (atom #{})}]
    (= ["n1" "n2" "n3" "n4" "n5"]
       (sort (can/safe-mostly-small-nonempty-subset (:nodes test) test)))

    (swap! (:decommissioned test) conj "n3")
    (= ["n1" "n2" "n4" "n5"]
       (sort (can/safe-mostly-small-nonempty-subset (:nodes test) test)))))

(deftest test-aware-node-start-stopper-test
  (with-redefs [c/session (spy/spy)
                c/disconnect (spy/spy)]
    (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
                :rf 3
                :decommissioned (atom #{})}
          nemesis (can/test-aware-node-start-stopper
                   (fn [_ _] (list "n3" "n2"))
                   (fn [_ n] [:killed n])
                   (fn [_ n]  [:restarted n]))
          start-result (n/invoke! nemesis test {:type :invoke :f :start})]
      (is (= nemesis (n/setup! nemesis test)))
      (is (= :info (:type start-result)))
      (is (= '([:killed "n3"] [:killed "n2"])
             (:value start-result)))

      (let [stop-result (n/invoke! nemesis test {:type :invoke :f :stop})]
        (is (= :info (:type stop-result)))
        (is (= '([:restarted "n3"] [:restarted "n2"])
               (:value stop-result)))))))

(deftest test-aware-node-start-stopper-teardown-test
  (with-redefs [c/session (spy/spy)
                c/disconnect (spy/spy)]
    (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
                :rf 3
                :decommissioned (atom #{})}
          nemesis (can/test-aware-node-start-stopper
                   (fn [_ _] (list "n3" "n2"))
                   (fn [_ n] [:killed n])
                   (fn [_ n]  [:restarted n]))
          start-result (n/invoke! nemesis test {:type :invoke :f :start})]
      (is (= :info (:type start-result)))
      (is (= '([:killed "n3"] [:killed "n2"])
             (:value start-result)))

      (n/teardown! nemesis test))))

(deftest test-aware-node-start-stopper-with-decommission-test
  (with-redefs [c/session (spy/spy)
                c/disconnect (spy/spy)]
    (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
                :rf 3
                :decommissioned (atom #{"n1" "n4"})}
          nemesis (can/test-aware-node-start-stopper
                   (fn [_ _] (list "n5" "n3" "n2")) ;; all nodes crash
                   (fn [_ n] [:killed n])
                   (fn [_ n]  [:restarted n]))
          start-result (n/invoke! nemesis test {:type :invoke :f :start})]
      (is (= :info (:type start-result)))
      (is (= '([:killed "n5"] [:killed "n3"] [:killed "n2"])
             (:value start-result)))

      (let [stop-result (n/invoke! nemesis test {:type :invoke :f :stop})]
        (is (= :info (:type stop-result)))
        (is (= '([:restarted "n2"] [:restarted "n5"] [:restarted "n3"])
               (:value stop-result)))))))

(deftest test-aware-node-start-stopper-no-target-test
  (with-redefs [c/session (spy/spy)
                c/disconnect (spy/spy)]
    (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
                :rf 3
                :decommissioned (atom #{"n4"})}
          nemesis (can/test-aware-node-start-stopper
                   (fn [_ _] nil) ;; no target
                   (fn [_ n] [:killed n])
                   (fn [_ n]  [:restarted n]))
          start-result (n/invoke! nemesis test {:type :invoke :f :start})]
      (is (= :info (:type start-result)))
      (is (= :no-target (:value start-result)))

      (let [stop-result (n/invoke! nemesis test {:type :invoke :f :stop})]
        (is (= :info (:type stop-result)))
        (is (= :not-started (:value stop-result)))))))
