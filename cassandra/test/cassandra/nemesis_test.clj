(ns cassandra.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen
             [control :as c]
             [nemesis :as n]]
            [cassandra
             [core :as cass]
             [nemesis :as can]]
            [spy.core :as spy]))

(deftest safe-mostly-small-nonempty-subset-test
  (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]
              :decommissioned (atom #{})}]
    (is (every? #(.contains ["n1" "n2" "n3" "n4" "n5"] %)
                (can/safe-mostly-small-nonempty-subset (:nodes test) test)))

    (swap! (:decommissioned test) conj "n3")
    (is (every? #(.contains ["n1" "n2" "n4" "n5"] %)
                (can/safe-mostly-small-nonempty-subset (:nodes test) test)))))

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
          kill-result (n/invoke! nemesis test {:type :invoke :f :kill})]
      (is (= nemesis (n/setup! nemesis test)))
      (is (= :info (:type kill-result)))
      (is (= '([:killed "n3"] [:killed "n2"])
             (:value kill-result)))

      (let [start-result (n/invoke! nemesis test {:type :invoke :f :start})]
        (is (= :info (:type start-result)))
        (is (= '([:restarted "n3"] [:restarted "n2"])
               (:value start-result)))))))

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
          kill-result (n/invoke! nemesis test {:type :invoke :f :kill})]
      (is (= :info (:type kill-result)))
      (is (= '([:killed "n3"] [:killed "n2"])
             (:value kill-result)))

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
          kill-result (n/invoke! nemesis test {:type :invoke :f :kill})]
      (is (= :info (:type kill-result)))
      (is (= '([:killed "n5"] [:killed "n3"] [:killed "n2"])
             (:value kill-result)))

      (let [start-result (n/invoke! nemesis test {:type :invoke :f :start})]
        (is (= :info (:type start-result)))
        (is (= '([:restarted "n2"] [:restarted "n5"] [:restarted "n3"])
               (:value start-result)))))))

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
          kill-result (n/invoke! nemesis test {:type :invoke :f :kill})]
      (is (= :info (:type kill-result)))
      (is (= :no-target (:value kill-result)))

      (let [start-result (n/invoke! nemesis test {:type :invoke :f :start})]
        (is (= :info (:type start-result)))
        (is (= :not-started (:value start-result)))))))

(deftest join-test
  (let [join (can/join-nemesis)]
    (is (= join (n/setup! join nil)))
    (is (= join (n/teardown! join nil)))

    ; a node bootstrap
    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cass/joining-nodes (spy/stub #{})
                  cass/start! (spy/spy)]
      (let [decommissioned (atom #{"n1"})
            result (n/invoke! join
                              {:decommissioned decommissioned}
                              {:type :info :f :bootstrap})]
        (is (= "n1 bootstrapped" (:value result)))
        (is (spy/called-once? cass/start!))
        (is (spy/called-once? cass/joining-nodes))
        (is (empty? @decommissioned))))

    ; no node bootstrap
    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cass/joining-nodes (spy/stub #{})
                  cass/start! (spy/spy)]
      (let [result (n/invoke! join
                              {:decommissioned (atom #{})}
                              {:type :info :f :bootstrap})]
        (is (= "no nodes left to bootstrap" (:value result)))
        (is (spy/not-called? cass/start!))))

  ; a node decommission
    (with-redefs [cass/live-nodes (spy/stub '("n1" "n2" "n3" "n4"))
                  cass/nodetool (spy/spy)]
      (let [decommissioned (atom #{})
            result (n/invoke! join
                              {:rf 3
                               :decommissioned decommissioned}
                              {:type :info :f :decommission})]
        (is (re-find #"n[1-4] decommissioned" (:value result)))
        (is (= 1 (count @decommissioned)))))

    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cass/live-nodes (spy/stub '("n1" "n2" "n3"))]
      (let [result (n/invoke! join
                              {:rf 3
                               :decommissioned (atom #{})}
                              {:type :info :f :decommission})]
        (is (= "no nodes eligible for decommission" (:value result)))))))

(deftest flush-and-compacter-test
  (let [fc (can/flush-nemesis)]
    (is (= fc (n/setup! fc nil)))
    (is (= fc (n/teardown! fc nil)))

    (with-redefs [cass/nodetool (spy/stub)
                  cass/live-nodes (spy/stub '("n1" "n2" "n3"))]
      (let [result-flush (n/invoke! fc
                                    {:nodes ["n1" "n2" "n3"]}
                                    {:type :info :f :flush})]
        (is (re-find #"n[1-3] started flushing" (:value result-flush)))
        (is (spy/called-n-times? cass/nodetool 1)))

      (let [result-compact (n/invoke! fc
                                      {:nodes ["n1" "n2" "n3"]}
                                      {:type :info :f :compact})]
        (is (re-find #"n[1-3] started compacting" (:value result-compact)))
        (is (spy/called-n-times? cass/nodetool 2))))))
