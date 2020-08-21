(ns cassandra.runner-test
  (:require [clojure.test :refer :all]
            [jepsen
             [control :as c]
             [nemesis :as jn]]
            [jepsen.nemesis.time :as nt]
            [cassandra
             [core :as cass]
             [runner :as runner]
             [nemesis :as nemesis]]
            [spy.core :as spy]))

(deftest combine-nemesis-test
  (with-redefs [c/session (spy/spy)
                c/disconnect (spy/spy)
                c/exec (spy/spy)
                c/on-nodes (spy/mock (fn
                                       ([_ _])
                                       ([test nodes f]
                                        (doall (map #(f test %) nodes)))))
                nt/current-offset (spy/spy)
                nt/strobe-time! (spy/spy)
                nt/bump-time! (spy/spy)
                cass/live-nodes (spy/stub #{"n2" "n1" "n3" "n4"})
                cass/joining-nodes (spy/stub #{})
                cass/nodetool (spy/spy)
                cass/start! (spy/spy)
                nemesis/safe-mostly-small-nonempty-subset (spy/stub #{"n1"})]
    (let [base-opts {:nodes ["n1" "n2" "n3" "n4" "n5"] :rf 3}
          nemesis (runner/nemeses "crash")
          joining (runner/joinings "rejoin")
          clock (runner/clocks "drift")
          new-opts (runner/combine-nemesis base-opts nemesis joining clock)
          n (jn/setup! (:nemesis new-opts) new-opts)]
      (is (= "crash-rejoining-clock-drift" (:suffix new-opts)))
      (is (= {:name "-rejoining" :bootstrap true :decommission true}
             (:join new-opts)))
      (is (= {:name "-clock-drift"  :bump true  :strobe true}
             (:clock new-opts)))
      (is (= #{"n5"} @(:decommissioned new-opts)))

      (jn/invoke! n new-opts {:f :start})
      (is (spy/called-with? c/exec :killall :-9 :java))

      (jn/invoke! n new-opts {:f :decommission})
      (is (spy/called-once? cass/nodetool))

      (jn/invoke! n new-opts {:f :bootstrap})
      (is (spy/called-once? cass/start!))

      (jn/invoke! n new-opts {:f :bump :value {"n1" 100}})
      (is (spy/called-once? nt/bump-time!))

      (jn/invoke! n new-opts {:f :strobe
                              :value {"n1" {:delta 100 :period 10 :duration 1}}})
      (is (spy/called-once? nt/strobe-time!)))))
