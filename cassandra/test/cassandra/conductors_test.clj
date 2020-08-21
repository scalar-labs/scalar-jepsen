(ns cassandra.conductors-test
  (:require [clojure.test :refer :all]
            [jepsen
             [control :as c]
             [nemesis :as jn]]
            [jepsen.nemesis.time :as nt]
            [cassandra.core :as cassandra]
            [cassandra.conductors :as conductors]
            [spy.core :as spy]))

(deftest bootstrapper-test
  (let [bootstrapper (conductors/bootstrapper)]
    (is (= bootstrapper (jn/setup! bootstrapper nil)))
    (is (= bootstrapper (jn/teardown! bootstrapper nil)))

    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cassandra/joining-nodes (spy/stub #{})
                  cassandra/start! (spy/spy)]
      (let [decommissioned (atom #{"n1"})
            result (jn/invoke! bootstrapper
                               {:decommissioned decommissioned}
                               {:type :info :f :start})]
        (is (= "n1 bootstrapped" (:value result)))
        (is (spy/called-once? cassandra/start!))
        (is (spy/called-once? cassandra/joining-nodes))
        (is (empty? @decommissioned))))

    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cassandra/joining-nodes (spy/stub #{})
                  cassandra/start! (spy/spy)]
      (let [result (jn/invoke! bootstrapper
                               {:decommissioned (atom #{})}
                               {:type :info :f :start})]
        (is (= "no nodes left to bootstrap" (:value result)))
        (is (spy/not-called? cassandra/start!))))))

(deftest decomissioner-test
  (let [decommissioner (conductors/decommissioner)]
    (is (= decommissioner (jn/setup! decommissioner nil)))
    (is (= decommissioner (jn/teardown! decommissioner nil)))

    (with-redefs [cassandra/live-nodes (spy/stub '("n1" "n2" "n3" "n4"))
                  cassandra/nodetool (spy/spy)]
      (let [decommissioned (atom #{})
            result (jn/invoke! decommissioner
                               {:rf 3
                                :decommissioned decommissioned}
                               {:type :info :f :start})]
        (is (re-find #"n[1-4] decommissioned" (:value result)))
        (is (= 1 (count @decommissioned)))))

    (with-redefs [c/session (spy/spy)
                  c/disconnect (spy/spy)
                  cassandra/live-nodes (spy/stub '("n1" "n2" "n3"))]
      (let [result (jn/invoke! decommissioner
                               {:rf 3
                                :decommissioned (atom #{})}
                               {:type :info :f :start})]
        (is (= "no nodes eligible for decommission" (:value result)))))))

(deftest flush-and-compacter-test
  (let [fc (conductors/flush-and-compacter)]
    (is (= fc (jn/setup! fc nil)))
    (is (= fc (jn/teardown! fc nil)))

    (with-redefs [cassandra/nodetool (spy/stub)]
      (let [result-start (jn/invoke! fc
                                     {:nodes ["n1" "n2" "n3"]}
                                     {:type :info :f :start})
            result-stop (jn/invoke! fc {} {:type :info :f :stop})]
        (is (= "[\"n1\" \"n2\" \"n3\"] nodes flushed and compacted"
               (:value result-start)))
        (is (spy/called-n-times? cassandra/nodetool 6))
        (is (= "stop is a no-op with this nemesis" (:value result-stop)))))))

(deftest std-gen-test
  (let [with-time-limit #(merge % {:time-limit 10})
        bootstrap (with-time-limit {:join {:bootstrap true :decommission false}})
        decommission (with-time-limit {:join {:bootstrap false :decommission true}})
        bump (with-time-limit {:clock {:bump true :strobe false}})
        strobe (with-time-limit {:clock {:bump false :strobe true}})]
    (with-redefs [nt/reset-gen (spy/stub {:type :info :f :reset})
                  nt/bump-gen (spy/stub {:type :info :f :bump})
                  nt/strobe-gen (spy/stub {:type :info :f :strobe})]
      (is (->> (conductors/std-gen bootstrap [])
               .gen .gens first .gen
               (some #(= {:type :info :f :bootstrap} %))))
      (is (->> (conductors/std-gen decommission [])
               .gen .gens first .gen
               (some #(= {:type :info :f :decommission} %))))
      (is (->> (conductors/std-gen bump [])
               .gen .gens first .gen
               (some #(= {:type :info :f :bump} %))))
      (is (->> (conductors/std-gen strobe [])
               .gen .gens first .gen
               (some #(= {:type :info :f :strobe} %)))))))
