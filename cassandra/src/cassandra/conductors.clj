(ns cassandra.conductors
  (:require [cassandra.core :as cassandra]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.nemesis.time :as nt]))

(defn bootstrapper
  []
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this test op]
      (let [decommissioned (:decommissioned test)]
        (if-let [node (first @decommissioned)]
          (do (info node "starting bootstrapping")
              (swap! decommissioned (comp set rest))
              (c/on node (cassandra/delete-data! test node false))
              (c/on node (cassandra/start! node test))
              (while (seq (cassandra/joining-nodes test))
                (info node "still joining")
                (Thread/sleep 1000))
              (assoc op :value (str node " bootstrapped")))
          (assoc op :value "no nodes left to bootstrap"))))
    (teardown! [this _] this)))

(defn decommissioner
  []
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this test op]
      (let [decommissioned (:decommissioned test)]
        (if-let [node (some-> test
                              cassandra/live-nodes
                              (set/difference @decommissioned)
                              shuffle
                              (get (:rf test)))] ; keep at least RF nodes
          (do (info node "decommissioning")
              (info @decommissioned "already decommissioned")
              (swap! decommissioned conj node)
              (cassandra/nodetool test node "decommission")
              (assoc op :value (str node " decommissioned")))
          (assoc op :value "no nodes eligible for decommission"))))
    (teardown! [this _] this)))

(defn flush-and-compacter
  "Flushes to sstables and forces a major compaction on all nodes"
  []
  (reify nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [this test op]
      (case (:f op)
        :start (do (doseq [node (:nodes test)]
                     (cassandra/nodetool test node "flush")
                     (cassandra/nodetool test node "compact"))
                   (assoc op :value (str (:nodes test)
                                         " nodes flushed and compacted")))
        :stop (assoc op :value "stop is a no-op with this nemesis")))
    (teardown! [this _] this)))

(defn- mix-failure
  "Make a seq with start and stop for nemesis failure,
  and mix bootstrapping and decommissioning"
  [opts]
  (let [decop  (when (:decommission (:join opts))
                 {:type :info :f :decommission})
        bootop (when (:bootstrap (:join opts))
                 {:type :info :f :bootstrap})
        reset  (when (:bump (:clock opts))
                 (nt/reset-gen opts nil))
        bump   (when (:bump (:clock opts))
                 (nt/bump-gen opts nil))
        strobe (when (:strobe (:clock opts))
                 (nt/strobe-gen opts nil))

        ops (remove nil? [decop bootop reset bump strobe])

        base [(gen/sleep (+ (rand-int 30) 60))
              {:type :info :f :start}
              (gen/sleep (+ (rand-int 30) 60))
              {:type :info :f :stop}]]
    (if-let [op (some-> ops seq rand-nth)]
      (conj base (gen/sleep (+ (rand-int 30) 60)) op)
      base)))

(defn mix-failure-seq
  [opts]
  (flatten (repeatedly #(mix-failure opts))))

(defn terminate-nemesis
  [opts]
  (if (or (:bump (:clock opts)) (:strobe (:clock opts)))
    (gen/nemesis [{:type :info :f :stop} (nt/reset-gen opts nil)])
    (gen/nemesis {:type :info :f :stop})))

(defn std-gen
  [opts gen]
  (->> gen
       gen/mix
       (gen/nemesis
        (mix-failure-seq opts))
       (gen/time-limit (:time-limit opts))))
