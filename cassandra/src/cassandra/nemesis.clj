(ns cassandra.nemesis
  (:require [clojure.tools.logging :refer [info]]
            [clojure.set :as set]
            [jepsen
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]
             [util    :as util :refer [meh]]]
            [jepsen.nemesis [combined :as nc]]
            [cassandra.core :as cass]))

(def ^:const ^:private default-interval
  "The default interval, in seconds, between nemesis operations."
  60)

(defn safe-mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.
      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs test]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))
      set
      (set/difference @(:decommissioned test))
      shuffle))

(defn- reorder-restarting-nodes
  "Reorder the list of nodes to start with a seed node when all nodes crash."
  [nodes test]
  (let [all-nodes (or (:cass-nodes test) (:nodes test))
        decommissioned @(:decommissioned test)
        seed (-> test cass/seed-nodes set (set/difference decommissioned) first)
        all-crashed? (= (+ (count nodes) (count decommissioned))
                        (count all-nodes))]
    (if all-crashed? (conj (remove #(= seed %) nodes) seed) nodes)))

(defn test-aware-node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify
      nemesis/Reflection
      (fs [_] #{:start :kill})

      nemesis/Nemesis
      (setup! [this _] this)

      (invoke! [_ test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :kill (if-let [ns (-> (or (:cass-nodes test) (:nodes test))
                                         (targeter test)
                                         util/coll)]
                           (if (compare-and-set! nodes nil ns)
                             (vals (c/on-many ns (start! test c/*host*)))
                             (str "nemesis already disrupting " @nodes))
                           :no-target)
                   :start (if-let [ns @nodes]
                            (let [reordered (reorder-restarting-nodes ns test)
                                  restarted (for [node reordered]
                                              (c/on node (stop! test node)))]
                              (reset! nodes nil)
                              restarted)
                            :not-started)))))

      (teardown! [_ test]
        (when-let [ns @nodes]
          (for [node (reorder-restarting-nodes ns test)]
            (c/on node (stop! test node)))
          (reset! nodes nil))))))

(defn crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (test-aware-node-start-stopper
   safe-mostly-small-nonempty-subset
   (fn start [_ node] (meh (c/su (c/exec :killall :-9 :java))) [:killed node])
   (fn stop  [test node]
     (meh (cass/guarded-start! node test))
     (meh (cass/wait-ready node 300 10 test))
     [:restarted node])))

(defn crash-generators
  [opts]
  (let [start  {:type :info, :f :start}
        kill   (fn [_ _] {:type :info, :f :kill})
        kill-start (gen/stagger (:interval opts default-interval)
                                (gen/flip-flop kill (repeat start)))]
    {:generator       [kill-start]
     :final-generator [start]}))

(defn crash-package
  "Crash nemesis and generator package for Cassandra."
  [opts]
  (when (contains? (:faults opts) :crash)
    (let [{:keys [generator final-generator]} (crash-generators opts)]
      {:generator       generator
       :final-generator final-generator
       :nemesis         (crash-nemesis)
       :perf #{{:name   "kill"
                :start  #{:kill}
                :stop   #{:start}
                :color  "#E9A4A0"}}})))

(defn join-nemesis
  []
  (reify
    nemesis/Reflection
    (fs [_] #{:bootstrap :decommission})

    nemesis/Nemesis
    (setup! [this _] this)

    (invoke! [_ test op]
      (let [decommissioned (:decommissioned test)]
        (case (:f op)
          :bootstrap (if-let [node (first @decommissioned)]
                       (do (info node "starting bootstrapping")
                           (swap! decommissioned (comp set rest))
                           (c/on node (cass/delete-data! test node false))
                           (c/on node (cass/start! node test))
                           (while (seq (cass/joining-nodes test))
                             (info node "still joining")
                             (Thread/sleep 1000))
                           (assoc op :value (str node " bootstrapped")))
                       (assoc op :value "no nodes left to bootstrap"))
          :decommission (if-let [node (some-> test
                                              cass/live-nodes
                                              shuffle
                                              (get (:rf test)))] ; keep at least RF nodes
                          (do (info node "decommissioning")
                              (info @decommissioned "already decommissioned")
                              (swap! decommissioned conj node)
                              (cass/nodetool test node "decommission")
                              (assoc op :value (str node " decommissioned")))
                          (assoc op :value "no nodes eligible for decommission")))))

    (teardown! [this _] this)))

(defn join-generator
  [opts]
  (when (contains? (:admin opts) :join)
    (->> (gen/mix [(repeat {:type :info, :f :bootstrap})
                   (repeat {:type :info, :f :decommission})])
         (gen/stagger default-interval))))

(defn join-package
  "A combined nemesis package for adding and removing nodes."
  [opts]
  (when (contains? (:admin opts) :join)
    {:nemesis   (join-nemesis)
     :generator (join-generator opts)
     :perf      #{{:name  "bootstrap"
                   :fs    [:bootstrap]
                   :color "#E9A0E6"}
                  {:name  "decommission"
                   :fs    [:decommision]
                   :color "#ACA0E9"}}}))

(defn flush-nemesis
  []
  (reify
    nemesis/Reflection
    (fs [_] #{:flush :compact})

    nemesis/Nemesis
    (setup! [this _] this)
    (invoke! [_ test op]
      (if-let [node (some-> test cass/live-nodes vec rand-nth)]
        (do (cass/nodetool test node (name (:f op)))
            (assoc op :value (str node " started " (name (:f op)) "ing")))
        (assoc op :value (str "no nodes left to " (name (:f op))))))
    (teardown! [this _] this)))

(defn flush-generator
  [opts]
  (when (contains? (:admin opts) :flush)
    (->> (gen/mix [(repeat {:type :info, :f :flush})
                   (repeat {:type :info, :f :compact})])
         (gen/stagger default-interval))))

(defn flush-package
  "A combined nemesis package for flush and compaction."
  [opts]
  (when (contains? (:admin opts) :flush)
    {:nemesis   (flush-nemesis)
     :generator (flush-generator opts)
     :perf      #{{:name  "flush"
                   :fs    [:flush]
                   :color "#E9A0E6"}
                  {:name  "compact"
                   :fs    [:compact]
                   :color "#ACA0E9"}}}))

(defn nemesis-package
  "Constructs a nemesis and generators for etcd."
  [opts]
  (let [opts (-> opts
                 (update :faults set)
                 (update :admin set))]
    (->> [(nc/partition-package opts)
          (nc/clock-package opts)
          (crash-package opts)
          (join-package opts)
          (flush-package opts)]
         (remove nil?)
         nc/compose-packages)))
