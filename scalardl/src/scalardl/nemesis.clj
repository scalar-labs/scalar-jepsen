(ns scalardl.nemesis
  (:require [jepsen
             [control :as c]
             [util :refer [meh]]]
            [cassandra
             [core :as cass]
             [nemesis :as cn]]
            [scalardl.util :as util]))

(defn- crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (cn/test-aware-node-start-stopper
   cn/safe-mostly-small-nonempty-subset
   (fn start [test node]
     (when-not (util/server? node test)
       (meh (c/su (c/exec :killall :-9 :java))) [:killed node]))
   (fn stop  [test node]
     (when-not (util/server? node test)
       (meh (cass/guarded-start! node test))
       (meh (cass/wait-ready node 300 10 test))
       [:cassandra-restarted node]))))

(defn crash
  []
  {:name "crash"
   :nemesis (crash-nemesis)})
