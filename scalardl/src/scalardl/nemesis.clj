(ns scalardl.nemesis
  (:require [jepsen
             [control :as c]
             [util :refer [meh]]]
            [cassandra
             [core :as cass]
             [nemesis :as cn]]
            [scalardl
             [core :as dl]
             [util :as util]]))

(defn- crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  []
  (cn/test-aware-node-start-stopper
   cn/safe-mostly-small-nonempty-subset
   (fn start [test node]
     (when-not (util/server? node test)
       (meh (c/su (c/exec :killall :-9 :java))) [:killed node]))
   (fn stop  [test node]
     (if (util/server? node test)
       (do
         (dl/start-server! node test)
         [:server-restarted node])
       (do
         (meh (cass/guarded-start! node test))
         (Thread/sleep (* 1000 60))
         [:cassandra-restarted node])))))

(defn crash
  []
  {:name "crash"
   :nemesis (crash-nemesis)})
