(ns scalardl.simple
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [clojure.tools.logging :refer [debug info warn]]
            [cassandra.conductors :as conductors]
            [knossos.model :as model]
            [scalardl.core :as dl]
            [scalardl.util :as util])
  (:import (javax.json Json)
           (java.util Optional)))

(def ^:private ^:const CONTRACTS [{:name "read"
                                   :class "com.scalar.jepsen.scalardl.Read"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Read.class"}
                                  {:name "write"
                                   :class "com.scalar.jepsen.scalardl.Write"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Write.class"}
                                  {:name "cas"
                                   :class "com.scalar.jepsen.scalardl.Cas"
                                   :path "target/classes/com/scalar/jepsen/scalardl/Cas.class"}])

(def ^:private ^:const ASSET_KEY "key")
(def ^:private ^:const ASSET_VALUE "value")
(def ^:private ^:const ASSET_VALUE_NEW "new_value")

(defn- create-argument
  ([key]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.build)))
  ([key value]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE value)
       (.build)))
  ([key cur next]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE cur)
       (.add ASSET_VALUE_NEW next)
       (.build))))

(defrecord SimpleClient [initialized? client-service]
  client/Client
  (open! [_ test _]
    (SimpleClient. initialized? (dl/prepare-client-service test)))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (util/create-tables test)
        (Thread/sleep 10000)  ;; Wait for the table creation
        (.registerCertificate client-service)
        (doseq [c CONTRACTS]
          (.registerContract client-service (:name c) (:class c) (:path c) (Optional/empty))))))

  (invoke! [_ test op]
    (let [[k v] (:value op)]
      (case (:f op)
        ;; TODO: unknown failure
        :read (let [resp (->> (create-argument k)
                              (.executeContract client-service "read"))]
                (if (util/success? resp)
                  (assoc op :type :ok :value (->> resp
                                                  util/response->obj
                                                  (.getInt ASSET_VALUE)))
                  (assoc op :type :fail :error (.getMessage resp))))

        :write (let [resp (->> (create-argument k v)
                               (.executeContract client-service "write"))]
                 (if (util/success? resp)
                   (assoc op :type :ok)
                   (assoc op :type :fail :error (.getMessage resp))))

        :cas (let [resp (->> (create-argument k (:cur v) (:next v))
                             (.executeContract client-service "cas"))]
               (if (util/success? resp)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error (.getMessage resp)))))))

  (close! [_ _]
    (.close client-service))

  (teardown! [_ test]))

(defn r [_ _] {:type :invoke :f :read :value [1 nil] })
(defn w [_ _] {:type :invoke :f :write :value [1 (rand-int 5)]})
(defn cas [_ _] {:type :invoke :f :cas :value [1 {:cur (rand-int 5) :next (rand-int 5)}]})

(defn simple-test
  [opts]
  (merge (dl/scalardl-test (str "simple-" (:suffix opts))
                           {:client     (SimpleClient. (atom false) nil)
                            :failures   (atom 0)
                            :generator  (gen/phases
                                         (conductors/std-gen opts [r w cas cas])
                                         (conductors/terminate-nemesis opts))
                            :checker   (checker/linearizable {:model (model/cas-register)
                                                              :algorithm :linear})})
         opts))
