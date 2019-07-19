(ns scalardl.simple
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [clojure.tools.logging :refer [debug info warn]]
            [cassandra.conductors :as conductors]
            [knossos.model :as model]
            [scalardl
             [cassandra :as cassandra]
             [core :as dl]
             [util :as util]])
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

(def ^:private ^:const NONCE "nonce")
(def ^:private ^:const ASSET_KEY "key")
(def ^:private ^:const ASSET_VALUE "value")
(def ^:private ^:const ASSET_VALUE_NEW "new_value")

(defn- create-argument
  ([key]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add NONCE (str (java.util.UUID/randomUUID)))
       (.build)))
  ([key value]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE value)
       (.add NONCE (str (java.util.UUID/randomUUID)))
       (.build)))
  ([key cur next]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE cur)
       (.add ASSET_VALUE_NEW next)
       (.add NONCE (str (java.util.UUID/randomUUID)))
       (.build))))

(defrecord SimpleClient [initialized? client-service]
  client/Client
  (open! [_ test _]
    (SimpleClient. initialized? (atom (dl/prepare-client-service test))))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (cassandra/create-tables test)
        (Thread/sleep 10000)  ;; Wait for the table creation
        (info "register a certificate and contracts")
        (.registerCertificate @client-service)
        (doseq [c CONTRACTS]
          (.registerContract @client-service (:name c) (:class c) (:path c) (Optional/empty))))))

  (invoke! [_ test op]
    (case (:f op)
      :read (let [argument (create-argument 1)
                  resp (.executeContract @client-service "read" argument)]
              (if (util/success? resp)
                (assoc op :type :ok :value (-> resp
                                               util/response->obj
                                               (.getInt ASSET_VALUE)))
                (do
                  (reset! client-service
                          (dl/try-switch-server! @client-service test))
                  (assoc op :type :fail :error (.getMessage resp)))))

      :write (let [v (:value op)
                   argument (create-argument 1 v)
                   resp (.executeContract @client-service "write" argument)]
               (if (or (util/success? resp)
                       (and (util/unknown? resp)
                            (dl/check-tx-committed (.getString argument NONCE)
                                                   test)))
                 (assoc op :type :ok)
                 (do
                   (reset! client-service
                           (dl/try-switch-server! @client-service test))
                   (assoc op :type :fail :error (.getMessage resp)))))

      :cas (let [[cur next] (:value op)
                 argument (create-argument 1 cur next)
                 resp (.executeContract @client-service "cas" argument)]
             (if (or (util/success? resp)
                     (and (util/unknown? resp)
                          (dl/check-tx-committed (.getString argument NONCE)
                                                 test)))
               (assoc op :type :ok)
               (do
                 (reset! client-service
                         (dl/try-switch-server! @client-service test))
                 (assoc op :type :fail :error (.getMessage resp)))))))

  (close! [_ _]
    (.close @client-service))

  (teardown! [_ _]))

(def r {:type :invoke :f :read})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn simple-test
  [opts]
  (merge (dl/scalardl-test (str "simple-" (:suffix opts))
                           {:client     (SimpleClient. (atom false) (atom nil))
                            :failures   (atom 0)
                            :generator  (gen/phases
                                         (conductors/std-gen opts [r w cas cas cas])
                                         (conductors/terminate-nemesis opts))
                            :checker   (checker/linearizable {:model (model/cas-register)
                                                              :algorithm :linear})})
         opts))
