(ns scalardl.cas
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
  (:import (com.scalar.client.exception ClientException)
           (javax.json Json)))

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
       .build))
  ([key value]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE value)
       (.add NONCE (str (java.util.UUID/randomUUID)))
       .build))
  ([key cur next]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE cur)
       (.add ASSET_VALUE_NEW next)
       (.add NONCE (str (java.util.UUID/randomUUID)))
       .build)))

(defn- handle-exception
  [e op txid test]
  (if (util/unknown? e)
    (let [committed (dl/check-tx-committed txid test)]
      (if (nil? committed)
        (assoc op :type :info :error (.getMessage e)) ;; unknown
        (if committed
          (assoc op :type :ok)
          (assoc op :type :fail :error (.getMessage e)))))
    (assoc op :type :fail :error (util/get-exception-info e))))

(defrecord CasRegisterClient [initialized? client-service]
  client/Client
  (open! [_ test _]
    (->CasRegisterClient initialized? (atom (dl/prepare-client-service test))))

  (setup! [_ test]
    (locking initialized?
      (when (compare-and-set! initialized? false true)
        (cassandra/create-tables test)
        (Thread/sleep 10000)  ;; Wait for the table creation
        (info "register a certificate and contracts")
        (dl/register-certificate @client-service)
        (dl/register-contracts @client-service CONTRACTS))))

  (invoke! [_ test op]
    (let [arg (case (:f op)
                :read (create-argument 1)
                :write (create-argument 1 (:value op))
                :cas (apply create-argument 1 (:value op)))]
      (try
        (let [contract (-> op :f name)
              result (.executeContract @client-service contract arg)]
          (if (= contract "read")
            (assoc op :type :ok :value (-> result
                                           util/result->json
                                           (.getInt ASSET_VALUE)))
            (assoc op :type :ok)))
        (catch ClientException e
          (reset! client-service
                  (dl/try-switch-server! @client-service test))
          (handle-exception e op (.getString arg NONCE) test)))))

  (close! [_ _]
    (.close @client-service))

  (teardown! [_ _]))

(def r {:type :invoke :f :read})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn cas-test
  [opts]
  (merge (dl/scalardl-test (str "cas-" (:suffix opts))
                           {:client     (->CasRegisterClient (atom false) (atom nil))
                            :failures   (atom 0)
                            :generator  (gen/phases
                                         (conductors/std-gen opts [r w cas cas cas])
                                         (conductors/terminate-nemesis opts))
                            :checker   (checker/linearizable {:model (model/cas-register)
                                                              :algorithm :linear})})
         opts))
