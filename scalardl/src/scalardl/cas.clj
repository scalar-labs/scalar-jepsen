(ns scalardl.cas
  (:require [jepsen
             [checker :as checker]
             [client :as client]]
            [clojure.tools.logging :refer [info]]
            [knossos.model :as model]
            [scalardl
             [cassandra :as cassandra]
             [core :as dl]
             [util :as util]])
  (:import (com.scalar.dl.client.exception ClientException)
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

(def ^:private ^:const ASSET_KEY "key")
(def ^:private ^:const ASSET_VALUE "value")
(def ^:private ^:const ASSET_VALUE_NEW "new_value")

(defn- create-argument
  ([key]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       .build))
  ([key value]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE value)
       .build))
  ([key cur next]
   (-> (Json/createObjectBuilder)
       (.add ASSET_KEY key)
       (.add ASSET_VALUE cur)
       (.add ASSET_VALUE_NEW next)
       .build)))

(defn- handle-exception
  [e op txid test]
  (if (util/unknown? e)
    (try
      (if (dl/committed? txid test)
        (assoc op :type :ok)
        (assoc op :type :fail :error :state-write-failure))
      (catch clojure.lang.ExceptionInfo ex
        (assoc op :type :info :error (.getMessage ex)))) ;; unknown
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
    (let [txid (str (java.util.UUID/randomUUID))
          arg (case (:f op)
                :read (create-argument 1)
                :write (create-argument 1 (:value op))
                :cas (apply create-argument 1 (:value op)))]
      (try
        (let [contract (-> op :f name)
              result (.executeContract @client-service txid contract arg)]
          (if (= contract "read")
            (assoc op :type :ok :value (-> result
                                           util/result->json
                                           (.getInt ASSET_VALUE)))
            (assoc op :type :ok)))
        (catch ClientException e
          (reset! client-service
                  (dl/try-switch-server! @client-service test))
          (handle-exception e op txid test)))))

  (close! [_ _]
    (.close @client-service))

  (teardown! [_ _]))

(def r {:type :invoke :f :read})
(defn w [_ _] {:type :invoke :f :write :value (rand-int 5)})
(defn cas [_ _] {:type :invoke :f :cas :value [(rand-int 5) (rand-int 5)]})

(defn workload
  [_]
  {:client (->CasRegisterClient (atom false) (atom nil))
   :generator [r w cas cas cas]
   :checker (checker/linearizable {:model (model/cas-register)
                                   :algorithm :linear})})
