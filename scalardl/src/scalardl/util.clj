(ns scalardl.util
  (:require [clojure.tools.logging :refer [debug info warn]])
  (:import (javax.json Json)
           (java.io StringReader)))

(def ^:private ^:const STATUS_CODE_SUCCESS 200)
(def ^:private ^:const STATUS_CODE_UNKNOWN 501)

(defn server?
  [node test]
  (if (some #(= % node) (:servers test)) true false))

(defn success?
  [response]
  (= (.getStatus response) STATUS_CODE_SUCCESS))

(defn response->obj
  "Returns the value from a ContractExecutionResponse if it exists, and nil otherwise."
  [response]
  (if (success? response)
    (-> response .getResult StringReader. (Json/createReader) .readObject)
    (if (= (.getStatus response) STATUS_CODE_UNKNOWN)
      (warn "The result of the request was unknown")
      (warn "The contract execution failed"))))
