(ns scalardl.util
  (:require [clojure.tools.logging :refer [debug info warn]])
  (:import (com.scalar.client.service StatusCode)
           (javax.json Json)
           (java.io StringReader)))

(defn server?
  [node test]
  (if (some #(= % node) (:servers test)) true false))

(defn success?
  [response]
  (= (.getStatus response) (.get StatusCode/OK)))

(defn unknown?
  [response]
  (= (.getStatus response) (.get StatusCode/UNKNOWN_TRANSACTION_STATUS)))

(defn response->obj
  "Returns the value from a ContractExecutionResponse if it exists, and nil otherwise."
  [response]
  (if (success? response)
    (-> response .getResult StringReader. (Json/createReader) .readObject)
    (warn "The contract execution failed")))

(defn response->txid
  [response]
  (when (success? response)
    (-> response .getProofs first .getNonce)))
