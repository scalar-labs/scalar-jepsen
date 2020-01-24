(ns scalardl.util
  (:require [clojure.tools.logging :refer [debug info warn]])
  (:import (com.scalar.dl.client.exception ClientException)
           (com.scalar.dl.ledger.service StatusCode)
           (javax.json Json)
           (java.io StringReader)))

(defn server?
  [node test]
  (if (some #(= % node) (:servers test)) true false))

(defn unknown?
  [^ClientException e]
  (= (.getStatusCode e) StatusCode/UNKNOWN_TRANSACTION_STATUS))

(defn get-exception-info
  [^ClientException e]
  (str "status code: " (.getStatusCode e)
       " error message: " (.getMessage e)))

(defn result->json
  "Returns the value from a ContractExecutionResult if it exists,
  and nil otherwise."
  [result]
  (-> result .getResult .get))
