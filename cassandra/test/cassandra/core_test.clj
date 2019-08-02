(ns cassandra.core-test
  (:require [clojure.java.jmx :as jmx]
            [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.nemesis :as n]
            [jepsen.os.debian :as debian]
            [cassandra.core :as cass]
            [spy.core :as spy])
  (:import (java.net UnknownHostException)))

(deftest dns-resolve-test
  (is (= "127.0.0.1" (cass/dns-resolve "localhost"))))

(deftest dns-resolve-exception-test
  (is (thrown? UnknownHostException (cass/dns-resolve "unknown"))))

(deftest dns-hostnames-test
  (with-redefs [cass/dns-resolve (spy/mock (fn [n]
                                             (case n
                                               "n1" "10.0.0.1"
                                               "n2" "10.0.0.2"
                                               "n3" "10.0.0.3")))]
    (let [test {:nodes ["n1" "n2" "n3"]}]
      (is (= #{"n1" "n2" "n3"}
             (cass/dns-hostnames test '("10.0.0.3" "10.0.0.1" "10.0.0.2")))))))

(deftest live-nodes-test
  (with-redefs [cass/get-jmx-status (spy/stub ["10.0.0.3" "10.0.0.1"])
                cass/dns-resolve (spy/mock (fn [n]
                                             (case n
                                               "n1" "10.0.0.1"
                                               "n2" "10.0.0.2"
                                               "n3" "10.0.0.3")))]
    (is (= #{"n1" "n3"}
           (cass/live-nodes {:nodes ["n1" "n2" "n3"]
                             :decommissioned (atom #{"n2"})})))))

(deftest live-nodes-no-alive-test
  (with-redefs [cass/get-jmx-status (spy/stub nil)
                cass/dns-resolve (spy/mock (fn [n]
                                             (case n
                                               "n1" "10.0.0.1"
                                               "n2" "10.0.0.2"
                                               "n3" "10.0.0.3")))]
    (is (= #{}
           (cass/live-nodes {:nodes ["n1" "n2" "n3"]
                             :decommissioned (atom #{"n2"})})))))

(deftest joining-nodes-test
  (with-redefs [cass/get-jmx-status (spy/stub ["10.0.0.3"])
                cass/dns-resolve (spy/mock (fn [n]
                                             (case n
                                               "n1" "10.0.0.1"
                                               "n2" "10.0.0.2"
                                               "n3" "10.0.0.3")))]
    (is (= #{"n3"}
           (cass/live-nodes {:nodes ["n1" "n2" "n3"]
                             :decommissioned (atom #{"n2"})})))))

(deftest joining-nodes-no-joining-test
  (with-redefs [cass/get-jmx-status (spy/stub nil)
                cass/dns-resolve (spy/mock (fn [n]
                                             (case n
                                               "n1" "10.0.0.1"
                                               "n2" "10.0.0.2"
                                               "n3" "10.0.0.3")))]
    (is (= #{}
           (cass/live-nodes {:nodes ["n1" "n2" "n3"]
                             :decommissioned (atom #{"n2"})})))))

(deftest seed-nodes-test
  (is (= '("n1") (cass/seed-nodes {:nodes ["n1" "n2" "n3"] :rf 1})))
  (is (= '("n1" "n2") (cass/seed-nodes {:nodes ["n1" "n2" "n3"] :rf 3}))))

(deftest install-with-url-test
  (let [test {:tarball "http://some.where/tarball-file"}]
    (with-redefs [debian/install (spy/spy)
                  c/upload (spy/spy)
                  cu/install-archive! (spy/spy)]
      (cass/install! "n1" test)
      (is (spy/called-once? debian/install))
      (is (spy/not-called? c/upload))
      (is (spy/called-with? cu/install-archive! (:tarball test) "/root/cassandra")))))

(deftest install-with-local-test
  (let [test {:tarball "file:///local-dir/tarball-file"}]
    (with-redefs [debian/install (spy/spy)
                  c/upload (spy/spy)
                  cu/install-archive! (spy/spy)]
      (cass/install! "n1" test)
      (is (spy/called-once? debian/install))
      (is (spy/called-with? c/upload
                            "/local-dir/tarball-file"
                            "/tmp/cassandra.tar.gz"))
      (is (spy/called-with? cu/install-archive!
                            "file:///tmp/cassandra.tar.gz"
                            "/root/cassandra")))))
