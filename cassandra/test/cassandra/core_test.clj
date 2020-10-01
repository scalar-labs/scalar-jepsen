(ns cassandra.core-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.control.util :as cu]
            [jepsen.generator :as gen]
            [jepsen.os.debian :as debian]
            [cassandra.core :as cass]
            [qbits.alia :as alia]
            [spy.core :as spy])
  (:import (com.datastax.driver.core WriteType)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                ReadTimeoutException
                                                TransportException
                                                WriteTimeoutException
                                                UnavailableException)
           (java.net UnknownHostException)))

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
  (with-redefs [cass/get-jmx-status (spy/mock (fn [n _]
                                                (case n
                                                  "n1" ["10.0.0.1"]
                                                  "n2" ["10.0.0.2"]
                                                  "n3" ["10.0.0.3"])))]
    (is (= #{"10.0.0.1" "10.0.0.3"}
           (cass/joining-nodes {:nodes ["n1" "n2" "n3"]
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
  (let [test {:tarball "http://some.where/tarball-file"
              :cassandra-dir "/root/cassandra"}]
    (with-redefs [debian/install (spy/spy)
                  c/upload (spy/spy)
                  cu/install-archive! (spy/spy)]
      (cass/install! "n1" test)
      (is (spy/called-once? debian/install))
      (is (spy/not-called? c/upload))
      (is (spy/called-with? cu/install-archive!
                            (:tarball test)
                            (:cassandra-dir test))))))

(deftest install-with-local-test
  (let [test {:tarball "file:///local-dir/tarball-file"
              :cassandra-dir "/root/cassandra"}]
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
                            (:cassandra-dir test))))))

(deftest install-jdk-fail-test
  (let [test {:tarball "http://some.where/tarball-file"
              :cassandra-dir "/root/cassandra"}]
    (with-redefs [cass/exponential-backoff (spy/spy)
                  debian/install (spy/mock (fn [_]
                                             (throw (ex-info
                                                     "install failed!" {}))))
                  debian/update! (spy/spy)
                  c/upload (spy/spy)
                  cu/install-archive! (spy/spy)]
      (is (thrown? clojure.lang.ExceptionInfo (cass/install! "n1" test)))
      (is (spy/called-n-times? cass/exponential-backoff 7))
      (is (spy/not-called? c/upload))
      (is (spy/not-called? cu/install-archive!)))))

(deftest start-test
  (let [test {:cassandra-dir "/root/cassandra"}]
    (with-redefs [c/exec (spy/spy)]
      (cass/start! "n1" test)
      (is (spy/called-with? c/exec (c/lit  "/root/cassandra/bin/cassandra -R"))))))

(deftest gurded-start-test
  (let [test {:cassandra-dir "/root/cassandra"
              :decommissioned (atom #{})}]
    (with-redefs [c/exec (spy/spy)]
      (cass/guarded-start! "n1" test)
      (is (spy/called-with? c/exec (c/lit  "/root/cassandra/bin/cassandra -R"))))))

(deftest guarded-start-decommissioned-node-test
  (let [test {:cassandra-dir "/root/cassandra"
              :decommissioned (atom #{"n3"})}]
    (with-redefs [c/exec (spy/spy)]
      (cass/guarded-start! "n3" test)
      (is (spy/not-called? c/exec)))))

(deftest stop-test
  (with-redefs [c/exec (spy/mock (fn [cmd & _]
                                   (case cmd
                                     :killall "kill"
                                     :ps "no cassandra")))]
    (cass/stop! "n1")
    (is (spy/called-with? c/exec :killall :java))))

(deftest wipe-test
  (let [test {:cassandra-dir "/root/cassandra"}]
    (with-redefs [c/exec (spy/mock (fn [cmd & _]
                                     (case cmd
                                       :killall "kill"
                                       :ps "no cassandra")))]
      (cass/wipe! test "n1")
      (is (spy/called-with? c/exec :killall :java))
      (is (spy/called-with? c/exec :rm :-r "/root/cassandra/logs"))
      (is (spy/called-with? c/exec :rm :-r "/root/cassandra/data")))))

(deftest db-setup-test
  (with-redefs [cass/wipe! (spy/spy)
                cass/install! (spy/spy)
                cass/configure! (spy/spy)
                cass/wait-turn (spy/spy)
                cass/guarded-start! (spy/spy)]
    (let [test {}
          cassandra (cass/db)]
      (db/setup! cassandra test "n1")
      (is (spy/not-called? cass/wipe!))
      (is (spy/called-once? cass/install!))
      (is (spy/called-once? cass/configure!))
      (is (spy/called-once? cass/wait-turn))
      (is (spy/called-once? cass/guarded-start!)))))

(deftest db-teardown-test
  (with-redefs [cass/wipe! (spy/spy)]
    (let [test {}
          cassandra (cass/db)]
      (db/teardown! cassandra test "n1")
      (is (spy/called-once? cass/wipe!)))))

(deftest db-log-files-test
  (let [test {}
        cassandra (cass/db)]
    (is (= [] (db/log-files cassandra test "n1")))))

(deftest create-my-keyspace-test
  (with-redefs [alia/execute (spy/spy)]
    (let [test {:rf 3}
          schema {:keyspace "test_keyspace"}]
      (cass/create-my-keyspace "session" test schema)
      (is (spy/called-with? alia/execute "session"
                            {:create-keyspace :test_keyspace
                             :if-exists false
                             :with {:replication {"class" "SimpleStrategy"
                                                  "replication_factor" 3}}})))))

(deftest create-my-table-test
  (with-redefs [alia/execute (spy/spy)]
    (let [schema {:keyspace "test_keyspace"
                  :table "test_table"
                  :schema {:id :text
                           :count :int
                           :primary-key [:id]}
                  :compaction-strategy :LeveledCompactionStrategy}]
      (cass/create-my-table "session" schema)
      (prn (meta alia/execute))
      (is (spy/called-with? alia/execute "session"
                            {:use-keyspace :test_keyspace}))
      (is (spy/called-with? alia/execute "session"
                            {:create-table :test_table
                             :if-exists false
                             :column-definitions (:schema schema)
                             :with {:compaction
                                    {:class :LeveledCompactionStrategy}}})))))

(deftest handle-exception-test
  (let [op {}
        cas-timeout (ex-info "Write timed out for CAS"
                             {:type :execute
                              :exception
                              (WriteTimeoutException. nil nil
                                                      WriteType/CAS
                                                      0 0)})
        simple-timeout (ex-info "Write timed out for SIMPLE"
                                {:type :execute
                                 :exception
                                 (WriteTimeoutException. nil nil
                                                         WriteType/SIMPLE
                                                         0 0)})
        batch-log-timeout (ex-info "Write timed out for BATCH_LOG"
                                   {:type :execute
                                    :exception
                                    (WriteTimeoutException. nil nil
                                                            WriteType/BATCH_LOG
                                                            0 0)})
        batch-timeout (ex-info "Write timed out for BATCH"
                               {:type :execute
                                :exception
                                (WriteTimeoutException. nil nil
                                                        WriteType/BATCH
                                                        0 0)})

        read-timeout (ex-info "Read timed out"
                              {:type :execute
                               :exception (ReadTimeoutException. nil nil
                                                                 0 0 false)})
        node-down (ex-info "Transport failed"
                           {:type :execute
                            :exception (TransportException. nil nil)})
        unavailable (ex-info "Unavailable exception"
                             {:type :execute
                              :exception (UnavailableException. nil nil 0 0)})
        no-host (ex-info "No host available"
                         {:type :execute
                          :exception (NoHostAvailableException. {})})]
    (is (= {:type :info :error :write-timed-out}
           (cass/handle-exception op cas-timeout true)))
    (is (= {:type :ok}
           (cass/handle-exception op simple-timeout true)))
    (is (= {:type :info :error :write-timed-out}
           (cass/handle-exception op simple-timeout)))
    (is (= {:type :info :error :write-timed-out}
           (cass/handle-exception op batch-log-timeout)))
    (is (= {:type :ok}
           (cass/handle-exception op batch-timeout)))
    (is (= {:type :fail :error :read-timed-out}
           (cass/handle-exception op read-timeout)))
    (is (= {:type :info :error :node-down}
           (cass/handle-exception op node-down)))
    (is (= {:type :fail :error :unavailable}
           (cass/handle-exception op unavailable)))
    (is (= {:type :fail :error :no-host-available}
           (cass/handle-exception op no-host)))))
