(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.2.1"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0"]]
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :use-released {:dependencies [[com.scalar-labs/scalardb "3.2.0"
                                            ;; avoid the netty dependency issue
                                            :exclusions [software.amazon.awssdk/*
                                                         com.oracle.database.jdbc/ojdbc8-production
                                                         com.azure/azure-cosmos
                                                         io.grpc/grpc-core
                                                         com.scalar-labs/scalardb-rpc]]]}
             :use-jars {:dependencies [[com.google.inject/guice "4.2.0"]
                                       [com.google.guava/guava "31.1-jre"]]
                        :resource-paths ["resources/scalardb.jar"]}
             :default [:base :system :user :provided :dev :use-released]}
  :jvm-opts ["-Djava.awt.headless=true"]
  :main scalardb.runner
  :aot [scalardb.runner])
