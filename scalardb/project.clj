(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.1" :exclusions [net.java.dev.jna/jna
                                              net.java.dev.jna/jna-platform]]
                 [net.java.dev.jna/jna "5.11.0"]
                 [net.java.dev.jna/jna-platform "5.11.0"]
                 [org.slf4j/slf4j-jdk14 "2.0.6"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0"]]
  :repositories {"sonartype" "https://oss.sonatype.org/content/repositories/snapshots/"}
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :use-released {:dependencies [[com.scalar-labs/scalardb "4.0.0-SNAPSHOT"
                                            ;; avoid the netty dependency issue
                                            :exclusions [software.amazon.awssdk/*
                                                         com.oracle.database.jdbc/ojdbc8-production
                                                         com.azure/azure-cosmos
                                                         io.grpc/grpc-core
                                                         com.scalar-labs/scalardb-rpc]]]}
             :use-jars {:dependencies [[com.google.guava/guava "31.1-jre"]
                                       [org.apache.commons/commons-text "1.10.0"]]
                        :resource-paths ["resources/scalardb.jar"]}
             :default [:base :system :user :provided :dev :use-released]}
  :jvm-opts ["-Djava.awt.headless=true"]
  :main scalardb.runner
  :aot [scalardb.runner])
