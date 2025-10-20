(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3" :exclusions [net.java.dev.jna/jna
                                              net.java.dev.jna/jna-platform]]
                 [net.java.dev.jna/jna "5.11.0"]
                 [net.java.dev.jna/jna-platform "5.11.0"]
                 [org.slf4j/slf4j-jdk14 "2.0.6"]
                 [cheshire "5.12.0"]
                 [clj-commons/clj-yaml "1.0.29"]
                 [com.scalar-labs/scalardb-schema-loader "4.0.0-SNAPSHOT" :exclusions [com.scalar-labs/scalardb]]
                 [environ "1.2.0"]]
  :repositories {"sonatype" "https://central.sonatype.com/repository/maven-snapshots/"}
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :use-released {:dependencies [[com.scalar-labs/scalardb "4.0.0-SNAPSHOT"
                                            ;; avoid the netty dependency issue
                                            :exclusions [software.amazon.awssdk/*
                                                         com.oracle.database.jdbc/ojdbc8-production
                                                         com.azure/azure-cosmos
                                                         com.google.cloud/alloydb-jdbc-connector]]]}
             :cassandra {:dependencies [[cassandra "0.1.0-SNAPSHOT"
                                         :exclusions [org.apache.commons/commons-lang3]]
                                        [com.scalar-labs/scalardb "4.0.0-SNAPSHOT"
                                         ;; avoid the netty dependency issue
                                         :exclusions [software.amazon.awssdk/*
                                                      com.oracle.database.jdbc/ojdbc8-production
                                                      com.azure/azure-cosmos
                                                      com.google.cloud/alloydb-jdbc-connector]]]
                         :env {:cassandra? "true"}}
             :cluster {:dependencies [[com.scalar-labs/scalardb-cluster-java-client-sdk "4.0.0-SNAPSHOT"
                                       ;; avoid the netty and gRPC dependency issues
                                       :exclusions [software.amazon.awssdk/*
                                                    com.oracle.database.jdbc/ojdbc8-production
                                                    com.azure/azure-cosmos
                                                    com.datastax.cassandra/cassandra-driver-core
                                                    com.google.cloud/alloydb-jdbc-connector]]]
                       :env {:scalardb-cluster-version "4.0.0-SNAPSHOT"
                             :helm-chart-version "1.7.2"}}
             :use-jars {:dependencies [[com.google.guava/guava "31.1-jre"]
                                       [org.apache.commons/commons-text "1.13.0"]]
                        :resource-paths ["resources/scalardb.jar"]}
             :default [:base :system :user :provided :dev :use-released]}
  :plugins [[lein-environ "1.2.0"]]
  :jvm-opts ["-Djava.awt.headless=true" "-Xmx4g"]
  :main scalardb.runner
  :aot [scalardb.runner])
