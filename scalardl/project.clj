(defproject scalardl "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DL"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3" :exclusions [net.java.dev.jna/jna
                                              net.java.dev.jna/jna-platform]]
                 [net.java.dev.jna/jna "5.11.0"]
                 [net.java.dev.jna/jna-platform "5.11.0"]
                 [org.slf4j/slf4j-jdk14 "2.0.6"]
                 [cassandra "0.1.0-SNAPSHOT" :exclusions [org.apache.commons/commons-lang3]]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0" :exclusions [org.apache.commons/commons-lang3]]]
  :repositories {"sonartype" "https://central.sonatype.com/repository/maven-snapshots/"}
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :use-released {:dependencies [[com.scalar-labs/scalardl-java-client-sdk "4.0.0-SNAPSHOT"
                                            :exclusions [org.slf4j/slf4j-log4j12
                                                         com.scalar-labs/scalardb]]]}
             :use-jars {:dependencies [[org.bouncycastle/bcpkix-jdk15on "1.59"]
                                       [org.bouncycastle/bcprov-jdk15on "1.59"]
                                       [com.google.inject/guice "4.2.0"]
                                       [com.moandjiezana.toml/toml4j "0.7.2"]
                                       [com.google.protobuf/protobuf-java-util "3.19.2"]
                                       [com.scalar-labs/scalar-admin "1.0.0"
                                        :exclusions [org.slf4j/slf4j-log4j12]]]
                        :resource-paths ["resources/scalardl-java-client-sdk.jar"
                                         "resources/scalardl-common.jar"
                                         "resources/scalardl-rpc.jar"]}
             :default [:base :system :user :provided :dev :use-released]}
  :java-source-paths ["contract"]
  :main scalardl.runner
  :aot :all
  :pedantic? false)
