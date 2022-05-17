(defproject scalardl "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DL"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.2.1"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0"]]
  :repositories {"sonartype" "https://oss.sonatype.org/content/repositories/snapshots"}
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :use-released {:dependencies [[com.scalar-labs/scalardl-java-client-sdk "4.0.0-SNAPSHOT'"
                                            :exclusions [org.slf4j/slf4j-log4j12
                                                         com.oracle.database.jdbc/ojdbc8-production
                                                         software.amazon.awssdk/*]]]}
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
  :aot :all)
