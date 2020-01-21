(defproject scalardl "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DL"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.1.15"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.1"]
                 [cc.qbits/hayt "4.1.0"]
                 [com.scalar-labs/scalardl-java-client-sdk "2.0.0"]]
                 ; :exclusions [[org.slf4j/slf4j-log4j12]
                 ;              [com.google.guava/guava]]]]
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}
             :latest {:dependencies [[org.bouncycastle/bcpkix-jdk15on "1.59"]
                                     [org.bouncycastle/bcprov-jdk15on "1.59"]
                                     [javax.json/javax.json-api "1.1.4"]
                                     [com.google.inject/guice "4.2.0"]
                                     [com.google.api.grpc/proto-google-common-protos "1.0.0"]
                                     [io.grpc/grpc-alts "1.13.2"]
                                     [io.grpc/grpc-netty "1.13.2"]
                                     [io.grpc/grpc-protobuf "1.13.2"]
                                     [io.grpc/grpc-stub "1.13.2"]
                                     [org.glassfish/javax.json "1.1.4"]]
                      :resource-paths ["resources/client.jar"
                                       "resources/common.jar"
                                       "resources/ledger-client.jar"
                                       "resources/rpc.jar"]}}
  :java-source-paths ["contract"]
  :main scalardl.runner
  :aot :all)
