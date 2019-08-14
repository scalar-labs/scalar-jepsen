(defproject scalardl "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DL"
  :url "https://github.com/scalar-labs/scalar-jepsen"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.13"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.1"]
                 [cc.qbits/hayt "4.1.0"]
                 [com.scalar-labs/scalardl-client-sdk "1.1.0" :exclusions [org.slf4j/slf4j-log4j12]]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  [tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.1"]]}}
  :java-source-paths ["contract"]
  :main scalardl.runner
  :aot :all)
