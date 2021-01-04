(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalardb"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.2.1"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0"]
                 [software.amazon.awssdk/sdk-core "2.14.24"]
                 [com.scalar-labs/scalardb "2.4.0"
                  :exclusions [software.amazon.awssdk/core]]]
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}}
  :jvm-opts ["-Djava.awt.headless=true"]
  :main scalardb.runner
  :aot [scalardb.runner])
