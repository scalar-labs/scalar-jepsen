(defproject scalardb "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Scalar DB"
  :url "https://github.com/scalar-labs/scalardb"
  :license {:name ""
            :url ""}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [jepsen "0.1.15"]
                 [cassandra "0.1.0-SNAPSHOT"]
                 [cc.qbits/alia "4.3.1"]
                 [cc.qbits/hayt "4.1.0"]
                 [com.scalar-labs/scalardb "2.0.0" :exclusions [org.slf4j/slf4j-log4j12]]]
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}}
  :main scalardb.runner
  :aot [scalardb.runner])
