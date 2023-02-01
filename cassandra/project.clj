(defproject cassandra "0.1.0-SNAPSHOT"
  :description "Jepsen testing for Cassandra"
  :url "http://github.com/scalar-labs/jepsen"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/java.jmx "0.3.1"]
                 [jepsen "0.2.1"]
                 [cc.qbits/alia "4.3.6"]
                 [cc.qbits/hayt "4.1.0"]]
  :profiles {:dev {:dependencies [[tortue/spy "2.0.0"]]
                   :plugins [[lein-cloverage "1.1.2"]]}}
  :main cassandra.runner
  :aot [cassandra.runner])
