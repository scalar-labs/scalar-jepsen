(ns scalardb.db.cluster-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.k8s.core :as k8s]
            [scalardb.db.cluster :as cluster]
            [scalardb.db.cluster-db.cluster-db :as cluster-db]
            [spy.core :as spy]))

(def ^:private find-load-balancer-ip
  #'cluster/find-load-balancer-ip)

(def ^:private expose-loadbalancers!
  #'cluster/expose-loadbalancers!)

(defn- lb-service
  [name ingress]
  {:metadata {:name name}
   :spec {:type "LoadBalancer"}
   :status {:loadBalancer {:ingress [ingress]}}})

(deftest find-load-balancer-ip-test
  (testing "returns the ingress IP when present"
    (with-redefs [k8s/services
                  (spy/stub {:items [(lb-service "postgres" {:ip "1.2.3.4"})]})]
      (is (= "1.2.3.4" (find-load-balancer-ip {} "postgres")))))

  (testing "falls back to the ingress hostname when there is no IP (e.g. AWS)"
    (with-redefs [k8s/services
                  (spy/stub {:items [(lb-service "postgres"
                                                 {:hostname "abc.elb.amazonaws.com"})]})]
      (is (= "abc.elb.amazonaws.com" (find-load-balancer-ip {} "postgres")))))

  (testing "matches services by name prefix"
    (with-redefs [k8s/services
                  (spy/stub {:items [(lb-service "scalardb-cluster-envoy"
                                                 {:ip "10.0.0.9"})
                                     (lb-service "postgres" {:ip "1.2.3.4"})]})]
      (is (= "10.0.0.9"
             (find-load-balancer-ip {} "scalardb-cluster-envoy")))))

  (testing "ignores non-LoadBalancer services"
    (with-redefs [k8s/services
                  (spy/stub {:items [{:metadata {:name "postgres"}
                                      :spec {:type "ClusterIP"}
                                      :status {}}]})]
      (is (nil? (find-load-balancer-ip {} "postgres"))))))

(defn- node
  [addresses]
  {:status {:addresses addresses}})

(deftest get-k8s-node-ip-test
  (testing "prefers the node ExternalIP when it reports one (e.g. EKS/GKE)"
    (with-redefs [k8s/nodes
                  (spy/stub {:items [(node [{:type "ExternalIP" :address "1.2.3.4"}
                                            {:type "InternalIP" :address "10.0.0.1"}])]})]
      (is (= "1.2.3.4" (cluster/get-k8s-node-ip {})))))

  (testing "falls back to the InternalIP when no ExternalIP (e.g. kind)"
    (with-redefs [k8s/nodes
                  (spy/stub {:items [(node [{:type "InternalIP" :address "10.0.0.1"}])]})]
      (is (= "10.0.0.1" (cluster/get-k8s-node-ip {}))))))

(defn- backend-db
  [lb-name]
  (reify cluster-db/ClusterDb
    (get-lb-service-name [_] lb-name)))

(defn- annotated-services
  "Runs expose-loadbalancers! against a fixed service list and returns the set of
  service names that got the internet-facing annotation."
  [test lb-name services]
  (let [annotate (spy/spy)]
    (with-redefs [k8s/services (spy/stub {:items services})
                  k8s/kubectl! annotate]
      (expose-loadbalancers! test (backend-db lb-name)))
    (->> (spy/calls annotate)
         ;; each call is (test :annotate :svc <name> :-n <ns> ...)
         (map #(nth % 3))
         set)))

(deftest expose-loadbalancers-test
  (let [services [(lb-service "scalardb-cluster-envoy" {:hostname "envoy.elb"})
                  (lb-service "postgresql-scalardb-cluster" {:hostname "pg.elb"})
                  (lb-service "some-leftover-lb" {:hostname "leftover.elb"})
                  {:metadata {:name "scalardb-cluster"}
                   :spec {:type "ClusterIP"}
                   :status {}}]]
    (testing "annotates only the Envoy and this backend's LoadBalancer"
      (is (= #{"scalardb-cluster-envoy" "postgresql-scalardb-cluster"}
             (annotated-services {:name "scalardb-transfer" :lb-internet-facing true}
                                 "postgresql-scalardb-cluster"
                                 services))))

    (testing "leaves unrelated/leftover LoadBalancers untouched"
      (is (not (contains? (annotated-services {:name "scalardb-transfer" :lb-internet-facing true}
                                              "postgresql-scalardb-cluster"
                                              services)
                          "some-leftover-lb"))))

    (testing "annotates only Envoy when the backend has no LoadBalancer (NodePort)"
      (is (= #{"scalardb-cluster-envoy"}
             (annotated-services {:name "scalardb-transfer" :lb-internet-facing true}
                                 nil services))))

    (testing "does nothing unless --lb-internet-facing is set"
      (is (empty? (annotated-services {:name "scalardb-transfer" :lb-internet-facing false}
                                      "postgresql-scalardb-cluster"
                                      services))))))
