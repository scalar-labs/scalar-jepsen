![Build and Test](https://github.com/scalar-labs/scalar-jepsen/actions/workflows/build-test.yml/badge.svg)
![Test with Docker](https://github.com/scalar-labs/scalar-jepsen/actions/workflows/test.yml/badge.svg)

# Run tests with Jepsen Docker
1. Move to the docker directory
```sh
$ cd docker
```
2. Run docker-compose
```sh
$ docker-compose up -d
```
3. Enter the control node
```sh
$ docker exec -it jepsen-control bash
```
4. Run a test
```
# cd /scalar-jepsen/cassandra
# lein run test --workload lwt --ssh-private-key ~/.ssh/id_rsa
```
  - Check README in each test for more detail
  - `--ssh-private-key` should be always set to specify the SSH key
  - `--workload` specifies a test workload. previously we used `--test`.
  - `--nemesis` sets `none`, `partition`, `clock`, or `crash` (default: `none`)
  - `--admin` sets `none`, `join` or `flush` (default: `none`)

# Run tests without Docker
1. Launch debian machines as a control machine and Cassandra nodes
  - We recommend 1 control machine and 5 node machines
    - You can decrease the number of nodes. If you do so then you will need to specify the nodes when starting a test.
2. Install Java 17 on each machine
```sh
sudo apt install openjdk-17-jre
```

3. Install Leiningen (https://leiningen.org/) on the control machine
4. Make an SSH key pair for Jepsen to login nodes from the control machine
5. Register the public key as root on each node

```sh
$ sudo echo ssh-rsa ... >> /root/.ssh/authorized_keys
```

6. Configure `/etc/hosts` on each machine

```sh
$ sudo sh -c "cat << EOF >> /etc/hosts
<NODE1_IP> n1
<NODE2_IP> n2
<NODE3_IP> n3
<NODE4_IP> n4
<NODE5_IP> n5
EOF"
```

7. Run a test on the control machine

```sh
$ cd ${SCALAR_JEPSEN}/cassandra
$ lein run test --workload lwt --ssh-private-key ~/.ssh/id_rsa
```

# Run ScalarDB Cluster test
This test runs against a Kubernetes cluster and requires `kind` (or a similar tool), `helm`, and `kubectl` to be installed beforehand.

The test will attempt to SSH into a machine where the Kubernetes cluster is deployed, so SSH access must be properly configured in advance.

Below is an example command to run the ScalarDB Cluster test:
```sh
DOCKER_USERNAME=${GITHUB_USER} \
DOCKER_ACCESS_TOKEN=${GITHUB_ACCESS_TOKEN} \
lein with-profile cluster run test --workload transfer --db postgres \
    --nodes ${KUBERNETES_CLUSTER_HOST} \
    --username ${USER}
```
  - `KUBERNETES_CLUSTER_HOST`: The IP address or hostname of the machine running the Kubernetes cluster.
  - `USER`: The SSH username used to connect to the Kubernetes cluster host. If needed, `--password` or `--ssh-private-key` can be added.
  - `GITHUB_USER`: Your GitHub username, used to authenticate with ghcr.io for pulling the ScalarDB Cluster Docker image.
  - `GITHUB_ACCESS_TOKEN`: A GitHub access token with permissions to pull images from ghcr.io.
  - All other parameters match those used in standard ScalarDB tests

## Prerequisite for Amazon EKS: a default StorageClass
The test provisions a backend database (e.g. PostgreSQL) whose pod requests a `PersistentVolumeClaim`. A local `kind` cluster ships with a default `StorageClass`, but **an EKS cluster has none by default**, so the PVC stays `Pending` and the backend pod never starts. Create a default `StorageClass` before running the test.

For an EKS Auto Mode cluster:
```sh
kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: auto-ebs-sc
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.eks.amazonaws.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  type: gp3
  encrypted: "true"
EOF
```
  - On a non-Auto-Mode EKS cluster, install the EBS CSI driver add-on and use the `ebs.csi.aws.com` provisioner instead.
  - Exactly one `StorageClass` should carry the `storageclass.kubernetes.io/is-default-class: "true"` annotation.
  - Verify with `kubectl get storageclass`; the default is shown as `... (default)`.
  - This is cluster setup, so it's best added to whatever provisions your EKS cluster (e.g. your IaC) rather than applied by hand each time.

## Prerequisite for Amazon EKS: external access to the LoadBalancers
The Jepsen control connects to the cluster over `LoadBalancer` services: the ScalarDB Cluster (Envoy) for transactions and schema loading, and the backend DB, which the test's checker reaches directly via the ScalarDB storage API. With a local `kind` cluster (using MetalLB) these are reachable from the control machine, but on EKS they are not reachable from outside the VPC by default. When the control runs outside the VPC (e.g. on your laptop), complete the one-time setup below **before** running the test.

1. Tag the public subnets so the AWS load balancer controller can place internet-facing NLBs. This must be done before the test creates the `LoadBalancer` services; otherwise they stay `Pending` with a `FailedBuildModel` event, and tagging afterwards does not retrigger provisioning.
```sh
aws ec2 create-tags --region ${REGION} \
    --resources ${SUBNET_ID_1} ${SUBNET_ID_2} ${SUBNET_ID_3} \
    --tags Key=kubernetes.io/role/elb,Value=1
```

2. Allow the control machine's IP in the load balancers' security group on the relevant ports (e.g. `60053` for Envoy, `5432` for PostgreSQL).

Then run the test, pointing it at your EKS cluster and requesting internet-facing LoadBalancers:
```sh
lein with-profile cluster run test --workload transfer --db postgres \
    --kubeconfig ~/.kube/eks-jepsen-test --lb-internet-facing
```
  - `--kubeconfig FILE`: kubeconfig file for kubectl/helm (uses that file's current context).
  - `--lb-internet-facing`: annotates the Envoy and backend DB `LoadBalancer` services as internet-facing so the control can reach them.

Notes:
  - The subnet tags and security group rules (like the `StorageClass`) are one-time cluster setup that persists across test runs — the test's wipe does not remove them — so they are best handled by your IaC rather than applied each run.
  - Db2 and Oracle backends are exposed via a NodePort on the Kubernetes node rather than a `LoadBalancer`, so `--lb-internet-facing` does not apply to them; external access to those requires the node's external IP and a security group rule on the NodePort.
