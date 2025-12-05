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
2. Install Java8 on each machine
```sh
sudo apt install openjdk-8-jre
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
