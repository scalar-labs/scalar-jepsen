
# Run tests with Jepsen Docker
1. Clone [Jepsen repository](https://github.com/jepsen-io/jepsen)
2. Configure to mount `scalar-jepsen` directory by editing `${JEPSEN_ROOT}/docker/docker-compose.dev.yml` like the following

```yaml
   control:
     volumes:
       - ${JEPSEN_ROOT}:/jepsen # Mounts $JEPSEN_ROOT on host to /jepsen control container
       - ${SCALAR_JEPSEN_ROOT}:/scalar-jepsen # $SCALAR_JEPSEN_ROOT is the path of this repository on host
```

3. Start docker with `--dev` option

```sh
$ cd ${JEPSEN_ROOT}/docker
$ ./up.sh --dev
```

4. Run tests in `jepsen-control`
  - You can login `jepsen-control` by the following command
  ```sh
  $ docker exec -it jepsen-control bash
  ```

```
# cd /scalar-jepsen/cassandra
# lein run test --test lwt
```

  - Check README in each test for more detail

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
$ lein run test --test lwt --ssh-private-key ~/.ssh/id_rsa
```
