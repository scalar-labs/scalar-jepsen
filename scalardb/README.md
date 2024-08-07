# Jepsen tests for ScalarDB

This guide will teach you how to run Jepsen tests for ScalarDB.
The current tests use [Cassandra test tools in Jepsen](https://github.com/scalar-labs/scalar-jepsen/tree/cassandra).

## How to test
1. Start the Docker nodes or multiple machines and log into jepsen-control (as explained [here](https://github.com/scalar-labs/scalar-jepsen/tree/README.md)).

2. Install the Cassandra test tool

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/cassandra
    $ lein install
    ```

3. Run a test of ScalarDB

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/scalardb
    $ lein run test --workload transfer --nemesis crash --admin join --time-limit 300 --ssh-private-key ~/.ssh/id_rsa
    ```

  - For elle-* tests, `graphviz` package is required in jepsen-control. You can install it with `apt-get install graphviz`
  - See `lein run test --help` for full options
