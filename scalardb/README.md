# Jepsen tests for Scalar DB

This guide will teach you how to run Jepsen tests for Scalar DB.
The current tests use [Cassandra test tools in Jepsen](https://github.com/scalar-labs/scalar-jepsen/tree/cassandra).

## How to test
1. Start the Docker nodes or multiple machines and log into jepsen-control (as explained [here](https://github.com/scalar-labs/scalar-jepsen/tree/README.md)).

2. Install the Cassandra test tool

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/cassandra
    $ lein install
    ```

3. Run a test of Scalar DB

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/scalardb
    $ lein run test --test transfer --nemesis crash --join decommission --time-limit 300
    ```

  - See `lein run test --help` for full options
