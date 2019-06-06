# Jepsen tests for Scalar DB

This guide will teach you how to run Jepsen tests for Scalar DB.
The current tests use [Cassandra test tools in Jepsen](https://github.com/scalar-labs/scalar-jepsen/tree/cassandra).

## How to test
1. Install the Cassandra test tool

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/cassandra
    $ lein install
    ```

2. Run a test of Scalar DB

    ```
    # in jepsen-control

    $ cd ${SCALAR_JEPSEN}/scalardb
    $ lein run test --test transfer --nemesis crash --join decommission --time-limit 300
    ```

  - See `lein run test --help` for full options
