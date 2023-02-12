# Jepsen tests for Scalar DL

The Scalar DL Jepsen tests make use of the [Cassandra Jepsen tests](https://github.com/scalar-labs/scalar-jepsen/tree/master/cassandra).

## How to run a test
1. Get Scalar DL
  - Scalar DL is licensed under commercial license only
    - A test checks `resources/ledger.tar` as default
    - You can specify the DL archive by `--ledger-tarball` option
  - A certificate and a private key for a sample is stored in `resources`
    - You can specify your certificate or key by `--cert` or `--client-key` option

2. Start Jepsen with docker or start machines
  - See [here](https://github.com/scalar-labs/scalar-jepsen)

3. Install the Cassandra test tool

    ```
    # in jepsen-control

    $ cd /scalar-jepsen/cassandra
    $ lein install
    ```

4. Run a Scalar DL Jepsen test

    ```
    # in jepsen-control

    $ cd /scalar-jepsen/scalardl
    $ lein run test --workload cas
    ```

    Use `lein run test --help` to see a list of the options.
