# Cassandra tests with Jepsen

This is based on [riptano's jepsen](https://github.com/riptano/jepsen/tree/cassandra/cassandra).

## Current status
- Supports Apache Cassandra 3.11.x
- Support `collections.map-test`, `collections.set-test`, `batch-test`, `counter-test`(only add) and `lwt-test`
  - Removed `lww-test` and `mv-test`

## How to test

### Docker settings

You will probably need to increase the amount of memory that you make available to Docker in order to run these tests with Docker. We have had success using 8GB of memory and 2GB of swap. More, if you can spare it, is probably better.

### Run a test
1. Start the Docker nodes or multiple machines and log into jepsen-control (as explained [here](https://github.com/scalar-labs/scalar-jepsen/tree/README.md)).

2. Run a test

```
# in jepsen-control

$ cd ${SCALAR_JEPSEN}/cassandra
$ lein run test --test lwt --nemesis bridge --join bootstrap
```

- See `lein run test --help` for full options
