# Daily Test Types

What each scheduled Jepsen matrix job checks. For schedules and how CI runs, see [test-environment-and-setup.md](./test-environment-and-setup.md). For failure handling, see [daily-test-checks.md](./daily-test-checks.md).

Each matrix **job name** usually expands into several Jepsen runs (every listed workload × every listed nemesis), typically about 600 seconds each.

---

## Terms used below

| Term | Meaning |
|------|---------|
| **transfer / transfer-append** | Bank-style money moves between accounts. The checker verifies that total balance (and related history) is conserved. `transfer-append` keeps append-only age history. |
| **elle-append / elle-write-read** | Elle anomaly tests (list-append and write/read register histories). The checker looks for cycles/anomalies against a chosen consistency model. |
| **isolation** | ScalarDB Consensus Commit isolation configured for the run (`read-committed`, `snapshot`, `serializable`). |
| **consistency model** | What Elle *checks for* (`cursor-stability`, `snapshot-isolation`, `strict-serializable`). Not always the same string as isolation. |
| **nemesis** | Injected faults: `none` (no fault), `partition` (network split), `crash` (kill/restart). Cluster also uses `packet` (packet loss) and `clock` (clock skew). |

---

## Daily ScalarDB test (Cassandra)

Workflow: [`daily-db.yml`](../.github/workflows/daily-db.yml). Storage: Cassandra via the Jepsen Docker environment. Nemeses for every cell: `none`, `partition`, `crash`.

| Test | Workloads | Isolation | Consistency check | Features | What it verifies |
|------|-----------|-----------|-------------------|----------|------------------|
| **Transfers_SI** | `transfer`, `transfer-append` | snapshot | snapshot-isolation | — | Money-transfer correctness under snapshot isolation, with and without network/process faults. |
| **ReadCommitted** | `elle-append`, `elle-write-read` | read-committed | cursor-stability | — | Elle histories stay within cursor-stability when ScalarDB runs at read-committed. |
| **RCSI** | `elle-append`, `elle-write-read` | snapshot | cursor-stability | — | Snapshot-configured transactions checked against cursor-stability (RCSI-style pairing). |
| **Serializable** | `elle-append`, `elle-write-read` | serializable | strict-serializable | — | Strongest isolation + Elle check in the DB daily matrix. |
| **OnePhaseCommit_SI** | `elle-append`, `elle-write-read` | snapshot | cursor-stability | one-phase commit | Same as RCSI-style Elle, but with one-phase commit enabled when safe. |
| **OnePhaseCommit_Serializable** | `elle-append`, `elle-write-read` | serializable | strict-serializable | one-phase commit | Serializable Elle with one-phase commit enabled. |
| **GroupCommit** | `elle-append`, `elle-write-read` | serializable | strict-serializable | group commit | Serializable Elle with coordinator group commit (batched commits). |

---

## Daily ScalarDL test

Workflow: [`daily-dl.yml`](../.github/workflows/daily-dl.yml). Workload `all` means both **cas** and **transfer** (run one after the other). Cells differ only by fault injection.

| Test | Workloads | Nemesis | What it verifies |
|------|-----------|---------|------------------|
| **nemesis-none** | cas, transfer | none | Baseline ScalarDL correctness with no injected faults. |
| **nemesis-crash** | cas, transfer | crash | Same workloads while processes are killed and restarted. |
| **nemesis-partition** | cas, transfer | partition | Same workloads under network partitions. |

| Workload | What it does |
|----------|--------------|
| **cas** | Compare-and-swap style register via ScalarDL contracts; checked for linearizability. |
| **transfer** | Multi-account transfers via ScalarDL contracts; checker verifies total balance is conserved. |

---

## Daily ScalarDB Cluster test (Kind)

Workflow: [`daily-cluster.yml`](../.github/workflows/daily-cluster.yml). Runs against ScalarDB Cluster on Kind. Nemeses for every cell: `none`, `partition`, `packet`, `clock`, `crash`.

### Postgres feature cells

Same ideas as the ScalarDB (Cassandra) daily, plus Cluster-only **ClientSideOptimizations**. Backend JDBC isolation is `read-committed`.

| Test | Workloads | Isolation | Consistency check | Features | What it verifies |
|------|-----------|-----------|-------------------|----------|------------------|
| **Transfers_SI** | `transfer`, `transfer-append` | snapshot | snapshot-isolation | — | Transfer conservation on Cluster + Postgres under the wider fault set. |
| **ReadCommitted** | `elle-append`, `elle-write-read` | read-committed | cursor-stability | — | Elle at read-committed / cursor-stability on Cluster. |
| **RCSI** | `elle-append`, `elle-write-read` | snapshot | cursor-stability | — | Snapshot + cursor-stability pairing on Cluster. |
| **Serializable** | `elle-append`, `elle-write-read` | serializable | strict-serializable | — | Strongest Elle pair on Cluster + Postgres. |
| **OnePhaseCommit_SI** | `elle-append`, `elle-write-read` | snapshot | cursor-stability | one-phase commit | Elle with one-phase commit on Cluster. |
| **OnePhaseCommit_Serializable** | `elle-append`, `elle-write-read` | serializable | strict-serializable | one-phase commit | Serializable Elle with one-phase commit on Cluster. |
| **GroupCommit** | `elle-append`, `elle-write-read` | serializable | strict-serializable | group commit | Serializable Elle with group commit on Cluster. |
| **ClientSideOptimizations** | `elle-append`, `elle-write-read` | snapshot | cursor-stability | client piggyback-begin + write buffering | Ensures Cluster client optimizations stay correct under faults. |

### Backend coverage (Transfers vs Elle)

For each backend below, two jobs run with isolation `snapshot` and consistency check `cursor-stability`:

| Test pattern | Workloads | What it verifies |
|--------------|-----------|------------------|
| **`<Backend>_Transfers`** | `transfer`, `transfer-append` | Balance conservation through Cluster on that backend. |
| **`<Backend>_Elle`** | `elle-append`, `elle-write-read` | Elle anomaly detection through Cluster on that backend. |

| Backend test names | Backend |
|--------------------|---------|
| AlloyDB_Transfers, AlloyDB_Elle | AlloyDB |
| YugabyteDB_Transfers, YugabyteDB_Elle | YugabyteDB |
| MySQL_Transfers, MySQL_Elle | MySQL |
| MariaDB_Transfers, MariaDB_Elle | MariaDB |
| TiDB_Transfers, TiDB_Elle | TiDB |
| SQLServer_Transfers, SQLServer_Elle | SQL Server |
| Oracle_Transfers, Oracle_Elle | Oracle |
| Db2_Transfers, Db2_Elle | Db2 |
| Cassandra_Transfers, Cassandra_Elle | Cassandra |

**Transfers vs Elle (short):** Transfers ask “did the money stay conserved?” Elle asks “did the observed history violate the consistency model?” They are complementary checks, not different isolation modes.
