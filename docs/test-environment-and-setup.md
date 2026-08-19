# Test Environment and Setup

Daily Jepsen tests run on GitHub-hosted `ubuntu-latest` runners. Environments are created in the workflow and torn down when the job ends. There is no Terraform or Azure VM retain step.

Local how-to for Docker, bare metal, and Cluster (including EKS) remains in the [root README](../README.md).

---

## Daily workflows

| Workflow | File | Schedule (UTC) | Environment |
|----------|------|----------------|-------------|
| Daily ScalarDB Cluster test | [`.github/workflows/daily-cluster.yml`](../.github/workflows/daily-cluster.yml) | `0 10 * * *` | Kind + MetalLB on the runner |
| Daily ScalarDL test | [`.github/workflows/daily-dl.yml`](../.github/workflows/daily-dl.yml) | `0 15 * * *` | Docker Compose (`docker/`) |
| Daily ScalarDB test | [`.github/workflows/daily-db.yml`](../.github/workflows/daily-db.yml) | `0 20 * * *` | Docker Compose (`docker/`) |

Related:

* **Update status page** ([`update-status.yml`](../.github/workflows/update-status.yml)) runs when a daily workflow completes and merges `result-*` into gh-pages `docs/data/status.json` (30-day history). Published at https://scalar-labs.github.io/scalar-jepsen/
* **ScalarDB Cluster Test (dispatch)** ([`cluster-test.yml`](../.github/workflows/cluster-test.yml)) is a manual Kind run with extra inputs (ref, version, backend, workload, nemesis).
* PR/push: `build-test.yml`, `fmt-lint.yml`, and a shorter smoke run in `test.yml` (not the daily matrix).

The three daily workflows are schedule-only (no `workflow_dispatch`).

---

## Docker environment (ScalarDB and ScalarDL)

Topology is defined in [`docker/docker-compose.yaml`](../docker/docker-compose.yaml):

* `jepsen-control` — JDK, Leiningen, graphviz; repo mounted at `/scalar-jepsen`
* `jepsen-n1` … `jepsen-n5` — privileged nodes with SSH

Daily DB/DL jobs:

1. Restore cached Docker images and Maven (`~/.m2`) when present.
2. `docker compose up -d` and wait until all six containers are running.
3. Copy m2 into the control container; `lein install` in `cassandra/`.
4. ScalarDL only: checkout `scalar-labs/scalardl`, `./gradlew distTar`, copy `ledger.tar` into `scalardl/resources/`.
5. `docker exec jepsen-control … lein … run test` with matrix options (`--concurrency 5`, `--time-limit 600`, `--ssh-private-key ~/.ssh/id_rsa`).
6. Copy `store` out of the control container; on failure run triage, Jira, then upload artifacts.

**ScalarDB daily matrix (11 jobs):** Transfers_SI, Transfers_2PC_SI, ReadCommitted, ReadCommitted_2PC, RCSI, RCSI_2PC, Serializable, Serializable_2PC, OnePhaseCommit_SI, OnePhaseCommit_Serializable, GroupCommit. Typical nemeses: `none`, `partition`, `crash`. Isolation/consistency flags are set per cell in `daily-db.yml`.

**ScalarDL daily matrix (3 jobs):** `nemesis-none`, `nemesis-crash`, `nemesis-partition`. The workflow currently passes `--workload all`.

---

## Kind environment (ScalarDB Cluster)

Daily Cluster jobs run on the runner itself (not inside `jepsen-control`):

1. Enable SSH to localhost for the runner user.
2. Java 21 + Leiningen.
3. Kind cluster `test-cluster` + MetalLB (L2 pool on the Kind Docker network).
4. `lein with-profile cluster run test --db <backend> …` with matrix workloads and nemeses (`none partition packet clock crash`), time-limit 600.

**Matrix (high level):** postgres feature cells matching the DB suite plus `ClientSideOptimizations`; Transfers and Elle pairs for alloydb, yugabytedb, mysql, mariadb, tidb, sqlserver, oracle, db2.

Daily Cluster is Kind-only. EKS StorageClass and LoadBalancer notes in the root README apply to manual/EKS runs, not to the scheduled daily.

---

## Artifacts and helper scripts

On every daily matrix job (`if: always()` unless noted):

| Artifact | Contents |
|----------|----------|
| `result-<name>` | One line: `<name>:success`, `failure`, or `timeout` |
| `logs-<name>` | Jepsen `store` copied from the control container (or Cluster working dir) |
| `triage-<name>` | `triage.md` (failure only; may be missing if triage failed) |

Scripts under [`.github/scripts/`](../.github/scripts/):

* `triage.py` — first-pass classification of `jepsen.log` (see [daily-test-checks.md](./daily-test-checks.md#ai-triage))
* `create_jira_ticket.sh` — Bug in Jira project `DLT`; embeds triage when present
* `notify_slack.sh` — run summary with triage labels on failed/timed-out tests

---

## Reproduce a failed cell locally

Match the failed job’s workload, nemesis, isolation, and backend from the Actions log or `daily-*.yml`.

**Docker (DB / DL)** — same as the root README:

```sh
cd docker
docker compose up -d
docker exec -it jepsen-control bash
# ScalarDB example
cd /scalar-jepsen/cassandra && lein install
cd /scalar-jepsen/scalardb
lein with-profile cassandra run test --workload transfer --nemesis none \
  --concurrency 5 --time-limit 600 --ssh-private-key ~/.ssh/id_rsa
```

**Cluster (Kind)** — install Kind, Helm, kubectl; SSH to localhost as in CI; then from `scalardb/`:

```sh
DOCKER_USERNAME=${GITHUB_USER} \
DOCKER_ACCESS_TOKEN=${GITHUB_ACCESS_TOKEN} \
lein with-profile cluster run test --workload transfer --db postgres \
  --nodes ${KUBERNETES_CLUSTER_HOST} --username ${USER}
```

Download `logs-<name>` from the failed run and compare `store/current/jepsen.log` with the local store.

---

## Secrets (names only)

| Secret / permission | Used for |
|---------------------|----------|
| `GITHUB_TOKEN` | Triage (GitHub Models); `models: read` on daily jobs |
| `GH_PAT` | Slack notify job and status-page artifact download |
| `SLACK_WEBHOOK_URL` | Daily Slack summary |
| `JIRA_AUTH`, `JIRA_ASSIGNEE_ID` | Failure tickets (`DLT`) |
| `CR_PAT` | GHCR / Cluster image pull |

Do not put secret values in tickets or this document.

Kelpie verification and benchmarks: [kelpie-test test environment and setup](https://github.com/scalar-labs/kelpie-test/blob/master/docs/test-environment-and-setup.md).
