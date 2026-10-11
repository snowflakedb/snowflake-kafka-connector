# End-to-End Tests for Snowflake Kafka Connector

E2E tests spin up a Kafka cluster in Docker, send records, and verify they appear in Snowflake.

CI workflows: **`end-to-end.yaml`** (E2E) and **`end-to-end-stress.yml`** (stress tests) in `.github/workflows/`.

### Prerequisites

- Docker (with Docker Compose v2)
- Snowflake credentials (`profile.json`)
- Built connector plugin (run `build_runtime_jar.sh` first)

## Quick Start

```bash
# 1. Build the connector (from project root)
export SNOWFLAKE_CREDENTIAL_FILE=/path/to/profile.json
./test/build_runtime_jar.sh . package confluent   # or 'apache'

# 2. Run tests
cd test
./run_tests.sh --platform=confluent --platform-version=7.8.0
```

### Usage

```bash
./run_tests.sh --platform=<confluent|apache> --platform-version=<version> [options]

# Confluent examples:
./run_tests.sh --platform=confluent --platform-version=7.8.0
./run_tests.sh --platform=confluent --platform-version=6.2.15
./run_tests.sh --platform=confluent --platform-version=7.8.0 --tests=test_string_json

# Apache Kafka examples:
./run_tests.sh --platform=apache --platform-version=3.7.2
./run_tests.sh --platform=apache --platform-version=2.8.2 --java-version=11

# Options:
./run_tests.sh --platform=confluent --platform-version=7.8.0 -- -m pressure  # Stress tests
./run_tests.sh --platform=apache --platform-version=3.7.2 --keep             # Keep containers after tests
./run_tests.sh --platform=confluent --platform-version=7.8.0 --rebuild       # Force rebuild images
./run_tests.sh --platform=confluent --platform-version=7.8.0 --logs          # Show logs on failure
./run_tests.sh --platform=apache --platform-version=3.7.2 --cloud=AWS        # Target specific Snowflake cloud
```

### Supported Versions

**Confluent Platform:**
- `6.2.x` (e.g., 6.2.15)
- `7.x` (e.g., 7.6.0, 7.8.2)

**Apache Kafka:** Any version available as an official tarball (e.g., 2.8.2, 3.7.2)

## Architecture

The test environment uses layered Docker Compose files in `docker/`:

- `docker-compose.base.yml` -- test-runner container (shared by all platforms)
- `docker-compose.confluent.yml` -- Confluent Platform (Zookeeper, Kafka, Schema Registry, Kafka Connect as separate containers)
- `docker-compose.apache.yml` -- Apache Kafka (single container with embedded services)
- `docker-compose.amd64.yml` -- forces linux/amd64 emulation (Confluent 6.2.x on ARM)

### Confluent Platform

```
┌────────────────────────────────────────────────────────────┐
│  zookeeper   │    kafka     │schema-registry│kafka-connect │
│   :2181      │   :9092      │    :8081      │    :8083     │
├──────────────┴──────────────┴───────────────┴──────────────┤
│                      test-runner                           │
│              (Python + protobuf + tests)                   │
└────────────────────────────────────────────────────────────┘
```

### Apache Kafka

```
┌────────────────────────────────────────────────────────────┐
│                        kafka                               │
│    (Zookeeper + Kafka + Connect + Schema Registry)         │
│     :2181       :9092   :8083     :8081                    │
├────────────────────────────────────────────────────────────┤
│                      test-runner                           │
│              (Python + protobuf + tests)                   │
└────────────────────────────────────────────────────────────┘
```

### How E2E Tests Work

There are two test infrastructures that run side by side:

**Pytest (primary)** -- New tests live in `tests/` as standard pytest modules.
Fixtures in `conftest.py` handle connector lifecycle, table creation, and cleanup
(including Kafka topic deletion). The runner script passes connection addresses and
platform version as CLI options; see `conftest.py` for the full list.

**Legacy infra** -- Older tests live in `test_suit/` as classes with
`send`/`verify`/`clean` methods. They are orchestrated by `test_verify.py` via
`test_executor.py`. Connector config templates in `rest_request_template/` have a
one-to-one correspondence with these classes (e.g., `travis_correct_json_json.json`
maps to `test_suit/test_json_json.py`). The driver replaces placeholder values
(e.g., `SNOWFLAKE_TEST_TOPIC`, `CONFLUENT_SCHEMA_REGISTRY`) with runtime values.

Both share the same `KafkaDriver` (`lib/driver.py`) and connector config templates.
`run_tests.sh` passes all pytest CLI options explicitly when launching the
test-runner container.

## Functional CI load and duration

Document version: 2.1, 2026-10-11 20:00 UTC

**Review decision:** #1587 changes draft CI across the repository. Maintainers
must approve that policy before merge. It is separate from the [SPCS test and
harness](spcs/README.md) in #1585/#1586.

The `functional` matrix in `end-to-end.yaml` runs one lane at a time. All seven
lanes and their assertions remain available for ready PRs, pushes, schedules,
and explicit full-suite dispatches. Draft PRs use the narrow trial below instead.
Allow the sum of the lane durations,
plus build and queue time, for the complete functional result rather than the
slowest parallel lane. Each lane retains GitHub Actions' default six-hour job
limit; serialization does not require multiplying a per-job timeout.

CI also passes `--functional-test-delay 5`, which waits five seconds before each
functional case's fixture setup. Budget up to five seconds per selected case in
addition to its existing runtime (some skipped cases may also incur the delay).
The delay is outside each case's assertion deadline. Local runs default to no
delay; select an integer from 0 to 60 after the runner's `--` separator to opt in.
Pressure, soak, fault-injection, and SPCS tests are excluded from this pacing.
This is case-level pacing, not a rate limit on every DDL or channel-open call.

Per-type compatibility checks poll once every five seconds rather than two,
without extending their 60-second observation window. Expected rows, value
checks, and error assertions are unchanged. Functional runtime under pacing is
not a throughput benchmark; stress and soak workloads keep their existing rates.

The matrix cap is per workflow run, not per Snowflake account. Separate PR runs,
fault-injection jobs, and other account users can still overlap. For a controlled
trial, run only one functional workflow against the shared account at a time.
This reduces test load but does not guarantee that shared-service throttling ends.

### KC v4-only trial

Draft PRs run a single Apache Kafka 4.1.1 / Java 17 / AWS job covering VECTOR,
structured OBJECT, and structured ARRAY in both `v4-compat` and `v4-ht` modes.
All six cases must be collected; other cases are deselected, with a second guard
before fixture setup. The runner skips the KC v3 download and mounts a fresh
empty legacy-plugin directory. Assertions and the 60-second case deadline remain
unchanged. Service logs are retained on success as well as failure.

```bash
./test/run_tests.sh --platform=apache --platform-version=4.1.1 \
  --java-version=17 --cloud=AWS --v4-trial --logs-dir="$PWD/test-logs"
```

`--v4-trial` rejects extra pytest arguments so callers cannot override its fixed
selection. It applies five-second case pacing. Once this workflow is available
on the default branch, it can also be dispatched with `run_v4_trial=true`; that
input takes precedence over all other live E2E inputs. Draft PR creation and
updates run the trial without needing a manual dispatch.

The trial has a 45-minute job limit including build/setup. A repository-wide
trial concurrency group prevents two trials from running together; GitHub may
replace an older pending trial when newer runs queue. This is not an account-wide
lock against other workflows or repositories. Broad Java integration and mixed
E2E lanes are skipped on draft PRs and restored by the `ready_for_review` event.
A green trial does not establish full-suite or release qualification.

Offline checks (no credentials or running services):

```bash
python3 test/scripts/functional_pacing_contract.py
python3 test/scripts/workflow_contract.py  # requires PyYAML==6.0.3
```

## Stress Tests

Stress tests use the same Docker infrastructure but with `-m pressure` passed to pytest:

```bash
./run_tests.sh --platform=confluent --platform-version=7.6.0 -- -m pressure
```

CI stress (`.github/workflows/end-to-end-stress.yml`) sets `CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000` on the `run-e2e-tests` step so Kafka Connect uses a **10 second** `offset.flush.interval.ms` (less frequent `preCommit` than the default 1 s E2E setting). For local pressure runs, export the same variable before `./run_tests.sh` if you want that behavior.

When `-m pressure` is set, pytest selects only the pressure-marked tests:

1. **test_pressure_restart** (`tests/pressure/test_pressure_restart.py`) -- Creates 10 topics with 3 partitions each and sends 200,000 records per partition. During verification, the connector is periodically restarted, paused, resumed, and deleted/recreated to test resilience under load.

2. **test_pressure_init** (`tests/pressure/test_pressure_init.py`) -- Creates 200 topics with 12 partitions each and sends 10,000 records per partition (2,400 partitions, 24M records total). Sends are parallelized across 10 threads.

Both tests verify that the exact expected row count appears in Snowflake.

## Debugging

### View logs
```bash
docker logs -f test-kafka-connect
docker logs -f test-kafka
```

### Check connector status
```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/<name>/status
```

### Manual cleanup
```bash
cd test/docker
docker compose -f docker-compose.base.yml -f docker-compose.confluent.yml down -v --remove-orphans
```

## Directory Structure

```
test/
  tests/                      Pytest test modules (new tests go here)
  conftest.py                 Pytest fixtures and CLI options
  pyproject.toml              Pytest configuration
  lib/                        Shared helpers (KafkaDriver, config, crypto)
  rest_request_template/      Connector config templates (one per test case)
  test_suit/                  Legacy E2E test classes (send/verify/clean)
  test_data/                  Protobuf schema and generated code
  docker/                     Docker Compose files, Dockerfiles, and test runner scripts
  apache_properties/          Kafka/Zookeeper/Connect config (used by Apache Docker image)
  build_runtime_jar.sh        Builds connector JAR/ZIP
  test_verify.py              Legacy E2E test entry point
  test_suites.py              Legacy test suite registry
  test_selector.py            Legacy test filtering logic
  test_executor.py            Legacy test execution engine
```
