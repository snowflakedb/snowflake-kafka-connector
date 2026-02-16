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
cd test/docker
./run_tests.sh --platform=confluent --version=7.8.0
```

### Usage

```bash
./run_tests.sh --platform=<confluent|apache> --version=<version> [options]

# Confluent examples:
./run_tests.sh --platform=confluent --version=7.8.0
./run_tests.sh --platform=confluent --version=6.2.15
./run_tests.sh --platform=confluent --version=7.8.0 --tests=TestStringJson

# Apache Kafka examples:
./run_tests.sh --platform=apache --version=3.7.2
./run_tests.sh --platform=apache --version=2.8.2 --java-version=11

# Options:
./run_tests.sh --platform=confluent --version=7.8.0 --pressure   # Stress tests
./run_tests.sh --platform=apache --version=3.7.2 --keep          # Keep containers after tests
./run_tests.sh --platform=confluent --version=7.8.0 --rebuild    # Force rebuild images
./run_tests.sh --platform=confluent --version=7.8.0 --logs       # Show logs on failure
./run_tests.sh --platform=apache --version=3.7.2 --cloud=AWS     # Target specific Snowflake cloud
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

The E2E framework has three phases: **configure**, **send**, **verify**, and **clean**.

1. **Connector configuration** -- Templates in `rest_request_template/` have a one-to-one correspondence with test classes in `test_suit/`. For example, `travis_correct_json_json.json` maps to `test_suit/test_json_json.py`. The driver `test_verify.py` replaces placeholder values (e.g., `SNOWFLAKE_TEST_TOPIC`, `CONFLUENT_SCHEMA_REGISTRY`) with actual runtime values.

2. **Send** -- Each test class (e.g., `TestJsonJson`) produces records to specific topic partitions. The corresponding connector consumes and ingests them into Snowflake.

3. **Verify** -- The test verifies:
   - Row count in the Snowflake table matches the number of sent records
   - The first record matches the expected content
   - Internal stages are cleaned up (skipped for large pressure tests where the connector may not have time to purge)

4. **Clean** -- Tables, stages, and pipes are deleted. Docker containers are torn down.

## Stress Tests

Stress tests use the same Docker infrastructure but with the `--pressure` flag.

```bash
./run_tests.sh --platform=confluent --version=7.6.0 --pressure
```

When `--pressure` is set, `test_verify.py` runs two test suites instead of the regular E2E suite:

1. **TestPressureRestart** (`test_suit/test_pressure_restart.py`) -- Creates 10 topics with 3 partitions each and sends 200,000 records per partition. During verification, the connector is periodically restarted, paused, resumed, and deleted/recreated to test resilience under load.

2. **TestPressure** (`test_suit/test_pressure.py`) -- Creates 200 topics with 12 partitions each and sends 10,000 records per partition (2,400 partitions, 24M records total). Sends are parallelized across 10 threads.

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
  docker/                     Docker Compose files, Dockerfiles, and test runner scripts
  rest_request_template/      Connector config templates (one per test case)
  test_suit/                  Python E2E test classes (send/verify/clean)
  test_data/                  Protobuf schema and generated code
  apache_properties/          Kafka/Zookeeper/Connect config (used by Apache Docker image)
  build_runtime_jar.sh        Builds connector JAR/ZIP
  test_verify.py              E2E test entry point
  test_suites.py              Test suite registry
  test_selector.py            Test filtering logic
  test_executor.py            Test execution engine
```
