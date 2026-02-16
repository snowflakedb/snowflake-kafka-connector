# Docker-based E2E Tests for Snowflake Kafka Connector

The `docker/` subdirectory contains a Docker-based test environment that runs the Snowflake Kafka Connector E2E tests with minimal host dependencies.

## Prerequisites

- **Docker** (with Docker Compose v2)
- **Snowflake credentials** (profile.json)
- **Built connector plugin** (run `build_runtime_jar.sh` first)

## Quick Start

```bash
# 1. Build the connector (from project root)
export SNOWFLAKE_CREDENTIAL_FILE=/path/to/profile.json
./test/build_runtime_jar.sh . package confluent   # or 'apache'

# 2. Run tests
cd test/docker
./run_tests.sh --platform=confluent --version=7.8.0
```

## Usage

```bash
./run_tests.sh --platform=<confluent|apache> --version=<version> [options]

# Confluent examples:
./run_tests.sh --platform=confluent --version=7.8.0
./run_tests.sh --platform=confluent --version=6.2.15
./run_tests.sh --platform=confluent --version=7.8.0 --tests=TestStringJson

# Apache Kafka examples:
./run_tests.sh --platform=apache --version=3.7.2
./run_tests.sh --platform=apache --version=2.8.2 --java-version=11

# Other options:
./run_tests.sh --platform=confluent --version=7.8.0 --pressure   # Run pressure/stress tests
./run_tests.sh --platform=apache --version=3.7.2 --keep          # Keep containers after tests
./run_tests.sh --platform=confluent --version=7.8.0 --rebuild    # Force rebuild test image
./run_tests.sh --platform=confluent --version=7.8.0 --logs       # Show logs on failure
./run_tests.sh --platform=apache --version=3.7.2 --cloud=AWS     # Target specific Snowflake cloud
```

## Supported Versions

**Confluent Platform:**
- `6.2.x` (e.g., 6.2.15)
- `7.x` (e.g., 7.6.0, 7.8.2)

**Apache Kafka:**
- Any version available as an official tarball (e.g., 2.8.2, 3.7.2)

## Architecture

The test environment uses multiple Docker Compose files layered together:

- `docker-compose.base.yml` -- test-runner container (shared by all platforms)
- `docker-compose.confluent.yml` -- Confluent Platform services (Zookeeper, Kafka, Schema Registry, Kafka Connect)
- `docker-compose.apache.yml` -- Apache Kafka (single container with embedded Zookeeper, Kafka, Connect, Schema Registry)
- `docker-compose.amd64.yml` -- forces linux/amd64 emulation (used for Confluent 6.2.x on ARM)

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
