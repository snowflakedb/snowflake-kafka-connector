# Docker-based E2E Tests for Snowflake Kafka Connector

This directory contains a Docker-based test environment that runs the Snowflake Kafka Connector E2E tests with minimal host dependencies.

## Prerequisites

- **Docker** (with Docker Compose v2)
- **Snowflake credentials** (profile.json)
- **Built connector plugin** (run `build_runtime_jar.sh` first)

## Quick Start

```bash
# 1. Build the connector (from project root)
export SNOWFLAKE_CREDENTIAL_FILE=/path/to/profile.json
./test/build_runtime_jar.sh . package confluent

# 2. Run tests
cd test/docker
./run_tests.sh 7.8.0
```

## Usage

```bash
./run_tests.sh <confluent_version> [options]

# Examples:
./run_tests.sh 7.8.0                           # Run all tests with Confluent 7.8.0
./run_tests.sh 6.2.15                          # Run all tests with Confluent 6.2.15
./run_tests.sh 7.8.0 --tests=TestStringJson    # Run specific test
./run_tests.sh 7.8.0 --pressure                # Run pressure tests
./run_tests.sh 7.8.0 --keep                    # Keep containers after tests
./run_tests.sh 7.8.0 --rebuild                 # Force rebuild test image
./run_tests.sh 7.8.0 --logs                    # Show logs on failure
```

## Supported Confluent Versions

- `6.2.x` (e.g., 6.2.15)
- `7.8.x` (e.g., 7.8.0, 7.8.2)

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    docker-compose.yml                      │
├──────────────┬──────────────┬───────────────┬──────────────┤
│  zookeeper   │    kafka     │schema-registry│kafka-connect │
│   :2181      │   :9092      │    :8081      │    :8083     │
├──────────────┴──────────────┴───────────────┴──────────────┤
│                      test-runner                           │
│              (Python + protobuf + tests)                   │
└────────────────────────────────────────────────────────────┘
```

## Components

| Service | Port | Description |
|---------|------|-------------|
| zookeeper | 2181 | Kafka coordination |
| kafka | 9092 | Kafka broker |
| schema-registry | 8081 | Avro/JSON/Protobuf schema registry |
| kafka-connect | 8083 | Kafka Connect with Snowflake connector |
| test-runner | - | Python test execution container |

## Debugging

### View logs
```bash
docker compose logs -f kafka-connect
docker compose logs -f test-runner
```

### Shell into containers
```bash
docker compose exec kafka-connect bash
docker compose exec test-runner bash
```

### Check connector status
```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/<name>/status
```

### Manual cleanup
```bash
docker compose down -v --remove-orphans
```
