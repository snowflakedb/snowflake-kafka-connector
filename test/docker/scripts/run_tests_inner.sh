#!/bin/bash
# This script runs INSIDE the test-runner container

set -e
set -a

# Determine test set and version
TEST_SET="${TEST_SET:-confluent}"
if [ "$TEST_SET" = "apache" ]; then
    VERSION="${KAFKA_VERSION:-3.7.0}"
    echo "=== Snowflake Kafka Connector E2E Tests (Apache Kafka) ==="
    echo "Apache Kafka Version: ${VERSION}"
else
    VERSION="${CONFLUENT_VERSION:-7.8.0}"
    echo "=== Snowflake Kafka Connector E2E Tests (Confluent Platform) ==="
    echo "Confluent Version: ${VERSION}"
fi
echo "Kafka Connect: http://${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}"
echo "Schema Registry: ${SCHEMA_REGISTRY_URL}"
echo "Test Set: ${TEST_SET}"

# Compile protobuf for Python (Java compilation not needed - connector has its own)
echo -e "\n=== Compiling protobuf ==="
echo "protoc version: $(protoc --version)"
cd /app/test_data
protoc --python_out=. sensor.proto
echo "Protobuf compiled successfully"
cd /app

# Wait a bit for services to stabilize after health checks pass
echo -e "\n=== Waiting for services to stabilize ==="
sleep 5

# Verify Kafka Connect is responsive
echo -e "\n=== Verifying Kafka Connect ==="
curl -s "http://${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}/connector-plugins" | jq '.[].class' || {
    echo "ERROR: Cannot reach Kafka Connect"
    exit 1
}

# Set variables for test_verify.py
KAFKA_ADDRESS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
SC_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
KC_ADDRESS="${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}"

echo -e "\n=== Running pytest tests ==="
PRESSURE_ARGS=()
if [ "$PRESSURE_TEST" = "true" ]; then
    PRESSURE_ARGS+=(-m pressure)
else
    PRESSURE_ARGS+=(-m 'not pressure')
fi
pytest \
    --kafka-address "$KAFKA_ADDRESS" \
    --schema-registry-address "$SC_URL" \
    --kafka-connect-address "$KC_ADDRESS" \
    --platform "$TEST_SET" \
    --platform-version "$VERSION" \
    -v \
    "${PRESSURE_ARGS[@]}" \
    "$@"
