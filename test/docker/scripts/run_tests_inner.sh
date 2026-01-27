#!/bin/bash
# This script runs INSIDE the test-runner container

set -e

echo "=== Snowflake Kafka Connector E2E Tests ==="
echo "Confluent Version: ${CONFLUENT_VERSION}"
echo "Kafka Connect: http://${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}"
echo "Schema Registry: ${SCHEMA_REGISTRY_URL}"

# Generate name salt if not provided
if [ -z "$TEST_NAME_SALT" ]; then
    TEST_NAME_SALT="_$(echo $RANDOM$RANDOM | base64 | cut -c1-7)"
fi
echo "Name Salt: ${TEST_NAME_SALT}"

# Compile protobuf for Python (Java compilation not needed - connector has its own)
echo -e "\n=== Compiling protobuf ==="
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
TEST_SET="confluent"

# Build arguments for tests
TESTS_ARG=""
if [ -n "$TESTS_TO_RUN" ]; then
    TESTS_ARG="$TESTS_TO_RUN"
fi

echo -e "\n=== Running cleanup ==="
python3 test_verify.py "$KAFKA_ADDRESS" "$SC_URL" "$KC_ADDRESS" clean "$CONFLUENT_VERSION" "$TEST_NAME_SALT" "$PRESSURE_TEST" "false" "false" "$TESTS_ARG" || true

echo -e "\n=== Running tests ==="
set +e
python3 test_verify.py "$KAFKA_ADDRESS" "$SC_URL" "$KC_ADDRESS" "$TEST_SET" "$CONFLUENT_VERSION" "$TEST_NAME_SALT" "$PRESSURE_TEST" "false" "false" "$TESTS_ARG"
TEST_EXIT_CODE=$?
set -e

echo -e "\n=== Running post-test cleanup ==="
python3 test_verify.py "$KAFKA_ADDRESS" "$SC_URL" "$KC_ADDRESS" clean "$CONFLUENT_VERSION" "$TEST_NAME_SALT" "$PRESSURE_TEST" "false" "false" "$TESTS_ARG" || true

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo -e "\n\033[0;31m=== TESTS FAILED ===\033[0m"
    exit $TEST_EXIT_CODE
fi

echo -e "\n\033[0;32m=== ALL TESTS PASSED ===\033[0m"
exit 0
