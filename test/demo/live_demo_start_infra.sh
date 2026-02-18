#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFLUENT_VERSION="${1:-7.8.0}"
CONFLUENT_DIR="${TEST_DIR}/confluent-${CONFLUENT_VERSION}"
APACHE_LOG_PATH="${TEST_DIR}/apache_log"
KAFKA_PLUGIN_PATH="/usr/local/share/kafka/plugins"

error_exit() {
  echo "$1" >&2
  exit 1
}

show_recent_logs() {
  echo "---- recent kafka connect log ----"
  tail -n 60 "${APACHE_LOG_PATH}/kc.log" 2>/dev/null || true
  echo "---- recent kafka broker log ----"
  tail -n 40 "${APACHE_LOG_PATH}/kafka.log" 2>/dev/null || true
  echo "---- recent zookeeper log ----"
  tail -n 20 "${APACHE_LOG_PATH}/zookeeper.log" 2>/dev/null || true
}

ensure_connect_internal_topics() {
  echo "Ensuring Kafka Connect internal topics are compacted"
  "${CONFLUENT_DIR}/bin/kafka-topics" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic connect-configs \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact >/dev/null 2>&1 || true
  "${CONFLUENT_DIR}/bin/kafka-topics" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic connect-offsets \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact >/dev/null 2>&1 || true
  "${CONFLUENT_DIR}/bin/kafka-topics" \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic connect-status \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=compact >/dev/null 2>&1 || true

  "${CONFLUENT_DIR}/bin/kafka-configs" \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name connect-configs \
    --alter --add-config cleanup.policy=compact >/dev/null 2>&1 || true
  "${CONFLUENT_DIR}/bin/kafka-configs" \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name connect-offsets \
    --alter --add-config cleanup.policy=compact >/dev/null 2>&1 || true
  "${CONFLUENT_DIR}/bin/kafka-configs" \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name connect-status \
    --alter --add-config cleanup.policy=compact >/dev/null 2>&1 || true
}

wait_http() {
  local url="$1"
  local label="$2"
  local max_wait="${3:-90}"
  local waited=0
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( waited % 5 == 0 )); then
      echo "Waiting for ${label} (${waited}s elapsed)"
    fi
    sleep 1
    waited=$((waited + 1))
    if [[ "$waited" -ge "$max_wait" ]]; then
      show_recent_logs
      error_exit "Timed out waiting for ${label} at ${url}"
    fi
  done
}

wait_kafka() {
  local max_wait="${1:-90}"
  local waited=0
  until "${CONFLUENT_DIR}/bin/kafka-topics" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    if (( waited % 5 == 0 )); then
      echo "Waiting for Kafka bootstrap localhost:9092 (${waited}s elapsed)"
    fi
    sleep 1
    waited=$((waited + 1))
    if [[ "$waited" -ge "$max_wait" ]]; then
      show_recent_logs
      error_exit "Timed out waiting for Kafka bootstrap localhost:9092"
    fi
  done
}

cd "${TEST_DIR}"

if [[ ! -f /tmp/sf-kafka-connect-plugin.zip ]]; then
  error_exit "Missing /tmp/sf-kafka-connect-plugin.zip. Run: ./build_runtime_jar.sh /home/repo/snowflake-kafka-connector package confluent AWS"
fi

if ! awk -v host="$(hostname)" '$1=="127.0.0.1" && $2==host {found=1} END{exit found?0:1}' /etc/hosts; then
  echo "Adding hostname to /etc/hosts for Kafka Connect advertised URL resolution"
  echo "127.0.0.1 $(hostname)" | sudo tee -a /etc/hosts >/dev/null
fi

echo "Stopping any old local demo infra"
pkill -f "connect-distributed" || true
pkill -f "kafka-server-start" || true
pkill -f "zookeeper-server-start" || true
pkill -f "schema-registry-start" || true
pkill -f "org.apache.kafka.connect.cli.ConnectDistributed" || true
pkill -f "kafka.Kafka" || true
pkill -f "org.apache.zookeeper.server.quorum.QuorumPeerMain" || true
pkill -f "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain" || true
sleep 2

if [[ ! -f "${TEST_DIR}/apache.tgz" ]]; then
  echo "Downloading Confluent ${CONFLUENT_VERSION}"
  curl -fsSL "https://packages.confluent.io/archive/7.8/confluent-community-${CONFLUENT_VERSION}.tar.gz" -o "${TEST_DIR}/apache.tgz"
fi

rm -rf "${CONFLUENT_DIR}"
tar xzf "${TEST_DIR}/apache.tgz" -C "${TEST_DIR}"

mkdir -p "${APACHE_LOG_PATH}"
rm -f "${APACHE_LOG_PATH}/zookeeper.log" "${APACHE_LOG_PATH}/kafka.log" "${APACHE_LOG_PATH}/kc.log" "${APACHE_LOG_PATH}/sc.log"
rm -rf /tmp/kafka-logs /tmp/zookeeper

mkdir -m 777 -p "${KAFKA_PLUGIN_PATH}" || sudo mkdir -m 777 -p "${KAFKA_PLUGIN_PATH}"
unzip -o /tmp/sf-kafka-connect-plugin.zip -d "${KAFKA_PLUGIN_PATH}" >/dev/null

cp -f "${TEST_DIR}/connect-log4j.properties" "${CONFLUENT_DIR}/etc/kafka/"

echo "Java in use:"
java -version

start_ts="$(date +%s)"

echo "Starting Zookeeper"
"${CONFLUENT_DIR}/bin/zookeeper-server-start" "${TEST_DIR}/apache_properties/zookeeper.properties" > "${APACHE_LOG_PATH}/zookeeper.log" 2>&1 &
ZOOKEEPER_PID=$!

echo "Starting Kafka broker"
"${CONFLUENT_DIR}/bin/kafka-server-start" "${TEST_DIR}/apache_properties/server.properties" > "${APACHE_LOG_PATH}/kafka.log" 2>&1 &
KAFKA_PID=$!

wait_kafka 120
ensure_connect_internal_topics

echo "Starting Kafka Connect"
KAFKA_HEAP_OPTS="-Xms512m -Xmx6g" "${CONFLUENT_DIR}/bin/connect-distributed" "${TEST_DIR}/apache_properties/connect-distributed.properties" > "${APACHE_LOG_PATH}/kc.log" 2>&1 &
CONNECT_PID=$!

echo "Starting Schema Registry"
"${CONFLUENT_DIR}/bin/schema-registry-start" "${TEST_DIR}/apache_properties/schema-registry.properties" > "${APACHE_LOG_PATH}/sc.log" 2>&1 &
SCHEMA_REGISTRY_PID=$!

echo "${ZOOKEEPER_PID} ${KAFKA_PID} ${CONNECT_PID} ${SCHEMA_REGISTRY_PID}" > "${SCRIPT_DIR}/.infra_pids"

wait_http "http://localhost:8083/connectors" "Kafka Connect REST" 120
wait_http "http://localhost:8081/subjects" "Schema Registry REST" 120

end_ts="$(date +%s)"
echo "Infra ready in $((end_ts - start_ts)) seconds"
echo "Kafka: localhost:9092 | Connect: localhost:8083 | Schema Registry: localhost:8081"
