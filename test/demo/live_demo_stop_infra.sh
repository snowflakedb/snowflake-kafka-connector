#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${SCRIPT_DIR}/.infra_pids" ]]; then
  read -r zk_pid kafka_pid connect_pid sr_pid < "${SCRIPT_DIR}/.infra_pids" || true
  for pid in "${zk_pid:-}" "${kafka_pid:-}" "${connect_pid:-}" "${sr_pid:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
      kill "${pid}" || true
    fi
  done
  rm -f "${SCRIPT_DIR}/.infra_pids"
fi

# Kill wrapper scripts when present.
pkill -f "connect-distributed" || true
pkill -f "kafka-server-start" || true
pkill -f "zookeeper-server-start" || true
pkill -f "schema-registry-start" || true

# Kill by Java main classes for cases where startup scripts have already exec'd Java.
pkill -f "org.apache.kafka.connect.cli.ConnectDistributed" || true
pkill -f "kafka.Kafka" || true
pkill -f "org.apache.zookeeper.server.quorum.QuorumPeerMain" || true
pkill -f "io.confluent.kafka.schemaregistry.rest.SchemaRegistryMain" || true

echo "Stopped local demo infra processes"
