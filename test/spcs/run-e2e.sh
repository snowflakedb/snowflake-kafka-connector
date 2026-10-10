#!/bin/bash
# Finite wrapper around the repository's Apache Kafka/Connect configuration.
set -euo pipefail
BROKER_PID=
CONNECT_PID=
finish() {
  result=$?
  trap - EXIT
  for pid in "$CONNECT_PID" "$BROKER_PID"; do
    if [ -n "$pid" ]; then kill "$pid" 2>/dev/null || true; fi
  done
  if [ "$result" -ne 0 ]; then
    tail -n 40 /work/kafka.log /work/connect.log 2>/dev/null || true
  fi
  echo "SPCS_SMOKE_EXIT=$result"
  exit "$result"
}
trap finish EXIT
trap 'exit 124' TERM INT

mkdir -p /usr/local/share/kafka/plugins/snowflake-connector
cp /work/kc.jar /usr/local/share/kafka/plugins/snowflake-connector/
K=/opt/kafka
cluster_id=$("$K/bin/kafka-storage.sh" random-uuid)
"$K/bin/kafka-storage.sh" format -t "$cluster_id" -c "$K/config/kraft-server.properties"
KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' "$K/bin/kafka-server-start.sh" \
  "$K/config/kraft-server.properties" > /work/kafka.log 2>&1 &
BROKER_PID=$!
ready=false
for attempt in {1..60}; do
  if timeout 10 "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then ready=true; break; fi
  kill -0 "$BROKER_PID"
  sleep 2
done
[ "$ready" = true ]
KAFKA_HEAP_OPTS='-Xms256m -Xmx1g' "$K/bin/connect-distributed.sh" \
  "$K/config/connect-distributed.properties" > /work/connect.log 2>&1 &
CONNECT_PID=$!
ready=false
for attempt in {1..60}; do
  if curl --max-time 3 -fsS http://localhost:8083/connectors >/dev/null; then ready=true; break; fi
  kill -0 "$CONNECT_PID"
  sleep 2
done
[ "$ready" = true ]
cd /work/test
python -m pytest tests/spcs/test_spcs_ingestion.py --spcs \
  --platform apache --platform-version 4.1.1 \
  --kafka-address localhost:9092 --kafka-connect-address localhost:8083 \
  -q -o log_cli=false
