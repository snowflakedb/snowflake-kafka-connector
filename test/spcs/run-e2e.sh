#!/bin/sh
# SPCS release test harness (SNOW-4202412). Runs inside ONE SPCS job container:
# a KRaft Kafka broker, a producer, and a Kafka Connect standalone worker hosting
# the Snowflake Kafka Connector with NO credentials (ambient SPCS auth).
#
# Finite by design: exits when the connector's committed consumer offset reaches
# NRECORDS, or after TIMEOUT_SECS. The in-container signal is advisory; the
# driver (run_spcs_release.py) is authoritative and counts rows in Snowflake.
#
# Inputs come from /mnt/harness (stage volume): kafka.tgz, kc.jar, this script.
# Output contract, parsed by the driver from SYSTEM$GET_SERVICE_LOGS:
#   E2E| ...               progress
#   E2E_ERR <code>=<n>     occurrences of 390422 / 395090 in connect.log
#   E2E_EXIT=<rc>          0 = offsets reached NRECORDS, nonzero otherwise

set -u
log() { echo "E2E| $*"; }

WORK=${WORK:-/work}
MNT=${MNT:-/mnt/harness}
TOPIC=${TOPIC:-kc_spcs_release_topic}
TABLE=${TABLE:?TABLE is required}
NRECORDS=${NRECORDS:-1000}
TIMEOUT_SECS=${TIMEOUT_SECS:-600}
ROLE=${ROLE:-SYSADMIN}
CONNECTOR=snowflake_spcs_release_sink

finish() {
  rc=$1
  for code in 390422 395090; do
    n=$(grep -c "$code" "$WORK/connect.log" 2>/dev/null || true)
    echo "E2E_ERR $code=${n:-0}"
  done
  log "---- connect.log: errors (last 40) ----"
  grep -E "ERROR|390422|395090" "$WORK/connect.log" 2>/dev/null | tail -40 | sed 's/^/E2E| /'
  echo "E2E_EXIT=$rc"
  exit "$rc"
}

mkdir -p "$WORK"
START=$(date +%s)
tar xzf "$MNT/kafka.tgz" -C "$WORK" || { log "FATAL tar failed"; finish 2; }
K=$(ls -d "$WORK"/kafka_2.13-* 2>/dev/null | head -1)
[ -x "$K/bin/kafka-server-start.sh" ] || { log "FATAL kafka not extracted"; finish 2; }
log "kafka home=$K table=$TABLE nrecords=$NRECORDS timeout=${TIMEOUT_SECS}s"

# ---------------------------------------------------------------- broker (KRaft)
cat > "$WORK/kraft.properties" <<PROPS
node.id=1
process.roles=broker,controller
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/kafka-logs
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
PROPS

CLUSTER_ID=$("$K/bin/kafka-storage.sh" random-uuid)
"$K/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$WORK/kraft.properties" --ignore-formatted \
  > "$WORK/format.log" 2>&1 || { log "FATAL storage format failed"; tail -20 "$WORK/format.log"; finish 2; }

KAFKA_HEAP_OPTS="-Xmx512M -Xms256M" \
  "$K/bin/kafka-server-start.sh" "$WORK/kraft.properties" > "$WORK/broker.log" 2>&1 &

ok=0; i=0
while [ $i -lt 60 ]; do
  if "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    ok=1; break
  fi
  i=$((i+1)); sleep 2
done
[ $ok -eq 1 ] || { log "FATAL broker did not come up"; tail -40 "$WORK/broker.log"; finish 2; }
log "broker is up"

# ---------------------------------------------------------------- topic + data
"$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 > "$WORK/topic.log" 2>&1
i=1
: > "$WORK/records.ndjson"
while [ $i -le "$NRECORDS" ]; do
  echo "{\"id\":$i,\"name\":\"spcs-release-$i\"}" >> "$WORK/records.ndjson"
  i=$((i+1))
done
"$K/bin/kafka-console-producer.sh" --bootstrap-server localhost:9092 --topic "$TOPIC" \
  < "$WORK/records.ndjson" > "$WORK/producer.log" 2>&1 || { log "FATAL producer failed"; finish 2; }
log "produced $NRECORDS records"

# ---------------------------------------------------------------- Kafka Connect
mkdir -p "$WORK/plugins/snowflake-kafka-connector"
cp "$MNT/kc.jar" "$WORK/plugins/snowflake-kafka-connector/" || { log "FATAL no kc.jar"; finish 2; }

cat > "$WORK/worker.properties" <<PROPS
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
offset.storage.file.filename=$WORK/connect.offsets
offset.flush.interval.ms=5000
plugin.path=$WORK/plugins
plugin.discovery=hybrid_warn
PROPS

# No credential and no authenticator: ambient SPCS auth must be auto-detected.
cat > "$WORK/connector.properties" <<PROPS
name=$CONNECTOR
connector.class=com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector
tasks.max=1
topics=$TOPIC
snowflake.topic2table.map=$TOPIC:$TABLE
snowflake.role.name=$ROLE
snowflake.streaming.validate.compatibility.with.classic=false
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
PROPS

KAFKA_HEAP_OPTS="-Xmx1G -Xms512M" \
  "$K/bin/connect-standalone.sh" "$WORK/worker.properties" "$WORK/connector.properties" \
  > "$WORK/connect.log" 2>&1 &
log "Kafka Connect started"

# ---------------------------------------------------------------- wait for commit
# KC v4 commits a sink offset only after Snowflake has persisted the rows, so the
# connect-<name> consumer group reaching NRECORDS is a meaningful (not sufficient)
# in-container signal. RUNNING state is deliberately never checked.
DEADLINE=$((START + TIMEOUT_SECS))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  sleep 10
  committed=$("$K/bin/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 \
      --describe --group "connect-$CONNECTOR" 2>/dev/null \
    | awk -v t="$TOPIC" '$2 == t && $4 ~ /^[0-9]+$/ { s += $4 } END { print s + 0 }')
  log "committed offset=$committed / $NRECORDS"
  if [ "$committed" -ge "$NRECORDS" ]; then
    log "all records committed"
    finish 0
  fi
done
log "TIMEOUT after ${TIMEOUT_SECS}s"
finish 1
