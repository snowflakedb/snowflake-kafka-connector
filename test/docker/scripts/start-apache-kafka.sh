#!/bin/bash
# Startup script that mirrors run_test_apache.sh (lines 144-154)
# Runs Zookeeper, Kafka, and Kafka Connect in sequence

set -e

KAFKA_HOME=/opt/kafka
LOG_DIR=/var/log/kafka

mkdir -p $LOG_DIR
rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true

# Start Zookeeper (same as run_test_apache.sh line 145)
echo "=== Starting Zookeeper ==="
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > $LOG_DIR/zookeeper.log 2>&1 &
ZOOKEEPER_PID=$!

# Wait for Zookeeper to be ready
echo "Waiting for Zookeeper..."
for i in {1..30}; do
    if nc -z localhost 2181 2>/dev/null; then
        echo "Zookeeper is ready"
        break
    fi
    sleep 1
done

# Start Kafka (same as run_test_apache.sh line 148)
echo "=== Starting Kafka ==="
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $LOG_DIR/kafka.log 2>&1 &
KAFKA_PID=$!

# Wait for Kafka to be ready
echo "Waiting for Kafka..."
for i in {1..30}; do
    if nc -z localhost 9092 2>/dev/null; then
        echo "Kafka is ready"
        break
    fi
    sleep 1
done

# Additional wait for Kafka to fully initialize
sleep 5

# Start Kafka Connect (same as run_test_apache.sh line 153)
echo "=== Starting Kafka Connect ==="
echo "Java version:"
java -version
$KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties > $LOG_DIR/kc.log 2>&1 &
KC_PID=$!

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect..."
for i in {1..60}; do
    if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
        echo "Kafka Connect is ready"
        break
    fi
    sleep 2
done

echo "=== All services started ==="
echo "Zookeeper PID: $ZOOKEEPER_PID"
echo "Kafka PID: $KAFKA_PID"
echo "Kafka Connect PID: $KC_PID"

# Keep container running and forward signals
trap "kill $KC_PID $KAFKA_PID $ZOOKEEPER_PID 2>/dev/null; exit 0" SIGTERM SIGINT

# Tail logs to keep container running
tail -f $LOG_DIR/*.log &
wait
