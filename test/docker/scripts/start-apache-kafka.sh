#!/bin/bash
# Startup script for Apache Kafka in Docker.
# Supports two modes controlled by the KRAFT_MODE env var:
#   KRAFT_MODE=true  -> KRaft (Kafka 4.x+): combined broker+controller, no ZooKeeper
#   KRAFT_MODE=false -> ZooKeeper mode (Kafka <=3.x): ZK + broker + Connect

set -e

KAFKA_HOME=/opt/kafka
LOG_DIR=/var/log/kafka

mkdir -p $LOG_DIR

echo "Java version:"
java -version

if [ "${KRAFT_MODE:-false}" = "true" ]; then
    #######################################################################
    # KRaft mode (Kafka 4.x+)
    #######################################################################
    echo "=== KRaft mode ==="
    rm -rf /tmp/kraft-combined-logs 2>/dev/null || true

    CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
    echo "Generated cluster ID: $CLUSTER_ID"

    echo "=== Formatting storage ==="
    $KAFKA_HOME/bin/kafka-storage.sh format \
        -t "$CLUSTER_ID" \
        -c $KAFKA_HOME/config/kraft-server.properties

    echo "=== Starting Kafka (KRaft combined broker+controller) ==="
    $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kraft-server.properties > $LOG_DIR/kafka.log 2>&1 &
    KAFKA_PID=$!

    echo "Waiting for Kafka broker..."
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null; then
            echo "Kafka broker is ready"
            break
        fi
        sleep 1
    done

    sleep 5

    echo "=== Starting Kafka Connect ==="
    $KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties > $LOG_DIR/kc.log 2>&1 &
    KC_PID=$!

    echo "Waiting for Kafka Connect..."
    for i in {1..60}; do
        if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
            echo "Kafka Connect is ready"
            break
        fi
        sleep 2
    done

    echo "=== All services started (KRaft) ==="
    echo "Kafka PID: $KAFKA_PID"
    echo "Kafka Connect PID: $KC_PID"

    trap "kill $KC_PID $KAFKA_PID 2>/dev/null; exit 0" SIGTERM SIGINT
else
    #######################################################################
    # ZooKeeper mode (Kafka <=3.x)
    #######################################################################
    echo "=== ZooKeeper mode ==="
    rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true

    echo "=== Starting Zookeeper ==="
    $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > $LOG_DIR/zookeeper.log 2>&1 &
    ZOOKEEPER_PID=$!

    echo "Waiting for Zookeeper..."
    for i in {1..30}; do
        if nc -z localhost 2181 2>/dev/null; then
            echo "Zookeeper is ready"
            break
        fi
        sleep 1
    done

    echo "=== Starting Kafka ==="
    $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > $LOG_DIR/kafka.log 2>&1 &
    KAFKA_PID=$!

    echo "Waiting for Kafka..."
    for i in {1..30}; do
        if nc -z localhost 9092 2>/dev/null; then
            echo "Kafka is ready"
            break
        fi
        sleep 1
    done

    sleep 5

    echo "=== Starting Kafka Connect ==="
    $KAFKA_HOME/bin/connect-distributed.sh $KAFKA_HOME/config/connect-distributed.properties > $LOG_DIR/kc.log 2>&1 &
    KC_PID=$!

    echo "Waiting for Kafka Connect..."
    for i in {1..60}; do
        if curl -s http://localhost:8083/connectors > /dev/null 2>&1; then
            echo "Kafka Connect is ready"
            break
        fi
        sleep 2
    done

    echo "=== All services started (ZooKeeper) ==="
    echo "Zookeeper PID: $ZOOKEEPER_PID"
    echo "Kafka PID: $KAFKA_PID"
    echo "Kafka Connect PID: $KC_PID"

    trap "kill $KC_PID $KAFKA_PID $ZOOKEEPER_PID 2>/dev/null; exit 0" SIGTERM SIGINT
fi

tail -f $LOG_DIR/*.log &
wait
