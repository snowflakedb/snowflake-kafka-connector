#!/bin/bash
#
# Scrape Snowflake Kafka Connector JMX metrics via Jolokia.
#
# Usage:
#   ./scrape_metrics.sh [options]
#
# Modes:
#   --once               Single snapshot to stdout (default)
#   --poll               Continuous scraping to a JSONL file
#   --interval=SECONDS   Poll interval (default: 10)
#   --output=FILE        Output file for --poll mode (default: /tmp/sf-metrics.jsonl)
#   --host=HOST          Jolokia host (default: kafka-connect)
#   --port=PORT          Jolokia port (default: 8778)
#   --pretty             Pretty-print JSON output (--once mode only)
#
# Examples:
#   ./scrape_metrics.sh --once --pretty
#   ./scrape_metrics.sh --poll --interval=5 --output=/tmp/metrics.jsonl
#   ./scrape_metrics.sh --once --host=localhost

set -e

HOST="${KAFKA_CONNECT_HOST:-kafka-connect}"
PORT="8778"
MODE="once"
INTERVAL=10
OUTPUT="/tmp/sf-metrics.jsonl"
PRETTY="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --once)     MODE="once"; shift ;;
        --poll)     MODE="poll"; shift ;;
        --interval=*) INTERVAL="${1#*=}"; shift ;;
        --output=*)   OUTPUT="${1#*=}"; shift ;;
        --host=*)     HOST="${1#*=}"; shift ;;
        --port=*)     PORT="${1#*=}"; shift ;;
        --pretty)     PRETTY="true"; shift ;;
        -h|--help)
            head -20 "$0" | grep "^#" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

JOLOKIA_URL="http://${HOST}:${PORT}/jolokia"
SF_DOMAIN="snowflake.kafka.connector"

fetch_snapshot() {
    local timestamp
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # Fetch all Snowflake connector MBeans in one request
    local raw
    raw=$(curl -sf "${JOLOKIA_URL}/read/${SF_DOMAIN}:*" 2>/dev/null) || {
        echo "{\"timestamp\":\"${timestamp}\",\"error\":\"Cannot reach Jolokia at ${JOLOKIA_URL}\"}"
        return 1
    }

    # Reshape: add timestamp, extract just the value map
    echo "$raw" | jq -c --arg ts "$timestamp" '{timestamp: $ts, metrics: .value}'
}

case $MODE in
    once)
        result=$(fetch_snapshot)
        if [ "$PRETTY" = "true" ]; then
            echo "$result" | jq .
        else
            echo "$result"
        fi
        ;;
    poll)
        echo "Scraping ${SF_DOMAIN} from ${JOLOKIA_URL} every ${INTERVAL}s → ${OUTPUT}" >&2
        mkdir -p "$(dirname "$OUTPUT")"
        while true; do
            fetch_snapshot >> "$OUTPUT"
            sleep "$INTERVAL"
        done
        ;;
esac
