#!/bin/bash
#
# Snowflake Kafka Connector - Docker-based E2E Tests
#
# Usage:
#   ./run_tests.sh --platform=<confluent|apache> --platform-version=<version> [options]
#
# Examples:
#   ./run_tests.sh --platform=apache --platform-version=2.8.2
#   ./run_tests.sh --platform=apache --platform-version=3.7.0
#   ./run_tests.sh --platform=confluent --platform-version=7.8.0
#   ./run_tests.sh --platform=confluent --platform-version=7.8.0 -- tests/test_string_json.py
#
# Prerequisites:
#   - Docker and Docker Compose
#   - SNOWFLAKE_CREDENTIAL_FILE environment variable set
#   - Connector plugin built (run build_runtime_jar.sh first)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error_exit() {
    echo -e "${RED}ERROR: $1${NC}" >&2
    exit 1
}

info() {
    echo -e "${GREEN}INFO: $1${NC}"
}

warn() {
    echo -e "${YELLOW}WARN: $1${NC}"
}

usage() {
    echo "Usage: $0 --platform=<confluent|apache> --platform-version=<version> [options]"
    echo ""
    echo "Required:"
    echo "  --platform=PLATFORM           Platform: 'confluent' or 'apache'"
    echo "  --platform-version=VERSION    Kafka/Confluent platform version"
    echo "                                Confluent: 7.8.x, 6.2.x"
    echo "                                Apache: any version (e.g., 2.8.2, 3.7.0)"
    echo ""
    echo "Options:"
    echo "  --cloud=CLOUD        Snowflake cloud platform: AWS, GCP, or AZURE"
    echo "  --java-version=VER   Java version for Apache Kafka (default: 11)"
    echo "  --jmx                Enable JMX metrics scraping via Jolokia"
    echo "  --keep               Keep containers running after tests"
    echo "  --rebuild            Force rebuild of images"
    echo "  --logs-dir=DIR       Save service logs to a file in DIR on failure"
    echo "  -h, --help           Show this help message"
    echo "  -- ARGS              Pass remaining args directly to pytest"
    echo ""
    echo "Environment:"
    echo "  SNOWFLAKE_CREDENTIAL_FILE  Path to Snowflake credentials JSON (required)"
    echo ""
    echo "Examples:"
    echo "  $0 --platform=confluent --platform-version=7.8.0"
    echo "  $0 --platform=apache --platform-version=2.8.2"
    echo "  $0 --platform=confluent --platform-version=7.8.0 -- -k test_string_json"
    echo "  $0 --platform=apache --platform-version=3.7.0 --keep -- -m pressure"
    echo "  $0 --platform=confluent --platform-version=7.8.0 --logs-dir=/tmp/test-logs"
    exit 1
}

# Parse arguments
PLATFORM=""
PLATFORM_VERSION=""
JAVA_VERSION="11"
JMX_ENABLED="false"
KEEP_RUNNING="false"
FORCE_REBUILD="false"
LOGS_DIR=""
PASSTHROUGH_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --platform=*)
            PLATFORM="${1#*=}"
            shift
            ;;
        --platform-version=*)
            PLATFORM_VERSION="${1#*=}"
            shift
            ;;
        --cloud=*)
            SF_CLOUD_PLATFORM="${1#*=}"
            shift
            ;;
        --java-version=*)
            JAVA_VERSION="${1#*=}"
            shift
            ;;
        --jmx)
            JMX_ENABLED="true"
            shift
            ;;
        --keep)
            KEEP_RUNNING="true"
            shift
            ;;
        --rebuild)
            FORCE_REBUILD="true"
            shift
            ;;
        --logs-dir=*)
            LOGS_DIR="${1#*=}"
            shift
            ;;
        -h|--help)
            usage
            ;;
        --)
            shift
            PASSTHROUGH_ARGS=("$@")
            break
            ;;
        *)
            error_exit "Unknown option: $1"
            ;;
    esac
done

# Validate required arguments
if [ -z "$PLATFORM" ]; then
    error_exit "Missing required argument: --platform=<confluent|apache>"
fi

if [ -z "$PLATFORM_VERSION" ]; then
    error_exit "Missing required argument: --platform-version=<version>"
fi

# Base compose file + platform-specific compose file
BASE_COMPOSE="-f docker-compose.base.yml"

case $PLATFORM in
    confluent)
        case $PLATFORM_VERSION in
            6.2.*)
                info "Platform: Confluent $PLATFORM_VERSION"
                # 6.2.x containers are only available for linux/amd64
                COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.confluent.yml -f docker-compose.amd64.yml"
                info "Note: Confluent 6.2.x requires linux/amd64 (using emulation on ARM)"
                ;;
            7.*)
                info "Platform: Confluent $PLATFORM_VERSION"
                COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.confluent.yml"
                ;;
            *)
                error_exit "Unsupported Confluent version: $PLATFORM_VERSION (supported: 6.2.x, 7.x)"
                ;;
        esac
        CONFLUENT_VERSION="$PLATFORM_VERSION"
        KAFKA_VERSION=""
        KAFKA_CONNECT_ADDRESS="kafka-connect:8083"
        HEALTH_CHECK_SERVICE="kafka-connect"
        START_SERVICES="zookeeper kafka schema-registry kafka-connect"
        ;;
    apache)
        info "Platform: Apache Kafka $PLATFORM_VERSION (official tarball)"
        COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.apache.yml"
        CONFLUENT_VERSION=""
        KAFKA_VERSION="$PLATFORM_VERSION"
        KAFKA_CONNECT_ADDRESS="kafka:8083"
        HEALTH_CHECK_SERVICE="kafka"
        START_SERVICES="kafka"
        ;;
    *)
        error_exit "Unknown platform: $PLATFORM (supported: confluent, apache)"
        ;;
esac

# Check prerequisites
command -v docker >/dev/null 2>&1 || error_exit "Docker is not installed"
command -v docker compose >/dev/null 2>&1 || command -v docker-compose >/dev/null 2>&1 || error_exit "Docker Compose is not installed"

# Check credentials file
if [ -z "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    error_exit "SNOWFLAKE_CREDENTIAL_FILE environment variable is not set"
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    error_exit "Credential file not found: $SNOWFLAKE_CREDENTIAL_FILE"
fi

# Convert to absolute path
SNOWFLAKE_CREDENTIAL_FILE="$(cd "$(dirname "$SNOWFLAKE_CREDENTIAL_FILE")" && pwd)/$(basename "$SNOWFLAKE_CREDENTIAL_FILE")"
info "Credentials: $SNOWFLAKE_CREDENTIAL_FILE"

# Check for connector plugin based on platform
PLUGIN_DIR="/tmp/sf-kafka-connect-plugin"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"

if [ "$PLATFORM" = "apache" ]; then
    # Apache: Look for JAR in plugin path
    PLUGIN_JAR_PATH="/usr/local/share/kafka/plugins"
    PLUGIN_JAR=$(ls "$PLUGIN_JAR_PATH"/snowflake-kafka-connector-*.jar 2>/dev/null | head -n 1)

    if [ -z "$PLUGIN_JAR" ]; then
        error_exit "Connector plugin JAR not found at $PLUGIN_JAR_PATH/. Run './build_runtime_jar.sh . package apache' first."
    fi

    info "Using Apache connector JAR: $PLUGIN_JAR"
    cp "$PLUGIN_JAR" "$PLUGIN_DIR/"

elif [ "$PLATFORM" = "confluent" ]; then
    # Confluent: Look for zip file
    PLUGIN_ZIP="/tmp/sf-kafka-connect-plugin.zip"

    if [ ! -f "$PLUGIN_ZIP" ]; then
        error_exit "Connector plugin zip not found at $PLUGIN_ZIP. Run './build_runtime_jar.sh . package confluent' first."
    fi

    info "Extracting Confluent connector zip: $PLUGIN_ZIP"
    unzip -q "$PLUGIN_ZIP" -d "$PLUGIN_DIR"
fi

info "Plugin prepared in $PLUGIN_DIR"

# Build protobuf dependencies
EXTRA_JARS_DIR="/tmp/kafka-connect-extra-jars"
mkdir -p "$EXTRA_JARS_DIR"

compile_protobuf_dependencies() {
    info "Building protobuf dependencies..."
    cd "$DOCKER_DIR"
    
    docker build -t protobuf-builder -f Dockerfile.builder ..
    
    info "Extracting JARs from image..."
    CONTAINER_ID=$(docker create protobuf-builder)
    docker cp "$CONTAINER_ID:/output/." "$EXTRA_JARS_DIR/"
    docker rm "$CONTAINER_ID" > /dev/null
    
    info "Extra JARs prepared in $EXTRA_JARS_DIR:"
    ls -la "$EXTRA_JARS_DIR"
}

compile_protobuf_dependencies

if [ "$JMX_ENABLED" = "true" ]; then
    # Download Jolokia JMX agent for metrics scraping
    JOLOKIA_DIR="/tmp/jolokia"
    JOLOKIA_VERSION="2.5.1"
    JOLOKIA_JAR="$JOLOKIA_DIR/jolokia-agent.jar"
    mkdir -p "$JOLOKIA_DIR"
    if [ ! -f "$JOLOKIA_JAR" ]; then
        info "Downloading Jolokia JMX agent v${JOLOKIA_VERSION}..."
        curl -fsSL -o "$JOLOKIA_JAR" \
            "https://repo1.maven.org/maven2/org/jolokia/jolokia-agent-jvm/${JOLOKIA_VERSION}/jolokia-agent-jvm-${JOLOKIA_VERSION}-javaagent.jar"
    fi
    export JOLOKIA_JAR_PATH="$JOLOKIA_JAR"
    export KAFKA_OPTS="-javaagent:/opt/jolokia/jolokia-agent.jar=port=8778,host=0.0.0.0"
fi

# Generate test name salt
TEST_NAME_SALT="_$(echo $RANDOM$RANDOM | base64 | cut -c1-7)"
info "Test name salt: $TEST_NAME_SALT"

# Export environment for docker-compose
export CONFLUENT_VERSION
export KAFKA_VERSION
export JAVA_VERSION
export SNOWFLAKE_CREDENTIAL_FILE
export CONNECTOR_PLUGIN_PATH="$PLUGIN_DIR"
export EXTRA_JARS_PATH="$EXTRA_JARS_DIR"

cd "$DOCKER_DIR"

# Build images
BUILD_ARGS=""
if [ "$FORCE_REBUILD" = "true" ]; then
    BUILD_ARGS="--no-cache"
fi

info "Building test runner image..."
docker compose $COMPOSE_FILES build $BUILD_ARGS test-runner

if [ "$PLATFORM" = "apache" ]; then
    info "Building Apache Kafka image..."
    docker compose $COMPOSE_FILES build $BUILD_ARGS kafka
fi

# Start services
info "Starting services: $START_SERVICES"
docker compose $COMPOSE_FILES up -d $START_SERVICES

# Wait for services
info "Waiting for services to be healthy..."
TIMEOUT=300
ELAPSED=0

while [ $ELAPSED -lt $TIMEOUT ]; do
    if docker compose $COMPOSE_FILES ps $HEALTH_CHECK_SERVICE 2>/dev/null | grep -q "healthy"; then
        info "All services are healthy!"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -n "."
done
echo ""

if [ $ELAPSED -ge $TIMEOUT ]; then
    error_exit "Services failed to become healthy within ${TIMEOUT}s"
fi

# Start JMX metrics scraper in the background
METRICS_FILE="/tmp/sf-metrics-${PLATFORM}-${PLATFORM_VERSION}-$(date +%Y%m%d-%H%M%S).jsonl"
METRICS_PID=""

start_metrics_scraper() {
    local scraper="$PROJECT_ROOT/test/scripts/scrape_metrics.sh"
    if [ ! -x "$scraper" ]; then
        error_exit "Metrics scraper not found or not executable: $scraper"
    fi
    "$scraper" \
        --poll --interval=10 --output="$METRICS_FILE" --host=localhost --port=8778 &
    METRICS_PID=$!
    disown "$METRICS_PID" 2>/dev/null || true
}

stop_metrics_scraper() {
    if [ -n "$METRICS_PID" ] && kill -0 "$METRICS_PID" 2>/dev/null; then
        kill "$METRICS_PID" 2>/dev/null || true
        wait "$METRICS_PID" 2>/dev/null || true
        METRICS_PID=""
    fi
}

cleanup() {
    stop_metrics_scraper
    if [ "$KEEP_RUNNING" = "false" ]; then
        info "Cleaning up containers..."
        docker compose $COMPOSE_FILES down -v --remove-orphans 2>/dev/null || true
    else
        warn "Keeping containers running (--keep specified)"
        echo "To stop: cd $DOCKER_DIR && docker compose $COMPOSE_FILES down -v"
    fi
}
trap cleanup EXIT

if [ "$JMX_ENABLED" = "true" ]; then
    # Give Jolokia a moment to initialize, then start scraping
    sleep 3
    start_metrics_scraper
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  JMX Metrics: ${METRICS_FILE}${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
fi

# Build pytest arguments
PYTEST_ARGS=(
    -v
    --platform "$PLATFORM"
    --platform-version "$PLATFORM_VERSION"
    --name-salt "$TEST_NAME_SALT"
    --kafka-connect-address "$KAFKA_CONNECT_ADDRESS"
)

case $PLATFORM in
    confluent)
        PYTEST_ARGS+=(
            --kafka-address "kafka:29092"
            --schema-registry-address "http://schema-registry:8081"
        )
        ;;
    apache)
        PYTEST_ARGS+=(
            --kafka-address "kafka:9092"
            --schema-registry-address ""
        )
        ;;
esac

# Run tests
info "Running tests..."
set +e
docker compose $COMPOSE_FILES run --rm -i test-runner \
    pytest "${PYTEST_ARGS[@]}" "${PASSTHROUGH_ARGS[@]}"
TEST_EXIT_CODE=$?
set -e

# Stop the scraper before containers go away
stop_metrics_scraper

# Save logs on failure
if [ $TEST_EXIT_CODE -ne 0 ] && [ -n "$LOGS_DIR" ]; then
    mkdir -p "$LOGS_DIR"
    LOG_FILE="$LOGS_DIR/${PLATFORM}-${PLATFORM_VERSION}-${HEALTH_CHECK_SERVICE}.log"
    warn "Tests failed. Saving service logs to $LOG_FILE..."
    docker compose $COMPOSE_FILES logs $HEALTH_CHECK_SERVICE > "$LOG_FILE" 2>&1
fi

if [ "$JMX_ENABLED" = "true" ]; then
    # Print metrics summary
    METRICS_LINES=0
    if [ -f "$METRICS_FILE" ]; then
        METRICS_LINES=$(wc -l < "$METRICS_FILE")
    fi

    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  JMX Metrics: ${METRICS_FILE}${NC}"
    echo -e "${GREEN}  Snapshots collected: ${METRICS_LINES}${NC}"
    if [ "$METRICS_LINES" -gt 0 ] 2>/dev/null; then
        echo -e "${GREEN}  Analyze: ${PROJECT_ROOT}/test/scripts/analyze_metrics.sh ${METRICS_FILE}${NC}"
    fi
    echo -e "${GREEN}========================================${NC}"
fi

if [ $TEST_EXIT_CODE -ne 0 ]; then
    echo -e "\n${RED}========================================${NC}"
    echo -e "${RED}  TESTS FAILED (exit code: $TEST_EXIT_CODE)${NC}"
    echo -e "${RED}========================================${NC}"
    exit $TEST_EXIT_CODE
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  ALL TESTS PASSED${NC}"
echo -e "${GREEN}========================================${NC}"
exit 0
