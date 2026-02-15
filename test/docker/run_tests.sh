#!/bin/bash
#
# Snowflake Kafka Connector - Docker-based E2E Tests
#
# Usage:
#   ./run_tests.sh --platform=<confluent|apache> --version=<version> [options]
#
# Examples:
#   ./run_tests.sh --platform=apache --version=2.8.2
#   ./run_tests.sh --platform=apache --version=3.7.0
#   ./run_tests.sh --platform=confluent --version=7.8.0
#   ./run_tests.sh --platform=confluent --version=6.2.15 --tests=TestStringJson
#
# Prerequisites:
#   - Docker and Docker Compose
#   - SNOWFLAKE_CREDENTIAL_FILE environment variable set
#   - Connector plugin built (run build_runtime_jar.sh first)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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
    echo "Usage: $0 --platform=<confluent|apache> --version=<version> [options]"
    echo ""
    echo "Required:"
    echo "  --platform=PLATFORM  Platform: 'confluent' or 'apache'"
    echo "  --version=VERSION    Kafka version"
    echo "                       Confluent: 7.8.x, 6.2.x"
    echo "                       Apache: any version (e.g., 2.8.2, 3.7.0)"
    echo ""
    echo "Options:"
    echo "  --cloud=CLOUD        Snowflake cloud platform: AWS, GCP, or AZURE"
    echo "  --java-version=VER   Java version for Apache Kafka (default: 11)"
    echo "  --tests=TEST1,TEST2  Run specific tests only"
    echo "  --pressure           Run pressure tests"
    echo "  --keep               Keep containers running after tests"
    echo "  --rebuild            Force rebuild of images"
    echo "  --logs               Show service logs on failure"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Environment:"
    echo "  SNOWFLAKE_CREDENTIAL_FILE  Path to Snowflake credentials JSON (required)"
    echo "  LOCAL_PROXY_PORT           Port of the proxy for the local Snowflake deployment"
    echo ""
    echo "Examples:"
    echo "  $0 --platform=confluent --version=7.8.0"
    echo "  $0 --platform=apache --version=2.8.2"
    echo "  $0 --platform=confluent --version=7.8.0 --tests=TestStringJson"
    echo "  $0 --platform=apache --version=3.7.0 --pressure --keep"
    exit 1
}

# Parse arguments
PLATFORM=""
VERSION=""
JAVA_VERSION="11"
TESTS_TO_RUN=""
PRESSURE_TEST="false"
KEEP_RUNNING="false"
FORCE_REBUILD="false"
SHOW_LOGS="false"

while [[ $# -gt 0 ]]; do
    case $1 in
        --platform=*)
            PLATFORM="${1#*=}"
            shift
            ;;
        --version=*)
            VERSION="${1#*=}"
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
        --tests=*)
            TESTS_TO_RUN="${1#*=}"
            shift
            ;;
        --pressure)
            PRESSURE_TEST="true"
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
        --logs)
            SHOW_LOGS="true"
            shift
            ;;
        -h|--help)
            usage
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

if [ -z "$VERSION" ]; then
    error_exit "Missing required argument: --version=<version>"
fi

# Base compose file + platform-specific compose file
BASE_COMPOSE="-f docker-compose.base.yml"

case $PLATFORM in
    confluent)
        case $VERSION in
            6.2.*)
                info "Platform: Confluent $VERSION"
                # 6.2.x containers are only available for linux/amd64
                COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.confluent.yml -f docker-compose.amd64.yml"
                info "Note: Confluent 6.2.x requires linux/amd64 (using emulation on ARM)"
                ;;
            7.*)
                info "Platform: Confluent $VERSION"
                COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.confluent.yml"
                ;;
            *)
                error_exit "Unsupported Confluent version: $VERSION (supported: 6.2.x, 7.x)"
                ;;
        esac
        TEST_SET="confluent"
        CONFLUENT_VERSION="$VERSION"
        KAFKA_VERSION=""
        HEALTH_CHECK_SERVICE="kafka-connect"
        START_SERVICES="zookeeper kafka schema-registry kafka-connect"
        ;;
    apache)
        info "Platform: Apache Kafka $VERSION (official tarball)"
        COMPOSE_FILES="$BASE_COMPOSE -f docker-compose.apache.yml"
        TEST_SET="apache"
        CONFLUENT_VERSION=""
        KAFKA_VERSION="$VERSION"
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
if [ -n "${LOCAL_PROXY_PORT:-}" ]; then
    # Fetch credentials from local proxy
    PROXY_CREDENTIAL_URL="http://localhost:${LOCAL_PROXY_PORT}/proxy/kafka-connector-profile"
    info "Fetching credentials from proxy: $PROXY_CREDENTIAL_URL"
    SNOWFLAKE_CREDENTIAL_FILE="$(mktemp /tmp/kafka-connector-test-snowflake-credentials-XXXXXX.json)"
    if ! curl -sf "$PROXY_CREDENTIAL_URL" -o "$SNOWFLAKE_CREDENTIAL_FILE"; then
        rm -f "$SNOWFLAKE_CREDENTIAL_FILE"
        error_exit "Failed to fetch credentials from $PROXY_CREDENTIAL_URL"
    fi
    info "Credentials fetched to: $SNOWFLAKE_CREDENTIAL_FILE"
else
    if [ -z "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
        error_exit "SNOWFLAKE_CREDENTIAL_FILE environment variable is not set"
    fi

    if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
        error_exit "Credential file not found: $SNOWFLAKE_CREDENTIAL_FILE"
    fi

    # Convert to absolute path
    SNOWFLAKE_CREDENTIAL_FILE="$(cd "$(dirname "$SNOWFLAKE_CREDENTIAL_FILE")" && pwd)/$(basename "$SNOWFLAKE_CREDENTIAL_FILE")"
fi
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
    cd "$SCRIPT_DIR"
    
    docker build -t protobuf-builder -f Dockerfile.builder ..
    
    info "Extracting JARs from image..."
    CONTAINER_ID=$(docker create protobuf-builder)
    docker cp "$CONTAINER_ID:/output/." "$EXTRA_JARS_DIR/"
    docker rm "$CONTAINER_ID" > /dev/null
    
    info "Extra JARs prepared in $EXTRA_JARS_DIR:"
    ls -la "$EXTRA_JARS_DIR"
}

compile_protobuf_dependencies

# Generate test name salt
TEST_NAME_SALT="_$(echo $RANDOM$RANDOM | base64 | cut -c1-7)"
info "Test name salt: $TEST_NAME_SALT"

# Export environment for docker-compose (no defaults - all explicit)
export CONFLUENT_VERSION
export KAFKA_VERSION
export JAVA_VERSION
export TEST_SET
export SNOWFLAKE_CREDENTIAL_FILE
export CONNECTOR_PLUGIN_PATH="$PLUGIN_DIR"
export EXTRA_JARS_PATH="$EXTRA_JARS_DIR"
export TEST_NAME_SALT
export PRESSURE_TEST
export TESTS_TO_RUN
export SF_CLOUD_PLATFORM="${SF_CLOUD_PLATFORM:-}"
if [ -n "${LOCAL_PROXY_PORT:-}" ]; then
    export SNOWPIPE_STREAMING_URL="http://host.docker.internal:${LOCAL_PROXY_PORT}"
    info "Snowpipe Streaming URL: $SNOWPIPE_STREAMING_URL"
fi

cd "$SCRIPT_DIR"

# Cleanup function
cleanup() {
    if [ "$KEEP_RUNNING" = "false" ]; then
        info "Cleaning up containers..."
        docker compose $COMPOSE_FILES down -v --remove-orphans 2>/dev/null || true
    else
        warn "Keeping containers running (--keep specified)"
        echo "To stop: cd $SCRIPT_DIR && docker compose $COMPOSE_FILES down -v"
    fi
}

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

# Run tests
info "Running tests..."
set +e
docker compose $COMPOSE_FILES run --rm -i test-runner
TEST_EXIT_CODE=$?
set -e

# Show logs on failure
if [ $TEST_EXIT_CODE -ne 0 ] && [ "$SHOW_LOGS" = "true" ]; then
    warn "Tests failed. Showing service logs..."
    docker compose $COMPOSE_FILES logs $HEALTH_CHECK_SERVICE
fi

# Cleanup
trap cleanup EXIT

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
