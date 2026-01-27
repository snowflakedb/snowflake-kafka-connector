#!/bin/bash
#
# Snowflake Kafka Connector - Docker-based E2E Tests
#
# Usage:
#   ./run_tests.sh <confluent_version> [options]
#
# Examples:
#   ./run_tests.sh 7.8.2
#   ./run_tests.sh 6.2.15 --tests=TestStringJson,TestAvro
#   ./run_tests.sh 7.8.2 --pressure
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
    echo "Usage: $0 <confluent_version> [options]"
    echo ""
    echo "Arguments:"
    echo "  confluent_version    Confluent Platform version (e.g., 7.8.2, 6.2.15)"
    echo ""
    echo "Options:"
    echo "  --tests=TEST1,TEST2  Run specific tests only"
    echo "  --pressure           Run pressure tests"
    echo "  --keep               Keep containers running after tests"
    echo "  --rebuild            Force rebuild of test runner image"
    echo "  --logs               Show service logs on failure"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Environment:"
    echo "  SNOWFLAKE_CREDENTIAL_FILE  Path to Snowflake credentials JSON (required)"
    echo ""
    echo "Examples:"
    echo "  $0 7.8.2"
    echo "  $0 7.8.2 --tests=TestStringJson"
    echo "  $0 6.2.15 --pressure --keep"
    exit 1
}

# Parse arguments
if [ $# -lt 1 ]; then
    usage
fi

CONFLUENT_VERSION="$1"
shift

TESTS_TO_RUN=""
PRESSURE_TEST="false"
KEEP_RUNNING="false"
FORCE_REBUILD="false"
SHOW_LOGS="false"

while [[ $# -gt 0 ]]; do
    case $1 in
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

# Validate Confluent version and set compose files
# Confluent 6.2.x images are only available for linux/amd64, so we need the platform override
case $CONFLUENT_VERSION in
    6.2.*)
        info "Using Confluent version: $CONFLUENT_VERSION"
        # 6.2.x containers are only available for linux/amd64, use override file for ARM emulation
        COMPOSE_FILES="-f docker-compose.yml -f docker-compose.amd64.yml"
        info "Note: Confluent 6.2.x requires linux/amd64 platform (using emulation on ARM)"
        ;;
    7.8.*)
        info "Using Confluent version: $CONFLUENT_VERSION"
        # 7.x has native ARM64 support, no platform override needed
        COMPOSE_FILES="-f docker-compose.yml"
        ;;
    *)
        error_exit "Unsupported Confluent version: $CONFLUENT_VERSION (supported: 6.2.x, 7.8.x)"
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
info "Using credentials: $SNOWFLAKE_CREDENTIAL_FILE"

# Check for connector plugin
PLUGIN_ZIP="/tmp/sf-kafka-connect-plugin.zip"
PLUGIN_DIR="/tmp/sf-kafka-connect-plugin"

if [ ! -f "$PLUGIN_ZIP" ]; then
    error_exit "Connector plugin not found at $PLUGIN_ZIP. Run build_runtime_jar.sh first."
fi

# Extract plugin if needed
info "Preparing connector plugin..."
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"
unzip -q "$PLUGIN_ZIP" -d "$PLUGIN_DIR"
info "Plugin extracted to $PLUGIN_DIR"

# Build protobuf converter and test data (mirrors compile_protobuf_converter_and_data from utils.sh)
EXTRA_JARS_DIR="/tmp/kafka-connect-extra-jars"
mkdir -p "$EXTRA_JARS_DIR"

compile_protobuf_dependencies() {
    info "Building protobuf dependencies..."
    
    # Check for required tools
    if ! command -v mvn >/dev/null 2>&1; then
        warn "Maven not found - skipping protobuf converter build. Protobuf tests will fail."
        return 0
    fi
    
    if ! command -v protoc >/dev/null 2>&1; then
        warn "protoc not found - skipping protobuf compilation. Protobuf tests will fail."
        return 0
    fi
    
    local TEST_DIR="$SCRIPT_DIR/.."
    
    # 1. Compile protobuf to Java and Python
    info "Compiling protobuf schema..."
    pushd "$TEST_DIR/test_data" > /dev/null
    local PROTOBUF_JAVA_DIR="protobuf/src/main/java"
    mkdir -p "$PROTOBUF_JAVA_DIR"
    
    # Use protobuf@21 if available (compatible with protobuf-java 3.21.x)
    local PROTOC_CMD="${PROTOC:-}"
    if [ -z "$PROTOC_CMD" ]; then
        if [ -x "/opt/homebrew/opt/protobuf@21/bin/protoc" ]; then
            PROTOC_CMD="/opt/homebrew/opt/protobuf@21/bin/protoc"
        else
            PROTOC_CMD="protoc"
        fi
    fi
    
    $PROTOC_CMD --java_out="$PROTOBUF_JAVA_DIR" sensor.proto
    $PROTOC_CMD --python_out=. sensor.proto
    info "Protobuf schema compiled"
    popd > /dev/null
    
    # 2. Build protobuf test data JAR
    info "Building protobuf test data JAR..."
    pushd "$TEST_DIR/test_data/protobuf" > /dev/null
    mvn clean package -q -DskipTests
    cp target/kafka-test-protobuf-*-jar-with-dependencies.jar "$EXTRA_JARS_DIR/" 2>/dev/null || true
    info "Protobuf test data JAR built"
    popd > /dev/null
    
    # 3. Clone and build BlueApron protobuf converter
    local CONVERTER_VERSION="3.1.0"
    local CONVERTER_DIR="$TEST_DIR/kafka-connect-protobuf-converter"
    
    if [ ! -d "$CONVERTER_DIR" ]; then
        info "Cloning BlueApron protobuf converter..."
        git clone -q "https://github.com/blueapron/kafka-connect-protobuf-converter" "$CONVERTER_DIR"
    fi
    
    info "Building BlueApron protobuf converter..."
    pushd "$CONVERTER_DIR" > /dev/null
    git checkout -q "tags/v$CONVERTER_VERSION" 2>/dev/null || true
    mvn clean package -q -DskipTests
    cp target/kafka-connect-protobuf-converter-*-jar-with-dependencies.jar "$EXTRA_JARS_DIR/" 2>/dev/null || true
    info "BlueApron protobuf converter built"
    popd > /dev/null
    
    info "Extra JARs prepared in $EXTRA_JARS_DIR:"
    ls -la "$EXTRA_JARS_DIR"
}

compile_protobuf_dependencies

# Generate test name salt
TEST_NAME_SALT="_$(echo $RANDOM$RANDOM | base64 | cut -c1-7)"
info "Test name salt: $TEST_NAME_SALT"

# Export environment for docker-compose
export CONFLUENT_VERSION
export SNOWFLAKE_CREDENTIAL_FILE
export CONNECTOR_PLUGIN_PATH="$PLUGIN_DIR"
export EXTRA_JARS_PATH="$EXTRA_JARS_DIR"
export TEST_NAME_SALT
export PRESSURE_TEST
export TESTS_TO_RUN

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

# Build test runner image
BUILD_ARGS=""
if [ "$FORCE_REBUILD" = "true" ]; then
    BUILD_ARGS="--no-cache"
fi

info "Building test runner image..."
docker compose $COMPOSE_FILES build $BUILD_ARGS test-runner

# Start services
info "Starting Kafka cluster (Confluent $CONFLUENT_VERSION)..."
docker compose $COMPOSE_FILES up -d zookeeper kafka schema-registry kafka-connect

# Wait for services
info "Waiting for services to be healthy..."
TIMEOUT=180
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    if docker compose $COMPOSE_FILES ps kafka-connect | grep -q "healthy"; then
        info "All services are healthy!"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
    echo -n "."
done
echo ""

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo ""
    error_exit "Services failed to become healthy within ${TIMEOUT}s"
fi

# Run tests
info "Running tests..."
set +e
docker compose $COMPOSE_FILES run --rm -it test-runner
TEST_EXIT_CODE=$?
set -e

# Show logs on failure if requested
if [ $TEST_EXIT_CODE -ne 0 ] && [ "$SHOW_LOGS" = "true" ]; then
    warn "Tests failed. Showing service logs..."
    docker compose $COMPOSE_FILES logs kafka-connect
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
