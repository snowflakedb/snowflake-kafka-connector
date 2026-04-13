#!/bin/bash
#
# Profile the Kafka Connect worker running in Docker.
#
# Wraps JFR and async-profiler commands against the Kafka Connect container.
# Requires the profiling overlay (docker-compose.profile-confluent.yml or
# docker-compose.profile-apache.yml) to be active.
#
# Usage:
#   ./profile_connect.sh <command> [options]
#
# Commands:
#   jfr-dump                    Dump the continuous JFR recording to a file
#   jfr-stop                    Stop JFR and dump final recording
#   heap-dump                   Take a heap dump (hprof)
#   thread-dump                 Print thread dump to stdout
#   async-cpu [DURATION]        CPU flame graph via async-profiler (default: 60s)
#   async-alloc [DURATION]      Allocation flame graph via async-profiler (default: 60s)
#   async-wall [DURATION]       Wall-clock flame graph via async-profiler (default: 60s)
#   collect [OUTPUT_DIR]        Collect all profiling artifacts from the container
#   status                      Show JFR recording status and JVM info
#
# Examples:
#   ./profile_connect.sh status
#   ./profile_connect.sh async-cpu 30
#   ./profile_connect.sh jfr-dump
#   ./profile_connect.sh heap-dump
#   ./profile_connect.sh collect ./profiling-results
#
# Prerequisites:
#   - Containers running with docker-compose.profile.yml overlay
#   - For async-* commands: async-profiler mounted via ASYNC_PROFILER_PATH env var

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/../docker"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error_exit() { echo -e "${RED}ERROR: $1${NC}" >&2; exit 1; }
info() { echo -e "${GREEN}INFO: $1${NC}"; }
warn() { echo -e "${YELLOW}WARN: $1${NC}"; }

# Detect which container is running Kafka Connect
detect_container() {
    local project_root
    project_root="$(cd "$SCRIPT_DIR/../.." && pwd)"
    local project_name
    project_name="$(basename "$project_root")"

    # Try confluent kafka-connect first, then apache kafka
    local container
    container=$(docker ps --filter "name=${project_name}.*kafka-connect" --format '{{.Names}}' | head -1)
    if [ -z "$container" ]; then
        container=$(docker ps --filter "name=${project_name}.*kafka" --format '{{.Names}}' | head -1)
    fi

    if [ -z "$container" ]; then
        error_exit "No Kafka Connect container found. Is the test environment running?"
    fi
    echo "$container"
}

# Find the Kafka Connect JVM PID inside the container
find_kc_pid() {
    local container="$1"
    # Kafka Connect main class
    local pid
    pid=$(docker exec "$container" jcmd 2>/dev/null \
        | grep -i "ConnectDistributed\|connect-distributed" \
        | awk '{print $1}' | head -1)

    if [ -z "$pid" ]; then
        # Fallback: find any java process
        pid=$(docker exec "$container" jcmd 2>/dev/null \
            | grep -v "^$\|jcmd" | head -1 | awk '{print $1}')
    fi

    if [ -z "$pid" ]; then
        error_exit "Cannot find Kafka Connect JVM PID in container $container"
    fi
    echo "$pid"
}

COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    status)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        info "Container: $CONTAINER"
        info "Kafka Connect PID: $PID"
        echo ""
        echo "=== JFR Recordings ==="
        docker exec "$CONTAINER" jcmd "$PID" JFR.check 2>/dev/null || echo "(no JFR recordings)"
        echo ""
        echo "=== VM Info ==="
        docker exec "$CONTAINER" jcmd "$PID" VM.info 2>/dev/null | head -20
        echo ""
        echo "=== Heap Usage ==="
        docker exec "$CONTAINER" jcmd "$PID" GC.heap_info 2>/dev/null || true
        ;;

    jfr-dump)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        OUTFILE="/tmp/profile/kc-profile-$(date +%Y%m%d-%H%M%S).jfr"
        info "Dumping JFR recording to $OUTFILE..."
        docker exec "$CONTAINER" jcmd "$PID" JFR.dump name=profile filename="$OUTFILE"
        info "Done. Retrieve with: docker cp $CONTAINER:$OUTFILE ."
        ;;

    jfr-stop)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        OUTFILE="/tmp/profile/kc-profile-final.jfr"
        info "Stopping JFR recording..."
        docker exec "$CONTAINER" jcmd "$PID" JFR.stop name=profile filename="$OUTFILE"
        info "Final recording at $OUTFILE"
        info "Retrieve with: docker cp $CONTAINER:$OUTFILE ."
        ;;

    heap-dump)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        OUTFILE="/tmp/profile/heap-$(date +%Y%m%d-%H%M%S).hprof"
        info "Taking heap dump (this may pause the JVM briefly)..."
        docker exec "$CONTAINER" jcmd "$PID" GC.heap_dump "$OUTFILE"
        info "Heap dump at $OUTFILE"
        info "Retrieve with: docker cp $CONTAINER:$OUTFILE ."
        ;;

    thread-dump)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        docker exec "$CONTAINER" jcmd "$PID" Thread.print
        ;;

    async-cpu|async-alloc|async-wall)
        CONTAINER=$(detect_container)
        PID=$(find_kc_pid "$CONTAINER")
        DURATION="${1:-60}"
        EVENT="${COMMAND#async-}"

        # Locate async-profiler (prefer /opt mount, fall back to /tmp copy)
        ASPROF=""
        for candidate in /opt/async-profiler/bin/asprof /tmp/async-profiler/bin/asprof; do
            if docker exec "$CONTAINER" test -f "$candidate" 2>/dev/null; then
                ASPROF="$candidate"
                break
            fi
        done
        if [ -z "$ASPROF" ]; then
            error_exit "async-profiler not found in container. Set ASYNC_PROFILER_PATH or docker cp it to /tmp/async-profiler/."
        fi

        # Use itimer for cpu profiling — perf_event_open is typically restricted
        # in containers even with SYS_PTRACE. itimer uses SIGPROF instead.
        if [ "$EVENT" = "cpu" ]; then
            EVENT="itimer"
        fi

        OUTFILE="/tmp/profile/flamegraph-${COMMAND#async-}-$(date +%Y%m%d-%H%M%S).html"
        info "Profiling $EVENT for ${DURATION}s (PID $PID)..."
        docker exec "$CONTAINER" "$ASPROF" \
            -d "$DURATION" -f "$OUTFILE" -e "$EVENT" "$PID"
        info "Flame graph at $OUTFILE"
        info "Retrieve with: docker cp $CONTAINER:$OUTFILE ."
        ;;

    collect)
        CONTAINER=$(detect_container)
        OUTPUT_DIR="${1:-./profiling-results-$(date +%Y%m%d-%H%M%S)}"
        mkdir -p "$OUTPUT_DIR"

        info "Collecting profiling artifacts from $CONTAINER into $OUTPUT_DIR/"

        # Dump JFR before collecting
        PID=$(find_kc_pid "$CONTAINER")
        docker exec "$CONTAINER" jcmd "$PID" JFR.dump name=profile \
            filename="/tmp/profile/kc-profile-collected.jfr" 2>/dev/null || true

        # Copy via tar pipe — docker cp cannot read from tmpfs mounts
        if ! docker exec "$CONTAINER" test -d /tmp/profile 2>/dev/null; then
            warn "/tmp/profile does not exist in container. Was --profile enabled?"
        elif [ "$(docker exec "$CONTAINER" find /tmp/profile -maxdepth 1 -type f | wc -l)" -eq 0 ]; then
            warn "/tmp/profile is empty — no profiling artifacts to collect."
        else
            docker exec "$CONTAINER" tar cf - -C /tmp/profile . \
                | tar xf - -C "$OUTPUT_DIR/"
        fi

        info "Collected artifacts:"
        ls -lh "$OUTPUT_DIR/"

        # Print analysis hints
        echo ""
        echo "=== Analysis ==="
        echo "  JFR:    jfr summary $OUTPUT_DIR/kc-profile-collected.jfr"
        echo "          jfr print --events jdk.ExecutionSample $OUTPUT_DIR/*.jfr | head -100"
        echo "          jfr print --events jdk.ObjectAllocationSample $OUTPUT_DIR/*.jfr | head -100"
        echo "  GC:     Upload $OUTPUT_DIR/gc.log to https://gceasy.io"
        echo "  Heap:   Open $OUTPUT_DIR/*.hprof in Eclipse MAT"
        echo "  Flames: Open $OUTPUT_DIR/flamegraph-*.html in a browser"
        ;;

    help|--help|-h)
        head -30 "$0" | grep "^#" | sed 's/^# \?//'
        ;;

    *)
        error_exit "Unknown command: $COMMAND. Run '$0 help' for usage."
        ;;
esac
