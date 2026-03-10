#!/bin/bash
#
# Downloads the KC v3 connector JAR from Maven Central.
# Skips download if the JAR already exists at the target path.
#
# Usage:
#   ./download_v3_jar.sh [target_dir]
#
# Default target: /tmp/sf-kafka-connect-v3
#

set -e

V3_VERSION="3.5.3"
JAR_NAME="snowflake-kafka-connector-${V3_VERSION}.jar"
MAVEN_URL="https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/${V3_VERSION}/${JAR_NAME}"

TARGET_DIR="${1:-/tmp/sf-kafka-connect-v3}"
TARGET_JAR="${TARGET_DIR}/${JAR_NAME}"

if [ -f "$TARGET_JAR" ]; then
    echo "KC v3 JAR already exists: $TARGET_JAR (skipping download)" >&2
    echo "$TARGET_DIR"
    exit 0
fi

mkdir -p "$TARGET_DIR"

echo "Downloading KC v3 JAR (${V3_VERSION}) from Maven Central..." >&2
curl -fSL -o "$TARGET_JAR" "$MAVEN_URL"
echo "Downloaded: $TARGET_JAR" >&2

echo "$TARGET_DIR"
