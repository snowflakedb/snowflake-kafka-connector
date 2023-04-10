#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

DOWNLOAD_URL="https://github.com/google/google-java-format/releases/download/v1.10.0/google-java-format-1.10.0-all-deps.jar"
JAR_FILE="./.cache/google-java-format-1.10.0-all-deps.jar"

if [ ! -f "${JAR_FILE}" ]; then
  mkdir -p "$(dirname "${JAR_FILE}")"
  echo "Downloading Google Java format to ${JAR_FILE}"
  curl -# -L --fail "${DOWNLOAD_URL}" --output "${JAR_FILE}"
fi

if ! command -v java > /dev/null; then
  echo "Java not installed."
  exit 1
fi
echo "Running Google Java Format"
find ./src -type f -name "*.java" -print0 | xargs -0 java -jar "${JAR_FILE}" --replace --set-exit-if-changed && echo "OK"
