#!/bin/bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <demo_id> [show args...]" >&2
  echo "Example: $0 demorun_1 --expected-row-count 4000 --poll-interval-sec 0.5" >&2
  exit 1
fi

DEMO_ID="$1"
shift
PROFILE_PATH="${SNOWFLAKE_CREDENTIAL_FILE:-/home/repo/snowflake-kafka-connector/profile.json}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ ! -f "${PROFILE_PATH}" ]]; then
  echo "Profile file not found: ${PROFILE_PATH}" >&2
  exit 1
fi

if [[ -z "${LD_LIBRARY_PATH:-}" ]]; then
  export LD_LIBRARY_PATH="$HOME/.local/rdkafka-1.9.2/lib"
fi

cd "${TEST_DIR}"

SHOW_ARGS=("$@")
has_expected=false
has_poll=false
has_stop=false
for arg in "${SHOW_ARGS[@]}"; do
  if [[ "${arg}" == "--expected-row-count" ]]; then
    has_expected=true
  elif [[ "${arg}" == "--poll-interval-sec" ]]; then
    has_poll=true
  elif [[ "${arg}" == "--stop-at-expected" ]]; then
    has_stop=true
  fi
done

if [[ "${has_expected}" == false ]]; then
  SHOW_ARGS+=(--expected-row-count 2000)
fi
if [[ "${has_poll}" == false ]]; then
  SHOW_ARGS+=(--poll-interval-sec 0.5)
fi
if [[ "${has_stop}" == false ]]; then
  SHOW_ARGS+=(--stop-at-expected)
fi

python "${SCRIPT_DIR}/live_demo.py" \
  --profile "${PROFILE_PATH}" \
  --demo-id "${DEMO_ID}" \
  show \
  "${SHOW_ARGS[@]}"
