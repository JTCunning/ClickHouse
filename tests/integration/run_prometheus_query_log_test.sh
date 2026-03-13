#!/usr/bin/env bash
# Run the Prometheus query log integration test locally with Docker.
#
# Requires: Docker.
#
# ClickHouse binary:
#   - On Linux: uses build/programs/clickhouse if present, else set CLICKHOUSE_TESTS_SERVER_BIN_PATH.
#   - On macOS: host build is Mach-O and cannot run in the Linux container; the script
#     downloads a Linux binary (aarch64 on Apple Silicon, amd64 on Intel) to ci/tmp/
#     unless CLICKHOUSE_TESTS_SERVER_BIN_PATH points to a Linux binary.
#
# Usage (from repo root):
#   ./tests/integration/run_prometheus_query_log_test.sh
#
# Optional: pass extra pytest options, e.g. -v -s
#   ./tests/integration/run_prometheus_query_log_test.sh -v -s

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

mkdir -p ci/tmp

# On macOS, host-built binary is Mach-O and cannot run in Linux Docker; use downloaded Linux binary.
if [[ "$(uname -s)" == "Darwin" ]]; then
  ARCH=$(uname -m)
  if [[ "$ARCH" == "arm64" ]]; then
    LINUX_ARCH="aarch64"
  else
    LINUX_ARCH="amd64"
  fi
  LINUX_BINARY="$REPO_ROOT/ci/tmp/clickhouse_$LINUX_ARCH"
  if [[ -z "$CLICKHOUSE_TESTS_SERVER_BIN_PATH" ]] || [[ "$CLICKHOUSE_TESTS_SERVER_BIN_PATH" == "$REPO_ROOT/build/programs/clickhouse" ]]; then
    if [[ ! -x "$LINUX_BINARY" ]]; then
      echo "Downloading Linux $LINUX_ARCH ClickHouse binary to $LINUX_BINARY (required for Docker on macOS)..."
      URL="https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/$LINUX_ARCH/clickhouse"
      if command -v wget &>/dev/null; then
        wget -q -O "$LINUX_BINARY" "$URL"
      else
        curl -sSL -o "$LINUX_BINARY" "$URL"
      fi
      chmod +x "$LINUX_BINARY"
    fi
    CLICKHOUSE_BIN="$LINUX_BINARY"
  else
    CLICKHOUSE_BIN="$CLICKHOUSE_TESTS_SERVER_BIN_PATH"
  fi
else
  CLICKHOUSE_BIN="${CLICKHOUSE_TESTS_SERVER_BIN_PATH:-$REPO_ROOT/build/programs/clickhouse}"
fi

if [[ ! -x "$CLICKHOUSE_BIN" ]]; then
  echo "ClickHouse binary not found at $CLICKHOUSE_BIN. Build it or set CLICKHOUSE_TESTS_SERVER_BIN_PATH."
  exit 1
fi

export CLICKHOUSE_TESTS_SERVER_BIN_PATH="$CLICKHOUSE_BIN"
export CLICKHOUSE_TESTS_BASE_CONFIG_DIR="$REPO_ROOT/programs/server"
export CLICKHOUSE_TESTS_CLIENT_BIN_PATH="$CLICKHOUSE_BIN"
export CLICKHOUSE_BINARY="$CLICKHOUSE_BIN"
export DOCKER_BASE_TAG="${DOCKER_BASE_TAG:-latest}"
export PYTEST_CLEANUP_CONTAINERS=1

echo "Using ClickHouse binary: $CLICKHOUSE_BIN"
echo "DOCKER_BASE_TAG=$DOCKER_BASE_TAG"
echo "Running pytest in tests/integration ..."

cd "$SCRIPT_DIR"
.venv/bin/python -m pytest test_prometheus_protocols/test_prometheus_query_log.py -v -p no:warnings "$@"
