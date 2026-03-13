#!/usr/bin/env bash
# Run the Prometheus query log integration test inside the CI runner container.
# This avoids host→container networking issues on macOS/OrbStack: pytest and
# ClickHouse both run in Docker, so they can reach each other.
#
# Requires: Docker. From repo root:
#   ./tests/integration/run_prometheus_query_log_in_container.sh
#
# Uses: clickhouse/integration-tests-runner (pull with :latest or :latest_arm on Apple Silicon).
# The runner needs Docker (socket or DinD); the job config mounts the socket and a volume.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Ensure we have a Linux ClickHouse binary for the container (host binary on macOS is Mach-O).
CLICKHOUSE_BIN=""
if [[ "$(uname -s)" == "Darwin" ]]; then
  ARCH=$(uname -m)
  if [[ "$ARCH" == "arm64" ]]; then
    LINUX_ARCH="aarch64"
  else
    LINUX_ARCH="amd64"
  fi
  LINUX_BINARY="$REPO_ROOT/ci/tmp/clickhouse_$LINUX_ARCH"
  if [[ ! -x "$LINUX_BINARY" ]]; then
    echo "Downloading Linux $LINUX_ARCH ClickHouse binary to $LINUX_BINARY..."
    mkdir -p ci/tmp
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
  CLICKHOUSE_BIN="${CLICKHOUSE_TESTS_SERVER_BIN_PATH:-$REPO_ROOT/build/programs/clickhouse}"
  if [[ ! -x "$CLICKHOUSE_BIN" ]]; then
    echo "ClickHouse binary not found at $CLICKHOUSE_BIN. Build it or set CLICKHOUSE_TESTS_SERVER_BIN_PATH."
    exit 1
  fi
fi

# Runner image: use latest_arm on Apple Silicon if latest is amd64-only.
RUNNER_IMAGE="clickhouse/integration-tests-runner:latest"
if [[ "$(uname -s)" == "Darwin" ]] && [[ "$(uname -m)" == "arm64" ]]; then
  if docker manifest inspect clickhouse/integration-tests-runner:latest_arm &>/dev/null; then
    RUNNER_IMAGE="clickhouse/integration-tests-runner:latest_arm"
  fi
fi

echo "Using ClickHouse binary: $CLICKHOUSE_BIN"
echo "Using runner image: $RUNNER_IMAGE"
echo "Running integration test inside container (pytest and ClickHouse in Docker)..."

# Run via praktika so the job runs inside the runner container; --docker skips workflow config.
PYTHONPATH=".:./ci" python3 -m ci.praktika run "integration" \
  --test "test_prometheus_protocols/test_prometheus_query_log.py" \
  --docker "$RUNNER_IMAGE" \
  --path "$CLICKHOUSE_BIN"
