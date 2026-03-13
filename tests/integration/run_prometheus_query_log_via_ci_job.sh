#!/usr/bin/env bash
# Run the Prometheus query log integration test via the CI integration job script
# (same as: python3 -m ci.praktika run "integration" --test "..." --no-docker,
# but with the right Python deps and Linux binary on macOS).
#
# Requires: Docker. From repo root:
#   ./tests/integration/run_prometheus_query_log_via_ci_job.sh
#
# One-time venv deps (script installs more_itertools if missing; install the rest if you hit import errors):
#   .venv/bin/pip install more_itertools cryptography pytest-reportlog pytest-xdist pytest-timeout pyspark
# On macOS a Linux ClickHouse binary is downloaded to ci/tmp/ if missing.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

mkdir -p ci/tmp
VENV_PYTHON="$REPO_ROOT/tests/integration/.venv/bin/python"
VENV_BIN="$REPO_ROOT/tests/integration/.venv/bin"

# Ensure job and pytest deps in venv (needed when running job script directly)
for pkg in more_itertools cryptography pytest-reportlog pytest-xdist pytest-timeout pyspark; do
  if ! "$VENV_PYTHON" -c "import ${pkg//-/_}" 2>/dev/null; then
    echo "Installing $pkg in integration venv..."
    "$VENV_BIN/pip" install "$pkg" --quiet
  fi
done

# On macOS, host binary is Mach-O; job needs a Linux binary for Docker containers.
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

export PATH="$VENV_BIN:$PATH"
export PYTHONPATH=".:./ci"

echo "Using ClickHouse binary: $CLICKHOUSE_BIN"
echo "Running integration test job (single test)..."

"$VENV_PYTHON" ./ci/jobs/integration_test_job.py \
  --options 'amd_tsan, 1/6' \
  --test test_prometheus_protocols/test_prometheus_query_log.py \
  --path "$CLICKHOUSE_BIN"
