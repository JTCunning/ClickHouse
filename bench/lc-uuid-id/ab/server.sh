#!/usr/bin/env bash
# server.sh start NAME TCP_PORT | stop NAME
# One ClickHouse server per variant under $DATA/NAME, all from the same binary ($CH).
# Env: SERVER_MEM (bytes, default 2800000000), QUERY_MEM (default 2500000000),
#      PROFILE_EXTRA (XML put in the default profile, e.g. variant C's
#      '<low_cardinality_use_single_dictionary_for_part>1</low_cardinality_use_single_dictionary_for_part>';
#      the profile reaches merges too, see MergeTreeIOSettings.cpp:108-109).
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/lc-uuid-id-ab}
DIR=$(cd "$(dirname "$0")" && pwd)
cmd=$1 name=$2 d=$DATA/$2
case $cmd in
start)
  port=$3; mkdir -p "$d"
  sed -e "s|@TCP@|$port|" -e "s|@HTTP@|$((port + 1000))|" -e "s|@SERVER_MEM@|${SERVER_MEM:-2800000000}|" \
      -e "s|@QUERY_MEM@|${QUERY_MEM:-2500000000}|" -e "s|@PROFILE_EXTRA@|${PROFILE_EXTRA:-}|" "$DIR/config.xml.in" > "$d/config.xml"
  (cd "$d" && "$CH" server --config-file="$d/config.xml" --pid-file="$d/server.pid" --daemon)
  for _ in $(seq 60); do "$CH" client --port "$port" -q 'SELECT 1' >/dev/null 2>&1 && { echo "$name up on $port, pid $(cat "$d/server.pid")"; exit 0; }; sleep 1; done
  echo "$name did not start; see $d/err.log" >&2; exit 1 ;;
stop)
  pid=$(cat "$d/server.pid"); ps -p "$pid" -o pid=,command= | grep -q clickhouse || { echo "pid $pid is not a clickhouse server" >&2; exit 1; }
  kill "$pid"; for _ in $(seq 60); do kill -0 "$pid" 2>/dev/null || { echo "$name stopped"; exit 0; }; sleep 1; done
  echo "$name (pid $pid) still running after 60 s" >&2; exit 1 ;;
esac
