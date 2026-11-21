#!/usr/bin/env bash
# scale.sh: the re-baseline matrix of research/ab-plans/remote-write-baseline.md on ONE server (master build, no B).
# Each round runs every cell (SENDERS x MERGES) in an order that rotates per round. Per cell: a fresh `prometheus`
# TimeSeries table (so the tags dedup cache starts empty), merges stopped on its inner tables if MERGES=stop, a cold pass
# (first sight of every series), then the measured warm pass with SETTLE=1 (run.sh waits for the merges the pass
# caused, so their CPU counts and none leaks into the next cell). Prints run.sh's result lines prefixed with
# "round=<r> merges=<m>" ; the cold line carries the label "cold", the warm line "warm".
# Start the server first with ../remote-write-arena/server.sh (remote write on TCP_PORT + 2000, path /write), e.g.
#   DATA=$D SERVER_MEM=... QUERY_MEM=... taskset -c 0-15 ../remote-write-arena/server.sh start R 19001
# and run this under the sender cores, e.g. `taskset -c 16-31 ./scale.sh > scale-<date>.txt`.
# Env: CH, DATA (default tools/clickhouse/data/remote-write-baseline-scale), NAME (R), TCP_PORT (19001),
#      SENDERS_LIST ("1 4 16"), MERGES_LIST ("run stop"), ROUNDS (3), SERIES (2000), SPS (1), UNIVERSE (20000, per sender),
#      REQUESTS (per sender, 20), LABELS (10)
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
export CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/remote-write-baseline-scale}
NAME=${NAME:-R}
export TCP_PORT=${TCP_PORT:-19001} RW_PORT=$(( ${TCP_PORT:-19001} + 2000 )) PIDFILE=$DATA/$NAME/server.pid
read -r -a SL <<< "${SENDERS_LIST:-1 4 16}"; read -r -a ML <<< "${MERGES_LIST:-run stop}"
ROUNDS=${ROUNDS:-3} SERIES=${SERIES:-2000} SPS=${SPS:-1} UNIVERSE=${UNIVERSE:-20000} REQUESTS=${REQUESTS:-20} LABELS=${LABELS:-10}
C="$CH client --port $TCP_PORT"
cells=(); for s in "${SL[@]}"; do for m in "${ML[@]}"; do cells+=("$s:$m"); done; done
n=${#cells[@]}
echo "# $($C -q "SELECT version(), getSetting('max_threads')" </dev/null) nproc=$(nproc) cells=${cells[*]} rounds=$ROUNDS series=$SERIES sps=$SPS universe=$UNIVERSE requests=$REQUESTS labels=$LABELS"
for r in $(seq 0 $((ROUNDS - 1))); do
  for k in $(seq 0 $((n - 1))); do
    cell=${cells[$(( (r + k) % n ))]}; s=${cell%%:*} m=${cell##*:}
    $C -q 'DROP TABLE IF EXISTS prometheus SYNC' </dev/null
    $C -q 'CREATE TABLE prometheus ENGINE = TimeSeries' </dev/null
    if [ "$m" = stop ]; then
      for t in $($C -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'" </dev/null); do
        $C -q "SYSTEM STOP MERGES \`$t\`" </dev/null
      done
    fi
    # timestamps 2 h back from now, so the recent samples table keeps them; the warm pass starts 10 min later
    # (rw.py moves a series 15 s forward each time it comes round again).
    ts0=$(( ($(date +%s) - 7200) * 1000 ))
    pre="round=$r merges=$m"
    echo "$pre $(SETTLE=1 "$DIR/run.sh" cold write "$s" "$SERIES" "$SPS" "$UNIVERSE" "$REQUESTS" --labels "$LABELS" --ts0 $ts0)"
    echo "$pre $(SETTLE=1 "$DIR/run.sh" warm write "$s" "$SERIES" "$SPS" "$UNIVERSE" "$REQUESTS" --labels "$LABELS" --ts0 $((ts0 + 600000)))"
  done
done
