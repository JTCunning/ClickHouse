#!/usr/bin/env bash
# A/B of the samples-table merge: on each server in turn (arm order rotates per round), gen.sh writes the same ROUNDS
# unmerged parts into a fresh default TimeSeries table, then OPTIMIZE FINAL merges the samples table. Prints one JSON line
# per merge: server, wall ms of the OPTIMIZE query, duration and CPU of the merge from system.part_log (the server needs
# part_log, as in ab/config.xml.in), rows, samples and parts after the merge, and a hash of the merged rows (must be equal
# on all servers).
# Usage: PORTS="9001 9002" N=10 SERIES=80000 ab/merge_ab.sh > ab/merge-<date>.jsonl
# Env: CH (client binary), PORTS, N (rounds per server), SERIES/ROUNDS/PER_ROUND (passed to gen.sh), T0 (fixed once here).
set -euo pipefail
DIR=$(cd "$(dirname "$0")/.." && pwd)
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; read -r -a P <<< "${PORTS:-9001 9002}"; N=${N:-10}
export CH SERIES=${SERIES:-80000} ROUNDS=${ROUNDS:-8} PER_ROUND=${PER_ROUND:-30}
export T0=${T0:-$("$CH" client --port "${P[0]}" -q "SELECT toUnixTimestamp(toStartOfHour(now())) - 3600" </dev/null)}
q() { local port=$1; shift; "$CH" client --port "$port" "$@" </dev/null; }
for ((r = 0; r < N; r++)); do
  for ((k = 0; k < ${#P[@]}; k++)); do
    port=${P[$(( (r + k) % ${#P[@]} ))]}
    (cd "$DIR" && ./gen.sh "$port" >/dev/null)
    t=$(q "$port" -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
    qid=merge-ab-$port-$r-$(date +%s%N)
    q "$port" -q "SYSTEM START MERGES \`$t\`"
    q "$port" --query_id "$qid" -q "OPTIMIZE TABLE \`$t\` FINAL"
    q "$port" -q "SYSTEM FLUSH LOGS"
    q "$port" -q "SELECT $port AS port, $r AS round, q.query_duration_ms AS optimize_wall_ms,
        m.duration_ms AS merge_ms, round(m.ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000) AS merge_cpu_ms,
        round(m.ProfileEvents['UserTimeMicroseconds'] / 1000) AS merge_user_ms,
        (SELECT count() FROM \`$t\`) AS rows, (SELECT sum(length(samples)) FROM \`$t\`) AS samples,
        (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$t' AND active) AS parts,
        (SELECT toString(sum(cityHash64(id, bucket, samples))) FROM \`$t\`) AS hash
      FROM (SELECT query_duration_ms, event_time_microseconds FROM system.query_log WHERE query_id = '$qid' AND type = 'QueryFinish') AS q,
           (SELECT duration_ms, ProfileEvents FROM system.part_log
            WHERE event_type = 'MergeParts' AND table = '$t' ORDER BY event_time_microseconds DESC LIMIT 1) AS m
      FORMAT JSONEachRow"
  done
done
