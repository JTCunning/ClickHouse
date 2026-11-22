#!/usr/bin/env bash
# The unmerged state: for each layout L, `CREATE TABLE u_L AS L` holding the same samples as L, where everything before
# the hour W0 = the start of the hour of T_END - 1 h is merged (one part per partition) and the samples from W0 to T_END
# arrive as one part per 2 minutes (remote-write inserts), with merges stopped on that table.
# For m this is done to both its samples table and its recent samples table (time u_m and u_m:0).
# Usage: tail.sh <layout ...>   Env: CH, PORT=19310, DATA
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19310}; DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/recent-samples-merge}
q() { "$CH" client --port "$PORT" --database rsm --max_memory_usage=2000000000 --max_threads=2 "$@" < /dev/null; }
TE=$(cat "$DATA/t_end"); W0=$(( (TE - 3600) / 3600 * 3600 ))
inner() { local u; u=$(q -q "SELECT uuid FROM system.tables WHERE database = 'rsm' AND name = '$1'"); echo "\`.inner_id.$2.$u\`"; }
for L in "$@"; do
  q -q "DROP TABLE IF EXISTS u_$L SYNC"; q -q "CREATE TABLE u_$L AS $L"
  for k in tags timeranges; do q -q "INSERT INTO $(inner "u_$L" $k) SELECT * FROM $(inner "$L" $k)"; done
  kinds=samples; [[ $L == m ]] && kinds="samples recentsamples"
  for kind in $kinds; do
    src=$(inner "$L" $kind); dst=$(inner "u_$L" $kind)
    q -q "INSERT INTO $dst SELECT * FROM $src WHERE max_time < toDateTime64($W0, 3, 'UTC')"
    q -q "OPTIMIZE TABLE $dst FINAL"
    q -q "SYSTEM STOP MERGES $dst"
    cols=$(q -q "SELECT arrayStringConcat(groupArray(name), ', ') FROM system.columns WHERE database = 'rsm' AND table = '${dst//\`/}' AND name NOT IN ('samples', 'min_time', 'max_time')")
    for ((w = W0; w < TE; w += 120)); do
      q -q "INSERT INTO $dst ($cols, samples, min_time, max_time)
            SELECT $cols, s, s[1].1, s[-1].1 FROM (
              SELECT $cols, arrayFilter(x -> x.1 >= toDateTime64($w, 3, 'UTC') AND x.1 < toDateTime64($((w + 120)), 3, 'UTC'), samples) AS s
              FROM $src WHERE max_time >= toDateTime64($W0, 3, 'UTC'))
            WHERE notEmpty(s)"
    done
    q -q "SELECT 'u_$L' AS layout, '$kind' AS kind, count() AS parts, sum(rows) AS rows, uniqExact(partition) AS partitions
          FROM system.parts WHERE database = 'rsm' AND active AND table = '${dst//\`/}' FORMAT TSV"

  done
done
