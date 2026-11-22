#!/usr/bin/env bash
# Candidate L4, "labelled tiers": ONE samples table (recent samples table off) whose partition key is a column `tier`:
#   new rows get tier = the start of their 5 h interval (DEFAULT expression, so the TimeSeries sink needs no change);
#   a mover (here: this script; in the engine: a background task) rewrites every 5 h partition entirely older than
#   YOUNG seconds into the month partition (tier = toYYYYMM) and drops it: INSERT ... SELECT, then DROP PARTITION.
# Each sample is stored once; young data is time-clustered (5 h partitions), old data id-clustered (month partitions);
# the selector reads the one table as today (1 h buckets).
# Usage: lt.sh <name> <YOUNG seconds> [hash]   builds `<name>` from the samples rows of layout m.
#   With `hash`, prints HASH=1 time.sh lines for a few shapes between the INSERT and the DROP (every moved row present twice).
# Env: CH, PORT=19310, DATA
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}
PORT=${PORT:-19310}; DATA=${DATA:-/root/nerve-workspace/tools/clickhouse/data/recent-samples-merge}
DIR=$(cd "$(dirname "$0")" && pwd)
NAME=${1:?name}; YOUNG=${2:?young seconds}; HASHMID=${3:-}
q() { "$CH" client --port "$PORT" --database rsm --max_memory_usage=2000000000 --max_threads=2 "$@" < /dev/null; }
inner() { local u; u=$(q -q "SELECT uuid FROM system.tables WHERE database = 'rsm' AND name = '$1'"); echo "\`.inner_id.$2.$u\`"; }
TE=$(cat "$DATA/t_end"); SPLIT=$(( (TE - YOUNG) / 18000 * 18000 ))
q -q "DROP TABLE IF EXISTS $NAME SYNC"
q -q "CREATE TABLE $NAME ENGINE = TimeSeries SETTINGS version = 11, samples_bucket_step_seconds = 3600, recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (
  id Tuple(UInt64, LowCardinality(UUID)),
  samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
  bucket DateTime64(3) CODEC(Delta, T64, ZSTD(3)),
  min_time SimpleAggregateFunction(min, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
  max_time SimpleAggregateFunction(max, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
  tier UInt32 DEFAULT toUInt32(toStartOfInterval(toDateTime(bucket, 'UTC'), toIntervalHour(5))),
  CODEC samples.timestamp (Delta, T64, ZSTD(3)), CODEC samples.value (ALP, ZSTD(4)))
SAMPLES INNER ENGINE = AggregatingMergeTree PARTITION BY tier ORDER BY (id, bucket)
  SETTINGS index_granularity = 256, index_granularity_bytes = 262144"
for k in tags timeranges; do q -q "INSERT INTO $(inner "$NAME" $k) SELECT * FROM $(inner m $k)"; done
T=$(inner "$NAME" samples)
# arrival: every row lands in its 5 h partition (the DEFAULT), as the sink would write it
q -q "INSERT INTO $T (id, bucket, samples, min_time, max_time) SELECT id, bucket, samples, min_time, max_time FROM $(inner m samples)
      SETTINGS max_partitions_per_insert_block = 1000"
q -q "OPTIMIZE TABLE $T FINAL"
# the mover: one INSERT ... SELECT for all 5 h partitions older than SPLIT, then DROP PARTITION for each of them
OLD=$(q -q "SELECT arrayStringConcat(groupUniqArray(partition_id), ' ') FROM system.parts WHERE database = 'rsm' AND active
            AND table = '${T//\`/}' AND toUInt32(partition) >= 1000000 AND toUInt32(partition) < $SPLIT")
t0=$SECONDS
q -q "INSERT INTO $T SELECT * REPLACE (toUInt32(toYYYYMM(toDateTime(bucket, 'UTC'))) AS tier) FROM $T
      WHERE _partition_id IN ($(sed -E "s/([^ ]+)/'\1'/g; s/ /, /g" <<< "$OLD")) SETTINGS max_partitions_per_insert_block = 1000"
# (filtering on `tier` here would read the REPLACE alias, not the column, and move nothing)
echo "move insert: $((SECONDS - t0)) s for $(wc -w <<< "$OLD") partitions, rows now $(q -q "SELECT count() FROM $T") (moved rows twice)"
if [[ $HASHMID == hash ]]; then
  echo "== mid-move (moved rows present twice):"; HASH=1 "$DIR/time.sh" "$NAME-mid" i_rate,r6h_sum,r7d_n,r14d_sum "$NAME"
fi
for p in $OLD; do q -q "ALTER TABLE $T DROP PARTITION ID '$p'"; done
q -q "OPTIMIZE TABLE $T FINAL"
q -q "SELECT '$NAME' AS layout, $SPLIT AS split, count() AS parts, uniqExact(partition) AS partitions, sum(rows) AS rows,
             sum(bytes_on_disk) AS bytes, sum(primary_key_bytes_in_memory) AS pk_bytes
      FROM system.parts WHERE database = 'rsm' AND active AND table = '${T//\`/}' FORMAT TSV"
