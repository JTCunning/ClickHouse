#!/usr/bin/env bash
# Times the plain-selector grid (timeSeriesLastToGrid, 5m lookback) over the raw samples and over an emulated rollup column,
# and measures the compressed size of each column with the codecs of the TimeSeries samples table.
# Runs on a master binary; the rollup columns are computed in SQL (right-closed intervals, as in emulate.sh).
#
# Usage: time.sh <clickhouse binary> <work dir> [series] [days] [runs]
set -euo pipefail

CH=${1:?clickhouse binary}
WORK=${2:?work dir}
SERIES=${3:-1000}
DAYS=${4:-7}
RUNS=${5:-5}

mkdir -p "$WORK"
T0=1767225600
q() { "$CH" local --path "$WORK/db" --max_memory_usage=2500000000 --enable_time_series_aggregate_functions=1 \
        --session_timezone=UTC "$@"; }

rk() { echo "arrayFilter((x, i) -> (i = length(samples)) OR (intDiv(toUnixTimestamp64Milli(samples[i + 1].1) - 1, $1 * 1000) != intDiv(toUnixTimestamp64Milli(x.1) - 1, $1 * 1000)), samples, arrayEnumerate(samples))"; }

if [ ! -f "$WORK/.loaded" ]; then
    # Same layout as the inner samples table: one row per (series, 1h bucket), codecs of version 11/12 on the elements.
    q -q "CREATE TABLE s (id UInt32, bucket DateTime64(3),
            ts Array(DateTime64(3)) CODEC(Delta, T64, ZSTD(3)), v Array(Float64) CODEC(ALP, ZSTD(3)),
            ts60 Array(DateTime64(3)) CODEC(Delta, T64, ZSTD(3)), v60 Array(Float64) CODEC(ALP, ZSTD(3)),
            ts300 Array(DateTime64(3)) CODEC(Delta, T64, ZSTD(3)), v300 Array(Float64) CODEC(ALP, ZSTD(3)),
            ts3600 Array(DateTime64(3)) CODEC(Delta, T64, ZSTD(3)), v3600 Array(Float64) CODEC(ALP, ZSTD(3)))
          ENGINE = MergeTree ORDER BY (id, bucket) SETTINGS index_granularity = 256"
    q -q "INSERT INTO s
          SELECT id, bucket, samples.1, samples.2, r60.1, r60.2, r300.1, r300.2, r3600.1, r3600.2 FROM (
            SELECT id, bucket, samples, $(rk 60) AS r60, $(rk 300) AS r300, $(rk 3600) AS r3600 FROM (
              SELECT id, toDateTime64($T0 + h * 3600, 3) AS bucket,
                     arrayMap(j -> (toDateTime64($T0 + j * 15 + (sipHash64(id, 'off') % 15000 + sipHash64(id, j, 'jit') % 40) / 1000, 3),
                                    toFloat64(round(100 + 50 * sin(j / 40 + id) + (sipHash64(id, j, 'n') % 20), 1))),
                              range(h * 240, h * 240 + 240)) AS samples
              FROM (SELECT toUInt32(number) AS id FROM numbers($SERIES)) ARRAY JOIN range($((DAYS * 24))) AS h))
          SETTINGS max_block_size = 4096, max_insert_threads = 1"
    q -q "OPTIMIZE TABLE s FINAL"
    touch "$WORK/.loaded"
fi

echo "== compressed bytes per column (jittered gauge, $SERIES series x $DAYS days at 15 s)"
q -q "SELECT name, formatReadableSize(data_compressed_bytes) AS compressed, round(data_compressed_bytes / (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 's' AND name IN ('ts', 'v')), 4) AS of_raw
      FROM system.columns WHERE database = currentDatabase() AND table = 's' AND name NOT IN ('id', 'bucket') ORDER BY name FORMAT PrettyCompactMonoBlock"

E0=$((T0 + DAYS * 86400 - 1))
run() { # label, step, ts column, value column
    local label=$1 step=$2 tc=$3 vc=$4 start=$(( (T0 + 3600 + $2 - 1) / $2 * $2 ))
    local sql="SELECT id, timeSeriesLastToGrid($start, $E0, $step, 300)($tc, $vc) FROM s WHERE bucket >= toDateTime64($start - 3600, 3) GROUP BY id FORMAT Null SETTINGS max_threads = 2"
    local times=()
    q -q "$sql" > /dev/null   # warm-up
    for _ in $(seq "$RUNS"); do
        times+=("$( { /usr/bin/time -f '%e' "$CH" local --path "$WORK/db" --max_memory_usage=2500000000 --enable_time_series_aggregate_functions=1 --session_timezone=UTC -q "$sql" > /dev/null; } 2>&1 )")
    done
    printf '%s\t%s\n' "$label" "$(printf '%s\n' "${times[@]}" | sort -n | tr '\n' ' ')"
}

echo "== selector grid, wall seconds of $RUNS runs (sorted), clickhouse local incl. ~startup"
run "step 1h raw"     3600 ts v
run "step 1h r3600"   3600 ts3600 v3600
run "step 5m raw"     300  ts v
run "step 5m r300"    300  ts300 v300
run "step 1m raw"     60   ts v
run "step 1m r60"     60   ts60 v60
startup=()
for _ in $(seq "$RUNS"); do
    startup+=("$( { /usr/bin/time -f '%e' "$CH" local --path "$WORK/db" -q "SELECT 1 FORMAT Null" > /dev/null; } 2>&1 )")
done
printf '%s\t%s\n' "startup only (SELECT 1)" "$(printf '%s\n' "${startup[@]}" | sort -n | tr '\n' ' ')"
