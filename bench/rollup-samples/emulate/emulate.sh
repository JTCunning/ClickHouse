#!/usr/bin/env bash
# Emulates the rollup columns of a TimeSeries samples table on a master binary (no build of the branch),
# and compares PromQL grid functions over the raw samples and over the rollups.
#
# The rollups are computed in SQL from the raw row of each (series, hour) bucket, in two conventions:
#   r<step>: last sample of every interval (k*step, (k+1)*step]   (closed on the right: lilred-perf-rollup-samples)
#   f<step>: last sample of every interval [k*step, (k+1)*step)   (closed on the left: jtcunning/timeseries-rollup-samples @ daa5327)
# The grid functions are the ones the PromQL converter calls: an instant selector is timeSeriesLastToGrid with the
# 5m lookback, rate(x[w]) is timeSeriesRateToGrid with window w, and so on.
#
# Usage: emulate.sh <clickhouse binary> <work dir> [series] [days]
set -euo pipefail

CH=${1:?clickhouse binary}
WORK=${2:?work dir}
SERIES=${3:-1000}
DAYS=${4:-2}

mkdir -p "$WORK"
T0=1767225600                     # 2026-01-01 00:00:00 UTC, a multiple of 1h
SAMPLES=$((DAYS * 86400 / 15))     # one sample every 15 s

q() { "$CH" local --path "$WORK/db" --max_memory_usage=2500000000 --enable_time_series_aggregate_functions=1 \
        --session_timezone=UTC -q "$1"; }

# key of the interval of a timestamp (ms) for a step (s): right-closed and left-closed
RK="intDiv(toUnixTimestamp64Milli(x.1) - 1, {S} * 1000)"
LK="intDiv(toUnixTimestamp64Milli(x.1), {S} * 1000)"
rollup() { # $1 = key expression with {S}, $2 = step
    local k=${1//\{S\}/$2}
    local kn=${k//x.1/samples[i + 1].1}
    echo "arrayFilter((x, i) -> (i = length(samples)) OR ($kn != $k), samples, arrayEnumerate(samples))"
}

if [ ! -f "$WORK/.loaded" ]; then
    q "DROP TABLE IF EXISTS rows"
    q "CREATE TABLE rows (shape LowCardinality(String), id UInt32, bucket DateTime64(3),
         samples Array(Tuple(DateTime64(3), Float64)),
         r60 Array(Tuple(DateTime64(3), Float64)), r300 Array(Tuple(DateTime64(3), Float64)), r3600 Array(Tuple(DateTime64(3), Float64)),
         f60 Array(Tuple(DateTime64(3), Float64)), f300 Array(Tuple(DateTime64(3), Float64)), f3600 Array(Tuple(DateTime64(3), Float64)))
       ENGINE = MergeTree ORDER BY (shape, id, bucket)"
    # Two timestamp shapes x two value shapes:
    #   aligned:  every sample at an exact multiple of 15 s (agents with aligned scrapes, synthetic loads)
    #   jittered: a per-series offset in [0, 15 s) plus up to 40 ms of jitter (Prometheus spreads targets over the interval)
    #   counter:  monotonic with a varying rate and a reset every 7-31 hours; gauge: a sine with noise and rare spikes
    for ts in aligned jittered; do
      for vs in counter gauge; do
        if [ $ts = aligned ]; then TSX="toDateTime64($T0 + j * 15, 3)"; else
            TSX="toDateTime64($T0 + j * 15 + (sipHash64(id, 'off') % 15000 + sipHash64(id, j, 'jit') % 40) / 1000, 3)"; fi
        if [ $vs = counter ]; then
            VX="(1 + sipHash64(id, 'r') % 100) * ((j % P) + 45 * sin((j % P) / 50))"
        else
            VX="100 + 50 * sin(j / 40 + id) + (sipHash64(id, j, 'n') % 20) + if(sipHash64(id, j, 's') % 500 = 0, 300, 0)"; fi
        q "INSERT INTO rows (shape, id, bucket, samples)
           SELECT '${ts}_${vs}', id, toDateTime64($T0 + h * 3600, 3),
                  arrayMap(j -> ($TSX, toFloat64($VX)), range(h * 240, h * 240 + 240))
           FROM (SELECT toUInt32(number) AS id, (7 + sipHash64(number, 'p') % 24) * 240 AS P FROM numbers($SERIES))
           ARRAY JOIN range($((SAMPLES / 240))) AS h
           SETTINGS max_block_size = 8192"
      done
    done
    q "ALTER TABLE rows UPDATE
         r60 = $(rollup "$RK" 60), r300 = $(rollup "$RK" 300), r3600 = $(rollup "$RK" 3600),
         f60 = $(rollup "$LK" 60), f300 = $(rollup "$LK" 300), f3600 = $(rollup "$LK" 3600)
       WHERE 1 SETTINGS mutations_sync = 2"
    q "OPTIMIZE TABLE rows FINAL"
    touch "$WORK/.loaded"
fi

q "SELECT shape, count() AS rows, sum(length(samples)) AS raw, sum(length(r60)) AS r60, sum(length(r300)) AS r300, sum(length(r3600)) AS r3600
   FROM rows GROUP BY shape ORDER BY shape FORMAT PrettyCompactMonoBlock"

S0=$((T0 + 3600)); E0=$((T0 + DAYS * 86400 - 1))
# compare <label> <function with params, {S} = step> <step> <rollup column>
compare() {
    local label=$1 fn=$2 step=$3 col=$4
    local start=$(( (S0 + step - 1) / step * step ))
    local f=${fn//\{S\}/$step}
    f=${f//\{A\}/$start}
    q "WITH raw AS (SELECT shape, id, $f(samples) AS g FROM rows GROUP BY shape, id),
            rl AS (SELECT shape, id, $f($col) AS g FROM rows GROUP BY shape, id)
       SELECT '$label' AS case, '$col' AS col, shape,
              count() AS points,
              countIf(isNotNull(a) AND isNull(b)) AS lost,
              countIf(isNull(a) AND isNotNull(b)) AS extra,
              countIf(isNotNull(a) AND isNotNull(b) AND abs(b - a) > 1e-9 * greatest(abs(a), 1)) AS differ,
              round(quantileExactIf(0.5)(abs(b - a) / greatest(abs(a), 1e-12), isNotNull(a) AND isNotNull(b)), 4) AS rel_p50,
              round(quantileExactIf(0.99)(abs(b - a) / greatest(abs(a), 1e-12), isNotNull(a) AND isNotNull(b)), 4) AS rel_p99,
              round(maxIf(abs(b - a) / greatest(abs(a), 1e-12), isNotNull(a) AND isNotNull(b)), 4) AS rel_max
       FROM raw JOIN rl USING (shape, id) ARRAY JOIN raw.g AS a, rl.g AS b
       GROUP BY shape ORDER BY shape FORMAT TSV"
}

echo -e "case\tcol\tshape\tpoints\tlost\textra\tdiffer\trel_p50\trel_p99\trel_max"
for st in 60 300 3600; do
    compare "selector step=$st" "timeSeriesLastToGrid({A}, $E0, {S}, 300)" $st r$st
    compare "selector step=$st" "timeSeriesLastToGrid({A}, $E0, {S}, 300)" $st f$st
done
compare "rate(x[5m]) step=300"       "timeSeriesRateToGrid({A}, $E0, {S}, 300)"     300  r300
compare "rate(x[5m]) step=300"       "timeSeriesRateToGrid({A}, $E0, {S}, 300)"     300  r60
compare "rate(x[1h5m]) step=3600"    "timeSeriesRateToGrid({A}, $E0, {S}, 3900)"    3600 r3600
compare "increase(x[1h]) step=3600"  "timeSeriesIncreaseToGrid({A}, $E0, {S}, 3600)" 3600 r3600
compare "increase(x[1h]) step=3600"  "timeSeriesIncreaseToGrid({A}, $E0, {S}, 3600)" 3600 r300
compare "max_over_time(x[1h]) step=3600" "timeSeriesMaxToGrid({A}, $E0, {S}, 3600)"  3600 r300
compare "avg_over_time(x[1h]) step=3600" "timeSeriesAvgToGrid({A}, $E0, {S}, 3600)"  3600 r300
