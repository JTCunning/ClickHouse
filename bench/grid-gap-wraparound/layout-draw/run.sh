#!/usr/bin/env bash
# A/A on one master binary: fill ts_bucketing (timeseries_to_grid_bucketing.xml) N times, freeze each layout with
# SYSTEM STOP MERGES, time the timeSeriesLastToGrid query 7 times at 16 streams; then pin to one part and repeat.
# Usage: run.sh <clickhouse binary> <scratch dir> <fills>
set -u
CH=$1; DIR=$2; FILLS=${3:-3}
Q="SELECT id, timeSeriesLastToGrid(0, 300000, 300, 300)(timestamp, value) FROM ts_bucketing GROUP BY id FORMAT Null SETTINGS max_threads = 16, max_threads_min_free_memory_per_thread = 0, allow_experimental_time_series_aggregate_functions = 1"
for f in $(seq 1 "$FILLS") pinned; do
  rm -rf "$DIR/db"
  if [ "$f" = pinned ]; then FILLQ="OPTIMIZE TABLE ts_bucketing FINAL SETTINGS optimize_throw_if_noop = 1;"; else FILLQ=""; fi
  {
    echo "CREATE TABLE ts_bucketing (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);"
    echo "INSERT INTO ts_bucketing SELECT number % 2500 AS id, toDateTime64(intDiv(number, 2500) * 15, 3) AS timestamp, (intDiv(number, 2500) % 1000)::Float64 AS value FROM numbers_mt(2500 * 20000) SETTINGS max_insert_threads = 8;"
    echo "$FILLQ"
    echo "SYSTEM STOP MERGES ts_bucketing;"
    echo "SELECT 'layout', count(), groupArray(rows) FROM (SELECT rows FROM system.parts WHERE active AND table = 'ts_bucketing' ORDER BY name);"
    for i in $(seq 1 8); do echo "$Q;"; done
  } > "$DIR/q.sql"
  "$CH" local --path "$DIR/db" --max_memory_usage=2500000000 --time --queries-file "$DIR/q.sql" > "$DIR/out.txt" 2> "$DIR/time.txt"
  rc=$?
  layout=$(grep layout "$DIR/out.txt" | cut -f2-)
  # --time prints one elapsed line per query; the last 8 are the measured query, drop the first (warm-up)
  times=$(tail -n 8 "$DIR/time.txt" | tail -n 7 | sort -n | tr '\n' ' ')
  med=$(tail -n 8 "$DIR/time.txt" | tail -n 7 | sort -n | sed -n 4p)
  echo "fill=$f rc=$rc parts/rows=[$layout] median=$med sorted=[$times]"
done
rm -rf "$DIR/db"
