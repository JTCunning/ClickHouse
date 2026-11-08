#!/bin/bash
# usage: load.sh <table> <ts_offset_ms_expr> <value_expr>   (s = series 0..299, i = sample index; 15 s scrape, 2 days)
CH=${CH:-clickhouse}
T=$1; TS=$2; V=$3
sed "s/NAME/$T/" ddl.sql.tmpl > /tmp/$T.sql
$CH local --path ./db --max_memory_usage=2500000000 --allow_experimental_time_series_table=1 --multiquery -q "
CREATE DATABASE IF NOT EXISTS lab ENGINE = Atomic;
$(cat /tmp/$T.sql)
INSERT INTO lab.$T (metric_name, tags, samples)
SELECT 'container_cpu_usage_seconds_total',
       map('job','cadvisor','image','img','namespace','ns'||toString(s % 20),'pod','pod-'||toString(s)),
       arraySort(x -> x.1, groupArray((toDateTime64('2024-01-01 00:00:00', 3, 'UTC') + toIntervalMillisecond(i * 15000 + ($TS)), toFloat64($V))))
FROM (SELECT number % 300 AS s, intDiv(number, 300) AS i FROM numbers(300 * 11520))
GROUP BY s
SETTINGS max_threads = 2;" || exit 1
INNER=$($CH local --path ./db -q "SELECT '.inner_id.samples.' || toString(uuid) FROM system.tables WHERE database='lab' AND name='$T'")
$CH local --path ./db -q "OPTIMIZE TABLE lab.\`$INNER\` FINAL" || exit 1
echo "$T $INNER"
