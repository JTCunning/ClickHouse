#!/usr/bin/env bash
# Builds the dataset of the tags-in-subquery-cold report on a server whose `s3` policy is a cache disk over an S3 endpoint.
# 64 inserts, one per 15 minutes of data (16 h), merges stopped: each inner table keeps one fresh part per insert.
# 21,560 active series: metric_0 has 2,560, metric_1..metric_19 have 1,000 each; 2% of the pods restart per insert (new series).
# The target selector matches 10 series of metric_0 at any time.
set -euo pipefail
CH=${CH:-"/root/nerve-workspace/tools/clickhouse/master/clickhouse client --port 9301"}
BATCHES=${BATCHES:-64}
T_END=${T_END:-$(( ( $(date +%s) / 900 ) * 900 ))}
T0=$(( T_END - BATCHES * 900 ))
echo "$T0 $T_END" > "${OUT:-.}/time_range.txt"
$CH -n -q "
DROP TABLE IF EXISTS ts SYNC;
CREATE TABLE ts ENGINE = TimeSeries
  SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS storage_policy = 's3'
  RECENT SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS storage_policy = 's3'
  TAGS INNER ENGINE = ReplacingMergeTree SETTINGS storage_policy = 's3'
  TIME RANGES INNER ENGINE = AggregatingMergeTree SETTINGS storage_policy = 's3';
"
for t in $($CH -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'"); do
  $CH -q "SYSTEM STOP MERGES \`$t\`"
done
for ((b = 0; b < BATCHES; b++)); do
  $CH --max_insert_threads=1 -q "
INSERT INTO ts (metric_name, tags, samples)
WITH
  if(number < 2560, 0, intDiv(number - 2560, 1000) + 1) AS metric_idx,
  if(number < 2560, number, (number - 2560) % 1000) AS i,
  intDiv($b + (i % 50), 50) AS gen
SELECT
  concat('metric_', toString(metric_idx)),
  map('label_a', concat('v', toString(i % 2)),
      'label_b', concat('v', toString(intDiv(i, 2) % 2)),
      'label_c', concat('v', toString(intDiv(i, 4) % 2)),
      'label_d', concat('v', toString(intDiv(i, 8) % 2)),
      'label_e', concat('v', toString(intDiv(i, 16) % 2)),
      'label_f', concat('v', toString(intDiv(i, 32) % 2)),
      'pod', concat(['server', 'worker', 'client', 'proxy'][intDiv(i, 64) % 4 + 1], '-', toString(i), '-g', toString(gen)),
      'job', concat('job', toString(metric_idx % 5)),
      'instance', concat('host-', toString(intDiv(i, 8)), ':9100'),
      'namespace', concat('ns', toString(i % 7))),
  arrayMap(k -> (toDateTime64($T0 + $b * 900 + k * 30, 3), toFloat64(number * 7 + $b * 30 + k)), range(30))
FROM numbers(21560)"
done
$CH -q "SELECT table, count() AS parts, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size FROM system.parts WHERE active AND database = currentDatabase() AND table LIKE '.inner_id.%' GROUP BY table ORDER BY table"
