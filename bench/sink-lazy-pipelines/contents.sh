#!/usr/bin/env bash
# contents.sh TCP_PORT [TABLE]: one line per inner table of a TimeSeries table (default default.prometheus):
# rows and an order-independent hash with FINAL. A and B must print the same lines after the same requests.
set -euo pipefail
PORT=$1 T=${2:-default.prometheus}
C="${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse} client --port $PORT"
for f in timeSeriesTags timeSeriesTimeRanges timeSeriesSamples timeSeriesMetricFamilies; do
  $C -q "SELECT '$f', count(), groupBitXor(cityHash64(*)) FROM $f($T) FINAL FORMAT TSV" < /dev/null
done
db=${T%%.*}; uuid=$($C -q "SELECT uuid FROM system.tables WHERE database = '$db' AND name = '${T#*.}'" < /dev/null)
$C -q "SELECT 'recent samples', count(), groupBitXor(cityHash64(*)) FROM \`$db\`.\`.inner_id.recentsamples.$uuid\` FINAL FORMAT TSV" < /dev/null
