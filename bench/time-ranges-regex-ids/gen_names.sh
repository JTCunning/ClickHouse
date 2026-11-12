#!/usr/bin/env bash
# Table ts2 for names_scaling.py: 20,000 metric names x 10 series, one sample each in the window; 20 names contain "cpu".
# Usage: gen_names.sh <clickhouse binary> <tcp port>
set -euo pipefail
CH=${1:?clickhouse binary}; PORT=${2:-19000}
q() { "$CH" client --port "$PORT" --max_memory_usage=2500000000 -q "$1"; }
q "DROP TABLE IF EXISTS ts2 SYNC"
q "CREATE TABLE ts2 ENGINE = TimeSeries"
q "INSERT INTO ts2 (metric_name, tags, samples)
   SELECT concat('m', toString(intDiv(number, 10)), if(intDiv(number, 10) % 1000 = 7, '_cpu', ''), '_total') AS metric_name,
          map('job', 'j', 'instance', concat('host-', toString(number % 10))) AS tags,
          [(toDateTime64(1700000000 + (number % 3600), 3), 1.)] AS samples
   FROM numbers(200000)"
q "OPTIMIZE TABLE ts2 FINAL"
