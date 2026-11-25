#!/usr/bin/env bash
# handler_cpu.sh TCP_PORT RW_PORT [REQUESTS]: server CPU per remote-write request, split into the handler's INSERT
# (the request thread: protobuf parsing, the block, and on master the target-table INSERTs that the async insert
# never uses) and the flush of the asynchronous insert queue, from system.query_log.
# Sends REQUESTS (default 70, the first 10 are warm-up) deterministic requests of ../remote-write-baseline/rw.py:
# 2,000 series per request (Prometheus' default max_samples_per_send) out of 20,000, one sample each.
# The table is default.prometheus (the handler of ../remote-write-arena/config.xml.in), created when missing.
# TS0 (ms, default rw.py's 1793000000000) is the first timestamp: pick one within the last 4 days, the same on A and B,
# so the recent samples table gets rows too (older samples are past its TTL and are not written to it).
# Run on A and B with the same server settings; the handler line is the one the branch changes.
set -euo pipefail
PORT=$1 RW=$2 N=${3:-70}
DIR=$(cd "$(dirname "$0")" && pwd)
C="${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse} client --port $PORT"
$C -q "CREATE TABLE IF NOT EXISTS default.prometheus ENGINE = TimeSeries" < /dev/null
t0=$($C -q "SELECT now64(6)" < /dev/null)
python3 "$DIR/../remote-write-baseline/rw.py" "http://127.0.0.1:$RW/write" --series 2000 --universe 20000 --requests "$N" --skip 10 --ts0 "${TS0:-1793000000000}" | tail -1
$C -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT if(query_kind = 'AsyncInsertFlush', 'flush', 'handler') AS part, count() AS queries,
        quantileExact(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_median,
        round(quantileExact(0.5)(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 2000, 3) AS cpu_us_per_sample,
        min(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_min, max(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us_max
    FROM (SELECT * FROM system.query_log
          WHERE type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush') AND has(tables, 'default.prometheus')
            AND event_time_microseconds > toDateTime64('$t0', 6) AND event_date >= yesterday()
          ORDER BY event_time_microseconds LIMIT 1000000 OFFSET 10)
    GROUP BY part ORDER BY part
    FORMAT PrettyCompactMonoBlock
" < /dev/null
