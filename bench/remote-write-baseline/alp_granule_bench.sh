#!/usr/bin/env bash
# CPU of writing 1M Float64 values as 500 parts of 2,000 rows (the shape of one remote-write request
# with 1 sample per series), per value codec and granule size. Needs a server on port 19000.
C="${CH:-clickhouse} client --port 19000"
Q="SET query_profiler_cpu_time_period_ns=0; SET max_threads=1; SET max_insert_threads=1; SET enable_alp_codec=1;"
for rep in 1 2 3 4 5; do for t in alp zstd alp8192 gorilla; do
case $t in alp) codec="ALP, ZSTD(3)"; g=256;; zstd) codec="ZSTD(3)"; g=256;; alp8192) codec="ALP, ZSTD(3)"; g=8192;; gorilla) codec="Gorilla, ZSTD(3)"; g=256;; esac
$C --multiquery -q "$Q DROP TABLE IF EXISTS b_$t SYNC; CREATE TABLE b_$t (id UInt64, v Float64 CODEC($codec)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = $g, index_granularity_bytes = 524288; SYSTEM STOP MERGES b_$t; INSERT INTO b_$t SELECT number, 100 + (number % 1000) * 0.25 FROM numbers(1000000) SETTINGS max_insert_block_size = 2000, min_insert_block_size_rows = 2000, min_insert_block_size_bytes = 0, max_block_size = 2000, log_comment = 'alpbench_$t'; DROP TABLE b_$t SYNC"
done; done
$C -q "SYSTEM FLUSH LOGS"
$C -q "SELECT log_comment, count() n, round(median(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000) cpu_ms_median, round(min(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000) min_ms, round(max(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'])/1000) max_ms FROM system.query_log WHERE log_comment LIKE 'alpbench_%' AND type='QueryFinish' GROUP BY 1 ORDER BY 1"
