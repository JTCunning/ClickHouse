#!/usr/bin/env bash
# Prewarmed, but each variant starts from an empty statistics cache before its prewarm query: the cache then holds only
# what that variant's own query loads, as on two separate servers (James's A/B).
set -u
R="python3 -I /root/nerve-workspace/research/bench/tags-in-subquery-cold/run.py"
CH="/root/nerve-workspace/tools/clickhouse/master/clickhouse client --port 9301"
OUT=${OUT:-/tmp/sifjo/e2e-own.jsonl}; N=${N:-5}
for ((r = 0; r < N; r++)); do
  for q in t1 t2 t3 t4; do
    if (( r % 2 )); then order="B A"; else order="A B"; fi
    for v in $order; do
      s="use_query_condition_cache=0"; [ $v = B ] && s="$s,query_plan_optimize_join_order_limit=0"
      $CH -q "SYSTEM DROP STATISTICS CACHE"
      $R --label "$q/$v/own" --state meta --latency-ms 50 --reps 1 --query /tmp/sifjo/q/$q.sql --settings "$s" --out $OUT | tail -1 | cut -c1-200
    done
  done
done
