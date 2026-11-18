#!/usr/bin/env bash
# #20 rerun: full prometheusQueryRange T1-T4, A = master, B~ = master + query_plan_optimize_join_order_limit = 0 (reaches only the selector's ids query here).
set -u
R="python3 -I /root/nerve-workspace/research/bench/tags-in-subquery-cold/run.py"
OUT=${OUT:-/tmp/sifjo/e2e.jsonl}; STATE=${STATE:-meta}; N=${N:-7}
for ((r = 0; r < N; r++)); do
  for q in t1 t2 t3 t4; do
    if (( r % 2 )); then order="B A"; else order="A B"; fi
    for v in $order; do
      s="use_query_condition_cache=0"; [ $v = B ] && s="$s,query_plan_optimize_join_order_limit=0"
      $R --label "$q/$v/$STATE" --state $STATE --latency-ms 50 --reps 1 --query /tmp/sifjo/q/$q.sql --settings "$s" --out $OUT | tail -1 | cut -c1-200
    done
  done
done
