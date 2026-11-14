#!/usr/bin/env bash
# #20: the ids query of four selectors x three cache states x variants, on the cold S3 lab (bench/tags-in-subquery-cold:
# server on 8323/9301 with merges stopped since gen.sh, s3mock.py on 9100). Writes $OUT (JSON lines from run.py).
set -u
H=/root/nerve-workspace/research/bench
R="python3 -I $H/tags-in-subquery-cold/run.py"
Q=$H/idlookup-adaptive-index/q
OUT=${OUT:-/root/nerve-workspace/tools/clickhouse/data/tags-in-subquery-cold/results20.jsonl}
L=${L:-50}; N=${N:-5}
# StorageTimeSeriesSelector::applySettingsForSelectIDsQuery and readImpl, plus the lab's 16 streams (run.py adds max_threads)
BASE="join_algorithm=full_sorting_merge,optimize_read_in_order=1,any_join_distinct_right_table_keys=0,merge_tree_min_bytes_for_concurrent_read=4194304,merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem=4194304,use_query_condition_cache=0"
declare -A V=(
  [base]=""
  [noidx]=",use_skip_indexes=0"
  [nosp]=",use_statistics_for_part_pruning=0"
  [nojo]=",query_plan_optimize_join_order_limit=0"
  [nostat]=",use_statistics=0"
  [combo]=",use_skip_indexes=0,use_statistics_for_part_pruning=0,query_plan_optimize_join_order_limit=0"
)
for state in ${STATES:-cold meta warm}; do
  for sel in ${SELS:-nc nr w lf}; do
    for v in ${VARS:-base noidx nosp nojo nostat combo}; do
      [ "$sel" = lf ] && [ "$v" = noidx ] && continue   # no tags matchers: no text index condition
      $R --label "$sel/$v/$state" --state $state --latency-ms $L --reps $N --query $Q/$sel.sql --settings "$BASE${V[$v]}" --out $OUT | tail -1
      # planning: EXPLAIN indexes = 1 runs the same analysis (primary key, text index, statistics, join planning) without the read
      [ "${PLAN:-1}" = 1 ] && $R --label "$sel/$v/$state/plan" --state $state --latency-ms $L --reps ${NP:-3} --query $Q/$sel.explain.sql --settings "$BASE${V[$v]}" --out $OUT | tail -1
    done
  done
done
echo MATRIX20-DONE
