#!/usr/bin/env bash
# Stream count of the tags subquery (sub_<k>.sql from gen.py + derive.py) per setting, from EXPLAIN PIPELINE at max_threads=$MT.
# On a small host the free-memory cap (max_threads_min_free_memory_per_thread, 1 GiB) silently caps streams: compare cap_on with cap_off.
set -euo pipefail
CH=${CH:-/root/nerve-workspace/tools/clickhouse/master/clickhouse}; PORT=${PORT:-19000}; MT=${MT:-16}
AS="merge_tree_min_bytes_for_concurrent_read=4194304, merge_tree_min_rows_for_concurrent_read=0"   # what readImpl adds on v11 tables
for k in ${SHAPES:-0 1 2 3}; do
 for v in "cap_on|$AS" "cap_off|max_threads_min_free_memory_per_thread=0, $AS" \
          "cap_off+default_thresholds|max_threads_min_free_memory_per_thread=0" \
          "cap_off+read_in_order_off|max_threads_min_free_memory_per_thread=0, optimize_read_in_order=0, $AS"; do
  out=$("$CH" client --port "$PORT" --max_query_size 100000000 -q "EXPLAIN PIPELINE SELECT sum(sipHash64(x)) FROM ($(cat sub_$k.sql)) SETTINGS join_algorithm='full_sorting_merge', optimize_read_in_order=1, max_threads=$MT, ${v#*|}")
  echo "shape$k ${v%%|*}: $(echo "$out" | grep -oE 'MergeTreeSelect\(pool: [A-Za-z]+, algorithm: [A-Za-z]+\)( × [0-9]+)?|MergingSortedTransform [0-9]+ → [0-9]+|MergeJoinTransform [0-9]+ → [0-9]+' | sort | uniq -c | sed 's/^ *//' | paste -sd';')"
 done
done
