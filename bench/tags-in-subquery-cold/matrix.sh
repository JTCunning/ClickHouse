#!/usr/bin/env bash
# The experiment matrix of the report. Needs the server (port 8323) and s3mock.py (port 9100) running, and the data of gen.sh.
set -u
R="python3 /root/nerve-workspace/research/bench/tags-in-subquery-cold/run.py"
L=${L:-50}; N=${N:-5}
$R --label cold-L0 --state cold --latency-ms 0 --reps $N
$R --label cold --state cold --latency-ms $L --reps $N
$R --label meta --state meta --latency-ms $L --reps $N
$R --label warm --state warm --latency-ms $L --reps $N
$R --label meta+keep-textidx --state meta --keep 'tags: text index' --latency-ms $L --reps $N
$R --label meta+keep-tagsdata --state meta --keep 'tags: column data' --latency-ms $L --reps $N
$R --label meta+keep-trdata --state meta --keep 'timeranges: column data' --latency-ms $L --reps $N
$R --label meta+keep-samplesdata --state meta --keep 'recentsamples: column data' --latency-ms $L --reps $N
$R --label meta+keep-idside --state meta --keep 'tags: text index,tags: column data,timeranges: column data' --latency-ms $L --reps $N
$R --label meta+noskip --state meta --settings use_skip_indexes=0 --latency-ms $L --reps $N
$R --label cold+noskip --state cold --settings use_skip_indexes=0 --latency-ms $L --reps $N
$R --label meta+align1M --state meta --settings filesystem_cache_boundary_alignment=1048576 --latency-ms $L --reps $N
$R --label meta+align64K --state meta --settings filesystem_cache_boundary_alignment=65536 --latency-ms $L --reps $N
echo MATRIX-DONE
