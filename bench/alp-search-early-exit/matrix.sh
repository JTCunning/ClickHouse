#!/usr/bin/env bash
# matrix.sh <harness> <out> <data dir>: every data set x block x type, median of 5 (master vs branch, same process).
H=$1; OUT=$2; D=${3:?data dir}
: > $OUT
for f in $D/fresh_*.f64 $D/merged_*.f64 $D/perf_*.f64 $D/bench_q.f64; do for g in 256 8192; do for t in f64 f32; do
  echo "## $(basename $f .f64) block=$g type=$t" >> $OUT; $H $f $g 5 $t >> $OUT 2>&1
done; done; done
for n in seq mon rnd rtn; do for t in f64 f32; do
  echo "## ins_$n block=8192 type=$t" >> $OUT; $H $D/ins_$n.f64 8192 5 $t >> $OUT 2>&1
done; done
echo DONE >> $OUT
