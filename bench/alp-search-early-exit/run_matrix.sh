#!/usr/bin/env bash
# run_matrix.sh <data dir>: every data set x block size x float type, median of 5; writes results-harness.txt.
D=$1
for f in "$D"/*.f64; do for g in 256 8192; do for t in f64 f32; do
  echo "## $(basename "$f" .f64) block=$g type=$t"; ./harness "$f" $g 5 $t
done; done; done
