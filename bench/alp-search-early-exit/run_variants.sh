#!/usr/bin/env bash
# run_variants.sh <file> <block>: ALP encode ms (median of 5), master vs each branch variant, clang 20 build.
f=$1; g=$2
for b in ${VARIANTS:?set VARIANTS="<harness>:<name> ..."}; do
  bin=${b%%:*}; name=${b#*:}
  out=$($bin $f $g 5 ${TYPE:-f64})
  m=$(echo "$out" | awk '/^master/{for(i=1;i<=NF;i++) if($i ~ /^alp_ms_median=/){split($i,a,"=");print a[2]}}')
  v=$(echo "$out" | awk '/^branch/{for(i=1;i<=NF;i++) if($i ~ /^alp_ms_median=/){split($i,a,"=");print a[2]}}')
  mb=$(echo "$out" | awk '/^master/{print $4}'); vb=$(echo "$out" | awk '/^branch/{print $4}')
  same=$([ "$mb" = "$vb" ] && echo bytes-same || echo BYTES-DIFFER)
  echo "$(basename $f .f64) block=$g $name master=$m variant=$v $(python3 -c "print(f'{($v/$m-1)*100:+.1f}%')") $same"
done
