#!/usr/bin/env bash
# gen_insert.sh <clickhouse binary> <out dir>: the value streams of codecs_float_insert.xml (20M rows each) and the two
# extra data sets of 05480 (NaN at every sample start; one 131,072-value block whose samples share their best pair).
CH=$1; D=$2
for q in "seq:number/pi()" "mon:number+sin(number)" "rnd:(intHash64(number) - 4294967295)/pi()" "rtn:round(number/20+sin(number), 3)"; do
  "$CH" local -q "SELECT toFloat64(${q#*:}) FROM numbers(20000000) FORMAT RowBinary" > "$D/ins_${q%%:*}.f64"
done
"$CH" local -q "SELECT if(number % 32 = 0, nan, (100000 + intHash32(number) % 4096) / 1000) FROM numbers(25600) FORMAT RowBinary" > "$D/t_nan.f64"
"$CH" local -q "SELECT if(intDiv(number, 1024) % 16 = 10, toFloat64(number % 1000), (100000 + intHash32(number) % 4096) / 1000) FROM numbers(131072) FORMAT RowBinary" > "$D/t_seed.f64"
