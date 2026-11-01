#!/usr/bin/env bash
# run.sh LABEL PATH SENDERS SERIES SPS UNIVERSE REQUESTS_PER_SENDER [extra rw.py args]
# Runs SENDERS parallel rw.py senders against 127.0.0.1:19093/PATH and prints one result line:
# wall time, samples/s, server CPU (utime+stime of the server process, includes merges), parts written, peak RSS.
set -euo pipefail
LABEL=$1 P=$2 S=$3 N=$4 SPS=$5 U=$6 R=$7; shift 7
DIR=$(cd "$(dirname "$0")" && pwd)
PID=$(pgrep -P $(cat /root/nerve-workspace/tools/clickhouse/data/remote-write-baseline/server.pid) -x clickhouse)
C="/root/nerve-workspace/tools/clickhouse/master/clickhouse client --port 19000"
ev() { $C -q "SELECT sumIf(value, event='MergeTreeDataWriterBlocks'), sumIf(value, event='InsertedRows'), sumIf(value, event='AsyncInsertQuery') FROM system.events"; }
cpu() { awk '{print $14+$15}' /proc/$PID/stat; }
read b0 r0 a0 < <(ev); c0=$(cpu); t0=$(date +%s.%N); peak=0
pids=()
for i in $(seq 0 $((S-1))); do
  # each sender gets its own slice of the series universe
  python3 $DIR/rw.py "http://127.0.0.1:19093/$P" --series $N --samples-per-series $SPS --universe $U --requests $R --id-offset $((i*U)) "$@" \
    > /tmp/rw_$i.log 2>&1 &
  pids+=($!)
done
while kill -0 "${pids[@]}" 2>/dev/null; do r=$(awk '/VmRSS/{print $2}' /proc/$PID/status); [ "$r" -gt "$peak" ] && peak=$r; sleep 0.2; done
rc=0; for p in "${pids[@]}"; do wait $p || rc=1; done
t1=$(date +%s.%N); c1=$(cpu); read b1 r1 a1 < <(ev)
[ $rc = 0 ] || { echo "FAILED"; cat /tmp/rw_*.log | tail -5; exit 1; }
tick=$(getconf CLK_TCK)
python3 - "$LABEL" $S $N $SPS $R $t0 $t1 $c0 $c1 $tick $b0 $b1 $r0 $r1 $a0 $a1 $peak <<'PY'
import sys, statistics, glob
l,S,N,SPS,R,t0,t1,c0,c1,tick,b0,b1,r0,r1,a0,a1,peak=sys.argv[1:]
S,N,SPS,R=map(int,(S,N,SPS,R)); wall=float(t1)-float(t0); cpu=(int(c1)-int(c0))/int(tick)
samples=S*N*SPS*R; reqs=S*R
lat=[]
for f in glob.glob('/tmp/rw_*.log'):
    lat += [float(x.split()[2][:-1]) for x in open(f) if x.startswith('req ')]
print(f"{l}: senders={S} series/req={N} samples/series={SPS} reqs={reqs} wall={wall:.2f}s "
      f"samples/s={samples/wall:,.0f} cpu={cpu:.2f}s cpu_us/sample={cpu*1e6/samples:.2f} cpu_ms/req={cpu*1e3/reqs:.2f} "
      f"parts={int(b1)-int(b0)} parts/req={(int(b1)-int(b0))/reqs:.2f} inner_rows={int(r1)-int(r0)} async_q={int(a1)-int(a0)} "
      f"p50_lat={statistics.median(lat)*1e3:.0f}ms peakRSS={int(peak)/1024:.0f}MiB")
PY
