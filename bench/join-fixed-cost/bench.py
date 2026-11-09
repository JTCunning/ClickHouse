#!/usr/bin/env python3
"""Handoff #14: fixed JOIN cost of a small-result ignoring()/group_left ratio query.

Runs every case interleaved (round-robin) against a local master server on 127.0.0.1:19000,
then reads system.query_log. Prints one JSON line per run.
Usage: python3 bench.py [rounds] [max_threads] > run.jsonl
"""
import json
import subprocess
import sys
import uuid

CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
PORT = "19000"
ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
THREADS = int(sys.argv[2]) if len(sys.argv) > 2 else 16

SEL_NUM = 'node_cpu_seconds_total{job="node-exporter", cluster="bench", mode=~"idle|iowait|steal"}'
SEL_DEN = 'node_cpu_seconds_total{job="node-exporter", cluster="bench", mode="idle"}'
NUM = f"(1 - sum without (mode) (rate({SEL_NUM}[5m])))"
DEN = f"count without (cpu, mode) ({SEL_DEN})"
RATIO = f"{NUM} / ignoring (cpu) group_left {DEN}"

FULL = (1704067800, 1706659185, 900)   # the capture's window: 30 days, 2880 steps
EMPTY = (1735689600, 1738281585, 900)  # same length in 2025: no samples, same plan

CASES = {
    "ratio": (RATIO, FULL, {}),
    "ratio_serial": (RATIO, FULL, {"hash_join_layout": "serial"}),
    "ratio_nospill": (RATIO, FULL, {"max_bytes_ratio_before_external_join": 0}),
    "ratio_serial_nospill": (RATIO, FULL, {"hash_join_layout": "serial", "max_bytes_ratio_before_external_join": 0}),
    "numerator": (NUM, FULL, {}),
    "denominator": (DEN, FULL, {}),
    "ratio_empty": (RATIO, EMPTY, {}),
    "ratio_empty_serial": (RATIO, EMPTY, {"hash_join_layout": "serial"}),
    "numerator_empty": (NUM, EMPTY, {}),
    "denominator_empty": (DEN, EMPTY, {}),
}

PE = [
    "OSCPUVirtualTimeMicroseconds", "UserTimeMicroseconds", "SystemTimeMicroseconds", "RealTimeMicroseconds",
    "QueryPlanOptimizeMicroseconds", "QueryPipelineBuildMicroseconds", "QueryAnalysisMicroseconds",
    "QueryPlanBuildMicroseconds", "JoinOptimizeMicroseconds", "FilteringMarksWithPrimaryKeyMicroseconds",
    "SelectedRows", "SelectedMarks", "OSCPUWaitMicroseconds", "JoinBuildTableRowCount",
]


def ch(query, *extra):
    return subprocess.run([CH, "client", "--port", PORT, *extra, "-q", query],
                          check=True, capture_output=True, text=True).stdout


def run(name, promql, rng, settings):
    # max_threads_min_free_memory_per_thread (default 1 GiB) caps this 4 GiB VM at 2 streams; lift it so the
    # pipeline has THREADS streams like the capture (execution is still capped at 4 threads by the soft limit).
    s = {"max_threads": THREADS, "max_threads_min_free_memory_per_thread": 0, **settings}
    sset = ", ".join(f"{k} = {v!r}" if isinstance(v, str) else f"{k} = {v}" for k, v in s.items())
    qid = f"jfc-{name}-{uuid.uuid4().hex[:8]}"
    q = (f"SELECT count(), sum(length(samples)) FROM prometheusQueryRange('bench', $${promql}$$, "
         f"{rng[0]}, {rng[1]}, {rng[2]}) SETTINGS {sset}")
    out = ch(q, "--query_id", qid).strip()
    return qid, out


def main():
    runs = []
    for r in range(ROUNDS + 1):  # round 0 = warm-up, discarded
        for name, (promql, rng, settings) in CASES.items():
            qid, out = run(name, promql, rng, settings)
            runs.append((r, name, qid, out))
    ch("SYSTEM FLUSH LOGS")
    for r, name, qid, out in runs:
        if r == 0:
            continue
        pe_sel = ", ".join(f"ProfileEvents['{e}']" for e in PE)
        row = ch(f"SELECT query_duration_ms, memory_usage, read_rows, peak_threads_usage, {pe_sel} "
                 f"FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish' FORMAT JSONCompactEachRow").strip()
        vals = json.loads(row)
        rec = {"round": r, "case": name, "threads": THREADS, "result": out,
               "ms": vals[0], "mem": vals[1], "read_rows": vals[2], "peak_threads": vals[3]}
        rec.update({e: v for e, v in zip(PE, vals[4:])})
        print(json.dumps(rec), flush=True)


if __name__ == "__main__":
    main()
