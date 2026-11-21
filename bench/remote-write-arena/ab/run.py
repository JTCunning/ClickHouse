#!/usr/bin/env python3
"""A/B of remote-write CPU per sample on two (or more) servers started with ../server.sh (remote write on TCP + 2000).

Every server gets the same requests: a warm-up that fills the tags cache, then ROUNDS rounds; in each round every
server receives REQUESTS requests of SERIES series x 1 sample x LABELS labels with the round's timestamps, in an order
that rotates per round. The server's CPU is read from /proc/<pid>/stat (utime + stime, all threads) around each batch,
so it includes the async insert flushes and, with MERGES=run, background merges. Prints one JSON line per batch and,
at the end, the hash of each server's tags and samples tables (must be equal).

Env: CH (client binary), PORTS="9001 9002" (TCP), PIDS="<pid> <pid>" (default: $DATA/<name>/server.pid for NAMES),
     NAMES="A B", DATA, ROUNDS=10, REQUESTS=20, WARMUP=20, SERIES=2000, LABELS=10, UNIVERSE=20000, MERGES=stop|run,
     SUFFIX (appended to every label value but the metric name; '-0123456789abcdefghij' makes them longer than 22 bytes)
Usage: ab/run.py > ab/run-<date>.jsonl
"""
import json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
CH = os.environ.get('CH', '/root/nerve-workspace/tools/clickhouse/master/clickhouse')
PORTS = [int(p) for p in os.environ.get('PORTS', '9001 9002').split()]
NAMES = os.environ.get('NAMES', 'A B').split()
DATA = os.environ.get('DATA', '/root/nerve-workspace/tools/clickhouse/data/remote-write-arena')
PIDS = [int(p) for p in os.environ['PIDS'].split()] if os.environ.get('PIDS') else \
       [int(open(f'{DATA}/{n}/server.pid').read()) for n in NAMES[:len(PORTS)]]
ROUNDS, REQUESTS, WARMUP = (int(os.environ.get(k, d)) for k, d in (('ROUNDS', 10), ('REQUESTS', 20), ('WARMUP', 20)))
SERIES, LABELS, UNIVERSE = (int(os.environ.get(k, d)) for k, d in (('SERIES', 2000), ('LABELS', 10), ('UNIVERSE', 20000)))
MERGES = os.environ.get('MERGES', 'stop')
SUFFIX = os.environ.get('SUFFIX', '')  # appended to label values (rw.py --value-suffix)
RW = os.path.join(HERE, '..', '..', 'remote-write-baseline', 'rw.py')
TICK = os.sysconf('SC_CLK_TCK')


def q(port, sql):
    return subprocess.run([CH, 'client', '--port', str(port), '-q', sql], check=True, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL).stdout.strip()


def cpu_s(pid):
    f = open(f'/proc/{pid}/stat').read().rsplit(')', 1)[1].split()
    return (int(f[11]) + int(f[12])) / TICK  # utime, stime (fields 14 and 15)


def send(port, requests, ts0):
    # --value-suffix=<value> as one argument: argparse rejects a separate value that starts with '-' (2026-11-21 E1)
    subprocess.run([sys.executable, RW, f'http://127.0.0.1:{port + 2000}/write', '--series', str(SERIES),
                    '--samples-per-series', '1', '--labels', str(LABELS), '--universe', str(UNIVERSE),
                    '--requests', str(requests), '--ts0', str(ts0), f'--value-suffix={SUFFIX}'], check=True, capture_output=True)


for port in PORTS:
    q(port, 'CREATE TABLE IF NOT EXISTS prometheus ENGINE = TimeSeries')
    if MERGES == 'stop':
        for t in q(port, "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'").split('\n'):
            q(port, f'SYSTEM STOP MERGES `{t}`')
# Timestamps: within the last hours, so the recent samples table keeps them; every batch moves forward
# (rw.py moves a series 15 s forward each time it comes round again, every UNIVERSE / SERIES requests).
ts0 = (int(time.time()) - 2 * 3600) * 1000
shards = max(1, UNIVERSE // SERIES)
step = (REQUESTS // shards + 1) * 15000
for port in PORTS:
    send(port, WARMUP, ts0)
ts0 += (WARMUP // shards + 1) * 15000
for r in range(ROUNDS):
    order = [(PORTS[(r + k) % len(PORTS)], PIDS[(r + k) % len(PORTS)]) for k in range(len(PORTS))]
    for port, pid in order:
        c0, t0 = cpu_s(pid), time.monotonic()
        send(port, REQUESTS, ts0)
        time.sleep(1.5)  # let the async insert of the last request flush (async_insert_busy_timeout is 200 ms by default)
        c1, t1 = cpu_s(pid), time.monotonic()
        samples = REQUESTS * SERIES
        print(json.dumps(dict(port=port, round=r, cpu_s=round(c1 - c0, 3), wall_s=round(t1 - t0, 3),
                              cpu_us_per_sample=round(1e6 * (c1 - c0) / samples, 3), samples=samples)), flush=True)
    ts0 += step
time.sleep(3)
for port in PORTS:
    tags = q(port, "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%'")
    smp = q(port, "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.samples.%'")
    h = q(port, f"SELECT (SELECT toString(sum(cityHash64(id, metric_name, tags))) FROM (SELECT DISTINCT id, metric_name, tags FROM `{tags}`)), "
                f"(SELECT toString(sum(cityHash64(id, bucket, samples))) || ' ' || toString(sum(length(samples))) FROM (SELECT id, bucket, samples FROM `{smp}` FINAL))")
    print(json.dumps(dict(port=port, hash=h)), flush=True)
