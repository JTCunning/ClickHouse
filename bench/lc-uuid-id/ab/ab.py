#!/usr/bin/env python3
"""Two-server A/B of the TimeSeries id type (plan: research/ab-plans/lowcardinality-uuid-id.md).

Every server runs the same binary. A holds tables with the v11 default id Tuple(UInt64, LowCardinality(UUID)),
B the same tables with TAGS INNER COLUMNS (id Tuple(UInt64, UUID)); an optional C is a default-id server started
with PROFILE_EXTRA (server.sh). Queries name the same table on every server, so `clickhouse benchmark` with one
--host/--port pair per server interleaves them and prints the t-test (--confidence 5 = 99.5%).

  ab.py fill-own SERIES WIDE SAMPLES   own dataset `ts` (the report's generator, WIDE series in metric_0)
  ab.py check [TABLE]                  rows, parts and id-column bytes per inner table; exits 1 if servers differ
  ab.py own [ITER]                     own cases (bench.py OWN) on `ts`, results compared, then benchmark
  ab.py xml NAME [ITER]                one in-tree perf test: its CREATE (+ the plain id on B), its fill, its queries

Env: PORTS=9001,9002[,9003] VARIANTS=lc,plain[,lc] (C keeps the default id) CH=<binary>
     MAX_THREADS (added to every query; empty keeps each query's own) MERGED=1 (OPTIMIZE FINAL every inner table,
     the report's layout; 0 = SYSTEM STOP MERGES before the fill, the CI-like many-part layout) OUT=out/
"""
import os, re, subprocess, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bench  # noqa: E402  (OWN cases, PLAIN clause, with_settings, unescape)

CH = os.environ.get("CH", bench.CH)
PORTS = os.environ.get("PORTS", "9001,9002").split(",")
VARIANTS = os.environ.get("VARIANTS", "lc,plain").split(",")
THREADS = os.environ.get("MAX_THREADS", "16")
MERGED = os.environ.get("MERGED", "1") == "1"
OUT = os.environ.get("OUT", os.path.join(os.path.dirname(os.path.abspath(__file__)), "out"))
T_END_FILE = os.path.join(OUT, "t_end")  # written by fill-own: the dataset must end near now, or the recent-samples TTL empties that table
# Deterministic single-threaded fill: every server gets the same parts (checked by `check`).
FILL = os.environ.get("FILL_SETTINGS", "max_insert_threads = 1, max_threads = 1, max_block_size = 16384, max_insert_block_size = 16384, min_insert_block_size_rows = 16384")
assert len(PORTS) == len(VARIANTS)


def q(port, sql, fmt=None):
    r = subprocess.run([CH, "client", "--port", port, "--max_query_size", "100000000", "--queries-file", "/dev/stdin"],
                       input=sql + (" FORMAT " + fmt if fmt else ""), capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"port {port}: {sql[:300]}\n{r.stderr[:1500]}")
    return r.stdout


def every(sql_for_variant):
    for port, v in zip(PORTS, VARIANTS):
        q(port, sql_for_variant(v))


def inner_tables(port, table):
    uuid = q(port, f"SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '{table}'", "TSVRaw").strip()
    return q(port, f"SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%{uuid}' "
                   f"AND name NOT LIKE '%metrics%' ORDER BY name", "TSVRaw").split()


def create(table, create_sql, fills):
    every(lambda v: f"DROP TABLE IF EXISTS {table} SYNC")
    every(lambda v: create_sql + (bench.PLAIN if v == "plain" else ""))
    for port in PORTS:
        for t in inner_tables(port, table):
            if not MERGED:
                q(port, f"SYSTEM STOP MERGES `{t}`")
        for f in fills:
            q(port, bench.with_settings(f, FILL))
        if MERGED:
            for t in inner_tables(port, table):
                q(port, f"OPTIMIZE TABLE `{t}` FINAL")


def layout(port, table):
    rows = []
    for t in inner_tables(port, table):
        kind = t.split(".")[2] if t.count(".") >= 2 else t
        r = q(port, f"SELECT sum(rows), count() FROM system.parts WHERE active AND table = '{t}'", "TSVRaw").split()
        b = q(port, f"SELECT sum(column_data_compressed_bytes), sum(column_data_uncompressed_bytes) FROM system.parts_columns "
                    f"WHERE active AND table = '{t}' AND column = 'id'", "TSVRaw").split()
        rows.append((kind, int(r[0] or 0), int(r[1]), int(b[0] or 0), int(b[1] or 0)))
    return rows


def check(table):
    base = None
    for port, v in zip(PORTS, VARIANTS):
        lay = layout(port, table)
        for kind, nrows, parts, cb, ub in lay:
            print(f"{v}@{port} {kind:14} rows={nrows} parts={parts} id_compressed={cb / 2**20:.2f}MiB id_uncompressed={ub / 2**20:.2f}MiB")
        shape = [(k, n, p) for k, n, p, _, _ in lay]
        if base is None:
            base = shape
        elif shape != base:
            sys.exit(f"layout differs on {v}@{port}: the A/B is not on the same data")


def same(a, b, rel=1e-9):
    if a == b:
        return True
    fa, fb = re.split(r"[\t\n,\[\]()]+", a), re.split(r"[\t\n,\[\]()]+", b)
    if len(fa) != len(fb):
        return False
    for x, y in zip(fa, fb):
        if x != y:
            try:
                if abs(float(x) - float(y)) > rel * max(abs(float(x)), abs(float(y))):
                    return False
            except ValueError:
                return False
    return True


def settings():
    return "use_query_cache = 0" + (f", max_threads = {THREADS}, max_threads_min_free_memory_per_thread = 0" if THREADS else "")


def run_case(label, sql, iters):
    sql = " ".join(sql.split())
    outs = [q(p, bench.with_settings(sql, settings()).replace(" FORMAT Null", ""), "TSVRaw") for p in PORTS]
    ok = all(same(outs[0], o) for o in outs[1:])
    cmd = [CH, "benchmark", "-i", str(iters), "--confidence", "5", "--use_query_cache=0"]
    if THREADS:
        cmd += [f"--max_threads={THREADS}", "--max_threads_min_free_memory_per_thread=0"]  # a query's own SETTINGS win
    for p in PORTS:
        cmd += ["--host", "127.0.0.1", "--port", p]
    r = subprocess.run(cmd, input=sql + "\n", capture_output=True, text=True)
    os.makedirs(OUT, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")[:60]
    with open(os.path.join(OUT, f"{slug}.txt"), "w") as fh:
        fh.write(f"{label}\n{sql}\nPORTS={PORTS} VARIANTS={VARIANTS} MAX_THREADS={THREADS} MERGED={MERGED} same_result={ok}\n\n{r.stderr}{r.stdout}")
    lines = (r.stderr + r.stdout).splitlines()
    verdict = [l.strip() for l in lines if "confidence" in l]
    medians = [l.split(None, 1)[1].replace("\t", " ").strip() for l in lines if l.startswith("50%")]
    median = medians[-1] if medians else ""  # the final report; one column per server, in PORTS order
    print(f"{label}: same_result={ok}; median per server {median or '?'}; {verdict[-1] if verdict else 'benchmark failed: ' + r.stderr[-300:]}")


def main():
    cmd = sys.argv[1]
    if cmd == "fill-own":
        series, wide, samples = (int(x) for x in sys.argv[2:5])
        T_END = int(os.environ.get("T_END", int(time.time()) // 900 * 900))
        os.makedirs(OUT, exist_ok=True)
        open(T_END_FILE, "w").write(str(T_END))
        t0 = T_END - samples * 30
        fill = (f"INSERT INTO ts (metric_name, tags, samples) WITH cityHash64(number) AS h "
                f"SELECT concat('metric_', toString(if(number < {wide}, 0, (number % 49) + 1))), "
                f"map('job', concat('job', toString(h % 10)), 'instance', concat('host-', toString(intDiv(h, 10) % 500), ':9100'), "
                f"'cluster', concat('c', toString(intDiv(h, 5000) % 5)), 'namespace', concat('ns', toString(intDiv(h, 25000) % 20)), "
                f"'pod', concat('pod-', toString(number)), "
                f"'mode', ['user','system','idle','iowait','nice','irq','softirq','steal'][(intDiv(h, 500000) % 8) + 1]), "
                f"arrayMap(i -> (toDateTime64({t0} + i * 30, 3), toFloat64((number % 1000) + i * 1.5)), range({samples})) "
                f"FROM numbers({series})")
        t = time.time()
        create("ts", "CREATE TABLE ts ENGINE = TimeSeries", [fill])
        print(f"filled ts: {series} series ({wide} in metric_0) x {samples} samples, T_END={T_END}, {time.time() - t:.0f} s, merged={MERGED}")
        check("ts")
    elif cmd == "check":
        check(sys.argv[2] if len(sys.argv) > 2 else "ts")
    elif cmd == "own":
        iters = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        T_END = int(open(T_END_FILE).read())
        p = dict(T=T_END, T0=T_END - 14400 + 300, T1h=T_END - 3600)
        for case, sql in bench.OWN.items():
            run_case(f"own: {case}", sql.format(t="ts", **p), iters)
    elif cmd == "xml":
        name, iters = sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 30
        import xml.etree.ElementTree as ET
        root = ET.fromstring(subprocess.run(["git", "-C", bench.REPO, "show", f"origin/master:tests/performance/{name}.xml"],
                                            capture_output=True, text=True, check=True).stdout)
        create_sql = " ".join(e.text.strip() for e in root.iter("create_query"))
        table = re.search(r"CREATE TABLE (\w+)", create_sql).group(1)
        fills = [bench.unescape(e.text) for e in root.iter("fill_query")]
        create(table, create_sql, fills)
        check(table)
        for i, sql in enumerate(e.text for e in root.iter("query") if table in e.text):
            run_case(f"{name}#{i + 1}", bench.unescape(sql), iters)
        every(lambda v: f"DROP TABLE IF EXISTS {table} SYNC")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
