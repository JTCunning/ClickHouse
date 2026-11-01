# A/B of the TimeSeries id type on a prebuilt master server (port 19100), no build needed.
#   lc    : the v11 default id Tuple(UInt64, LowCardinality(UUID))   (CREATE TABLE t ENGINE = TimeSeries)
#   plain : Tuple(UInt64, UUID)  (same, plus TAGS INNER COLUMNS (id Tuple(UInt64, UUID)), which propagates to all inner tables)
# Part 1: the in-tree perf tests that create TimeSeries tables, run for both id types.
# Part 2: own cases on ts_lc / ts_plain (dataset: README.md).
# Interleaved runs (lc, plain, lc, plain ...), RUNS each after one discarded warm-up; prints JSON lines.
import json, os, re, statistics, subprocess, sys, time
import xml.etree.ElementTree as ET

CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
REPO = "/root/nerve-workspace/repos/ClickHouse"
RUNS = int(os.environ.get("RUNS", "7"))
PLAIN = " TAGS INNER COLUMNS (id Tuple(UInt64, UUID))"


def q(sql, fmt=None):
    r = subprocess.run([CH, "client", "--port", "19100", "--max_query_size", "100000000", "--queries-file", "/dev/stdin"],
                       input=sql + (" FORMAT " + fmt if fmt else ""), capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(sql[:300] + "\n" + r.stderr[:1500])
    return r.stdout


def with_settings(sql, extra):
    sql = sql.strip().rstrip(";")
    fmt = ""
    m = re.search(r"\s+FORMAT\s+\w+\s*$", sql)
    if m:
        fmt, sql = m.group(0), sql[:m.start()]
    if re.search(r"\bSETTINGS\b[^)]*$", sql):
        return f"{sql}, {extra}{fmt}"
    return f"{sql} SETTINGS {extra}{fmt}"


def optimize(table):
    uuid = q(f"SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '{table}'", "TSVRaw").strip()
    for t in q(f"SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%{uuid}' AND name NOT LIKE '%metrics%'", "TSVRaw").split():
        q(f"OPTIMIZE TABLE `{t}` FINAL")


def ab(case, queries, rid):
    """queries: {'lc': sql, 'plain': sql}"""
    vals = {k: q(with_settings(v, "max_threads = 2, use_query_cache = 0").replace(" FORMAT Null", ""), "TSVRaw") for k, v in queries.items()}
    tagbase = f"lc{rid}_{re.sub(r'[^a-z0-9]', '', case.lower())[:40]}"
    for i in range(RUNS + 1):
        for k, v in queries.items():
            q(with_settings(v, f"max_threads = 2, use_query_cache = 0, log_comment = '{tagbase}_{k}_{i}'"))
    q("SYSTEM FLUSH LOGS")
    rows = q(f"SELECT log_comment, query_duration_ms, (ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1000, "
             f"memory_usage, read_rows, read_bytes FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE '{tagbase}\\_%'", "TSVRaw")
    res = {}
    for line in rows.splitlines():
        tag, dur, cpu, mem, rr, rb = line.split("\t")
        _, k, i = tag.rsplit("_", 2)
        if i == "0":
            continue  # warm-up
        res.setdefault(k, []).append((float(dur), float(cpu), int(mem), int(rr), int(rb)))
    out = {"case": case, "same_result": vals["lc"] == vals["plain"]}
    for k, rs in res.items():
        out[k] = {"ms_med": statistics.median(r[0] for r in rs), "ms_min": min(r[0] for r in rs), "ms_max": max(r[0] for r in rs),
                  "cpu_ms_med": statistics.median(r[1] for r in rs), "mem_mib_med": round(statistics.median(r[2] for r in rs) / 2**20, 1),
                  "read_rows": rs[0][3], "read_mib": round(rs[0][4] / 2**20, 1), "n": len(rs)}
    if "lc" in out and "plain" in out:
        out["lc_over_plain_ms"] = round(out["lc"]["ms_med"] / out["plain"]["ms_med"], 3)
        out["lc_over_plain_cpu"] = round(out["lc"]["cpu_ms_med"] / out["plain"]["cpu_ms_med"], 3)
        out["lc_over_plain_mem"] = round(out["lc"]["mem_mib_med"] / max(out["plain"]["mem_mib_med"], 0.1), 3)
    print(json.dumps(out), flush=True)


def unescape(s):
    return s.replace("{{", "{").replace("}}", "}")


def perf_test(name, rid):
    xml = subprocess.run(["git", "-C", REPO, "show", f"origin/master:tests/performance/{name}.xml"], capture_output=True, text=True, check=True).stdout
    root = ET.fromstring(xml)
    create = " ".join(e.text.strip() for e in root.iter("create_query"))
    table = re.search(r"CREATE TABLE (\w+)", create).group(1)
    fills = [unescape(e.text) for e in root.iter("fill_query")]
    queries = [unescape(e.text) for e in root.iter("query") if table in e.text]
    plain_table = table + "_plain"
    for t in (table, plain_table):
        q(f"DROP TABLE IF EXISTS {t} SYNC")
    q(create)
    q(re.sub(rf"\b{table}\b", plain_table, create) + PLAIN)
    for t in (table, plain_table):
        for f in fills:
            q(with_settings(re.sub(rf"\b{table}\b", t, f), "max_insert_threads = 1, max_threads = 2, max_block_size = 512, max_insert_block_size = 512, min_insert_block_size_rows = 512"))
        optimize(t)
    for i, sql in enumerate(queries):
        ab(f"{name}#{i + 1}: {' '.join(sql.split())[:150]}", {"lc": sql, "plain": re.sub(rf"\b{table}\b", plain_table, sql)}, rid)
    for t in (table, plain_table):
        q(f"DROP TABLE IF EXISTS {t} SYNC")


OWN = {
    "selector wide (100k series), instant": "SELECT count(), sum(value) FROM prometheusQuery({t}, 'metric_0', {T})",
    "sum(rate(metric_0[5m])), instant": "SELECT * FROM prometheusQuery({t}, 'sum(rate(metric_0[5m]))', {T})",
    "sum by (job) (rate[5m]), 4h range step 60": "SELECT count() FROM prometheusQueryRange({t}, 'sum by (job) (rate(metric_0[5m]))', {T0}, {T}, 60)",
    "rate[5m] per series, 1h range step 60": "SELECT count(), sum(length(samples)) FROM prometheusQueryRange({t}, 'rate(metric_0[5m])', {T1h}, {T}, 60)",
    "narrow selector (1 series), instant": "SELECT count(), sum(value) FROM prometheusQuery({t}, 'metric_0{{pod=\"pod-5\"}}', {T})",
    "narrow metric (~1k series), sum(rate)": "SELECT * FROM prometheusQuery({t}, 'sum(rate(metric_7[5m]))', {T})",
    "label filter (10% of metric_0), sum(rate)": "SELECT * FROM prometheusQuery({t}, 'sum(rate(metric_0{{job=\"job3\"}}[5m]))', {T})",
    "timeSeriesSelector raw scan 4h": "SELECT count(), sum(value) FROM timeSeriesSelector({t}, 'metric_0', {T0}, {T}, 'timestamp_and_value')",
    "x + x (two selectors join on group)": "SELECT count() FROM prometheusQuery({t}, 'metric_0 + metric_0', {T})",
}

if __name__ == "__main__":
    rid = str(int(time.time()))
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "own"):
        T = int(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "t_end")).read())
        for case, sql in OWN.items():
            p = dict(T=T, T0=T - 14400 + 300, T1h=T - 3600)
            ab(case, {"lc": sql.format(t="ts_lc", **p), "plain": sql.format(t="ts_plain", **p)}, rid)
    if which in ("all", "perf"):
        for name in ["promql_selector_id_range_row_cost", "promql_selector_pk_range", "promql_set_operator_presence_mask",
                     "promql_topk", "promql_tags_transform"]:
            perf_test(name, rid)
