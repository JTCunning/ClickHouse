# A/B emulation for lilred/perf-promql-drop-name-exact-selector, no build needed.
# Runs on a local master server (port 19000) holding the TimeSeries table `ts` (dataset: see README.md).
# master : prometheusQuery / prometheusQueryRange as shipped.
# handm  : the SQL master generates (taken from the server log), run as-is. Validates the method: same CPU and value as master.
# branch : the same SQL with the `__name__` drop step written as the branch generates it: a projection
#          `timeSeriesRemoveTag(group, '__name__') AS new_group, values` without GROUP BY / anyForEach / HAVING check.
import json, os, re, statistics, subprocess, sys, time

CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
T = int(open(os.environ.get("T_END_FILE", "t_end")).read())
SETTINGS = "max_threads = 2, use_query_cache = 0"


def q(sql, fmt="TSVRaw"):
    r = subprocess.run([CH, "client", "--port", "19000", "--max_query_size", "100000000", "--queries-file", "/dev/stdin"],
                       input=sql + (" FORMAT " + fmt if fmt else ""), capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr[:1500])
    return r.stdout


CASES = {
    "sum_rate":       f"SELECT * FROM prometheusQuery(ts, 'sum(rate(metric_0[5m]))', {T})",
    "sum_by_rate":    f"SELECT * FROM prometheusQuery(ts, 'sum by (job, cluster) (rate(metric_0[5m]))', {T})",
    "sum_abs":        f"SELECT * FROM prometheusQuery(ts, 'sum(abs(metric_0))', {T})",
    "rate_narrow":    f"SELECT count(), sum(value) FROM prometheusQuery(ts, 'rate(metric_1[5m])', {T})",
    "range_sum_rate": f"SELECT * FROM prometheusQueryRange(ts, 'sum(rate(metric_0[1m]))', {T - 540}, {T}, 15)",
}

DROP_RE = re.compile(r"anyForEach\(`values`\) AS `values` FROM (\w+) GROUP BY new_group HAVING equals\(timeSeriesThrowDuplicateSeriesIf\("
                     r"arrayExists\(lambda\(tuple\(c\), greater\(c, 1\)\), countForEach\(\w+\.`values`\)\), new_group\), 0\)")


def generated_sql(case, sql):
    tag = f"cap_{case}_{int(time.time() * 1000)}"
    q(f"{sql} SETTINGS {SETTINGS}, log_comment = '{tag}'")
    q("SYSTEM FLUSH LOGS", None)
    qid = q(f"SELECT query_id FROM system.query_log WHERE log_comment = '{tag}' AND type = 'QueryFinish' LIMIT 1").strip()
    msg = q(f"SELECT message FROM system.text_log WHERE query_id = '{qid}' AND logger_name = 'StoragePrometheusQuery' "
            f"AND message LIKE 'Will execute query:%' LIMIT 1")
    outer = msg.split("Will execute query:", 1)[1].strip()
    # keep the outer projection of the case (count/sum wrapper for rate_narrow)
    if case == "rate_narrow":
        return f"SELECT count(), sum(value) FROM ({outer})"
    return outer


def run(sql, tag):
    q(f"{sql} SETTINGS {SETTINGS}, log_comment = '{tag}'", None)


def stats(tag_prefix):
    q("SYSTEM FLUSH LOGS", None)
    rows = q(f"SELECT log_comment, query_duration_ms, ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'], "
             f"memory_usage, read_rows FROM system.query_log WHERE type = 'QueryFinish' AND log_comment LIKE '{tag_prefix}%' ORDER BY event_time_microseconds")
    return [r.split("\t") for r in rows.splitlines()]


if __name__ == "__main__":
    rounds, per_round = 3, 6
    rid = str(int(time.time()))
    variants = {}
    for case, sql in CASES.items():
        hand = generated_sql(case, sql)
        branch, n = DROP_RE.subn(r"`values` FROM \1", hand)
        assert n == 1, (case, n)
        variants[case] = {"master": sql, "handm": hand, "branch": branch}
        vals = {k: q(v + f" SETTINGS {SETTINGS}") for k, v in variants[case].items()}
        print(json.dumps({"case": case, "values_equal": len(set(vals.values())) == 1, "value": vals["master"][:200]}), flush=True)
        with open(f"sql_{case}_branch.sql", "w") as f:
            f.write(branch + "\n")
    for rnd in range(rounds):
        for i in range(per_round):
            for case, vs in variants.items():
                for k, v in vs.items():
                    run(v, f"b{rid}_{case}_{k}_{rnd}_{i}")
    res = {}
    for tag, dur, cpu, mem, rr in stats(f"b{rid}_"):
        parts = tag.split("_")
        k, rnd, i = parts[-3:]
        case = "_".join(parts[1:-3])
        if i == "0":
            continue  # first run of each round is the warm-up
        res.setdefault((case, k, rnd), []).append((int(dur), int(cpu) / 1000, int(mem) / 2**20, int(rr)))
    out = {}
    for (case, k, rnd), xs in res.items():
        o = out.setdefault((case, k), {"wall": [], "cpu": [], "mem": [], "read_rows": xs[0][3]})
        o["wall"].append(statistics.median(x[0] for x in xs))
        o["cpu"].append(statistics.median(x[1] for x in xs))
        o["mem"].extend(x[2] for x in xs)
    for (case, k), o in sorted(out.items()):
        print(json.dumps({"case": case, "variant": k, "wall_ms": round(statistics.median(o["wall"]), 1),
                          "cpu_ms": round(statistics.median(o["cpu"]), 1), "cpu_round_medians": o["cpu"],
                          "mem_mib": [round(min(o["mem"])), round(max(o["mem"]))], "read_rows": o["read_rows"]}), flush=True)
