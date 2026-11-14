#!/usr/bin/env python3
"""Handoff #19: a continuous small-insert loop into a version 11 TimeSeries table, measured in steady state.
Usage: loop.py <variant> <table> <seconds> [rate per second] [series per statement]
Variants:
  base      CREATE TABLE <table> ENGINE = TimeSeries SETTINGS version = 11 (defaults: tags deduplication cache on)
  batch<K>  base, but K consecutive statements are sent as one (K times the series, 1/K the rate): batched inserts
  merge:<s> base, plus `ALTER TABLE <inner tags and time ranges> MODIFY SETTING <s>` (comma separated, e.g.
            merge:min_age_to_force_merge_seconds=10)
  trcache<B>  base with store_time_ranges = 0, plus an AggregatingMergeTree table `<table>_tr` with the inner time ranges structure,
            written by this script the way a time ranges deduplication cache with outward rounding to B seconds would write it:
            a series gets the row [floor(t / B) * B, floor(t / B) * B + B - 1 ms] only when the cache doesn't hold that bucket for
            its id yet (one server, no eviction; the cache would be per server like the tags cache).
Data: SLOTS (60000) series slots of metric_0, labels instance = host-intDiv(i, 10), job = job(i % 10), pod = pod-<i>-<generation>,
region = r(i % 7); a slot's generation changes every CHURN seconds (phase spread by slot), so CHURN / SLOTS seconds is the lifetime
of a series (pod restarts) and SLOTS / CHURN new series appear per second. Every statement writes the next slice of `series per
statement` slots (default 3000) with one sample at the current wall-clock time: a round of all slots takes SLOTS / slice statements.
A sampler records the active parts and rows of the inner tags and time ranges tables every 5 s. At the end the loop stops, merges of
the tags and time ranges tables are stopped (the snapshot that streams.py measures) and the samples are written to <table>.parts.json.
HISTORY (300000) dead series of metric_0 with one sample a day earlier are written first and merged into one part per inner table.
REPLICATED=1 (with DB=<a Replicated database>) declares Replicated* inner engines: their merges are assigned by the merge selecting
task (merge_selecting_sleep_ms) as in the Cloud's replicated tables, instead of right after each insert as in a plain MergeTree.
Environment: SLOTS=60000 CHURN=1800 HISTORY=300000 PORT=18124 (HTTP) DB=default REPLICATED=0."""
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse

VARIANT, TABLE, SECONDS = sys.argv[1], sys.argv[2], float(sys.argv[3])
RATE = float(sys.argv[4]) if len(sys.argv) > 4 else 6.0
SLICE = int(sys.argv[5]) if len(sys.argv) > 5 else 3000
SLOTS, CHURN, PORT = int(os.environ.get("SLOTS", 60000)), int(os.environ.get("CHURN", 1800)), int(os.environ.get("PORT", 18124))
DB, REPLICATED = os.environ.get("DB", "default"), os.environ.get("REPLICATED", "0") == "1"
BATCH = int(VARIANT[5:]) if VARIANT.startswith("batch") else 1
BUCKET = int(VARIANT[7:]) if VARIANT.startswith("trcache") else 0
TAIL = float(os.environ.get("TAIL", 0))  # the last TAIL seconds run with the merges of the tags and time ranges tables stopped
ALIGN = os.environ.get("ALIGN", "0") == "1"  # trcache: put a bucket boundary at the start of the tail (the worst case)


class Conn:
    def __init__(self):
        self.c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=600)

    def q(self, sql, **settings):
        params = urllib.parse.urlencode({"max_memory_usage": 1500000000, "max_query_size": 100000000, "database": DB, **settings})
        try:
            self.c.request("POST", "/?" + params, body=sql.encode())
            r = self.c.getresponse()
        except (http.client.HTTPException, ConnectionError):  # the server closed an idle keep-alive connection
            self.c.close()
            self.c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=600)
            self.c.request("POST", "/?" + params, body=sql.encode())
            r = self.c.getresponse()
        body = r.read().decode()
        if r.status != 200:
            raise RuntimeError(f"{r.status}: {body[:500]}")
        return body


main = Conn()
main.q(f"DROP TABLE IF EXISTS {TABLE} SYNC")
main.q(f"DROP TABLE IF EXISTS {TABLE}_tr SYNC")
rep = "Replicated" if REPLICATED else ""
engines = (f" TAGS INNER ENGINE = {rep}ReplacingMergeTree SAMPLES INNER ENGINE = {rep}AggregatingMergeTree "
           f"RECENT SAMPLES INNER ENGINE = {rep}AggregatingMergeTree" + ("" if BUCKET else f" TIME RANGES INNER ENGINE = {rep}AggregatingMergeTree"))
main.q(f"CREATE TABLE {TABLE} ENGINE = TimeSeries SETTINGS version = 11{', store_time_ranges = 0' if BUCKET else ''}{engines if REPLICATED else ''}")
uuid = main.q(f"SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = '{TABLE}'").strip()
TAGS, TR = f"`.inner_id.tags.{uuid}`", (f"{TABLE}_tr" if BUCKET else f"`.inner_id.timeranges.{uuid}`")
if BUCKET:
    main.q(f"CREATE TABLE {TR} (id Tuple(UInt64, LowCardinality(UUID)), min_time SimpleAggregateFunction(min, DateTime64(3)), "
           f"max_time SimpleAggregateFunction(max, DateTime64(3))) ENGINE = {rep}AggregatingMergeTree ORDER BY id SETTINGS index_granularity = 8192")
if VARIANT.startswith("merge:"):
    for t in (TAGS, TR):
        main.q(f"ALTER TABLE {t} MODIFY SETTING {VARIANT[6:]}")


HISTORY = int(os.environ.get("HISTORY", 300000))
if HISTORY:
    # Dead series of metric_0 (old pods), one sample a day ago, merged into one part per inner table: the history a long-running
    # table holds. The fresh parts of the loop are small next to it, so the merge selector leaves a tail of small parts.
    hist_labels = ("map('instance', concat('host-', toString(intDiv(i % {s}, 10))), 'job', concat('job', toString(i % 10)), "
                   "'pod', concat('pod-', toString(i % {s}), '-h', toString(intDiv(i, {s}))), 'region', concat('r', toString(i % 7)))").format(s=SLOTS)
    t_hist = time.time() - 86400
    main.q(f"INSERT INTO {TABLE} (metric_name, tags, samples) SELECT 'metric_0', {hist_labels}, [(toDateTime64({t_hist:.3f}, 3), 1.)] "
           f"FROM (SELECT number AS i FROM numbers({HISTORY}))", max_insert_threads=1)
    if BUCKET:
        b = int(t_hist // BUCKET) * BUCKET
        main.q(f"INSERT INTO {TR} SELECT id, toDateTime64({b}, 3), toDateTime64({b + BUCKET} - 0.001, 3) FROM {TAGS}")
    for t in (TAGS, TR):
        main.q(f"OPTIMIZE TABLE {t} FINAL")


def gen(i, t):
    return int((t + (i * 7919) % CHURN) // CHURN)


LABELS = ("map('instance', concat('host-', toString(intDiv(i, 10))), 'job', concat('job', toString(i % 10)), "
          "'pod', concat('pod-', toString(i), '-', toString(g)), 'region', concat('r', toString(i % 7)))")
LABELS_ID = f"CAST(map('__name__', 'metric_0', {LABELS[4:]}, 'Map(LowCardinality(String), String)')"  # the tags column holds __name__ too; the id hashes this type


def series(slots, t):
    """A subquery of (i, g) for the slots at time t: the slot and its generation."""
    return (f"(SELECT i, intDiv(toUInt64({t:.3f} + (i * 7919) % {CHURN}), {CHURN}) AS g FROM "
            f"(SELECT arrayJoin([{','.join(map(str, slots))}]) AS i))") if slots else None


cache = {}  # trcache: slot -> (generation, bucket) whose rounded row is written
stats = {"statements": 0, "lat": [], "tr_rows": 0, "tr_inserts": 0, "samples": 0}


OFFSET = 0.0


def run():
    global OFFSET
    c = Conn()
    start, k, pos, frozen = time.time(), 0, 0, False
    if ALIGN and BUCKET:
        OFFSET = (start + SECONDS - TAIL) % BUCKET
    while time.time() - start < SECONDS:
        if TAIL and not frozen and time.time() - start >= SECONDS - TAIL:
            for t in (TAGS, TR):
                c.q(f"SYSTEM STOP MERGES {t}")
            frozen = True
            stats["tail_start"] = time.time()
        due = start + k * BATCH / RATE
        if time.time() < due:
            time.sleep(due - time.time())
        t = time.time()
        n = SLICE * BATCH
        slots = [(pos + j) % SLOTS for j in range(n)]
        pos = (pos + n) % SLOTS
        t0 = time.time()
        c.q(f"INSERT INTO {TABLE} (metric_name, tags, samples) SELECT 'metric_0', {LABELS}, [(toDateTime64({t:.3f}, 3), toFloat64(i))] "
            f"FROM {series(slots, t)}")
        if BUCKET:
            b = int((t - OFFSET) // BUCKET)
            need = [s for s in slots if cache.get(s) != (gen(s, t), b)]
            for s in need:
                cache[s] = (gen(s, t), b)
            if need:
                c.q(f"INSERT INTO {TR} SELECT tuple(sipHash64('metric_0'), toLowCardinality(reinterpretAsUUID(sipHash128({LABELS_ID})))), "
                    f"toDateTime64({b * BUCKET + OFFSET:.3f}, 3), toDateTime64({b * BUCKET + BUCKET + OFFSET:.3f} - 0.001, 3) FROM {series(need, t)}")
                stats["tr_rows"] += len(need)
                stats["tr_inserts"] += 1
        stats["lat"].append(time.time() - t0)
        stats["statements"] += 1
        stats["samples"] += n
        k += 1


samples = []
done = threading.Event()


def sampler():
    c = Conn()
    while not done.is_set():
        r = c.q(f"SELECT table LIKE '%tags%' ? 'tags' : 'tr', count(), countIf(level = 0), sum(rows) FROM system.parts "
                f"WHERE database = currentDatabase() AND active AND table IN ('{TAGS.strip('`')}', '{TR.strip('`')}') GROUP BY 1 FORMAT JSONCompact")
        samples.append({"t": time.time(), **{x[0]: x[1:] for x in json.loads(r)["data"]}})
        done.wait(5)


th = threading.Thread(target=sampler, daemon=True)
th.start()
t_start = time.time()
run()
wall = time.time() - t_start
for t in (TAGS, TR):
    main.q(f"SYSTEM STOP MERGES {t}")
done.set()
th.join()
final = main.q(f"SELECT _table LIKE '%tags%' ? 'tags' : 'tr', count(), uniqExact(id) FROM merge(currentDatabase(), "
               f"'^({TAGS.strip('`').replace('.', '[.]')}|{TR.strip('`').replace('.', '[.]')})$') GROUP BY 1 FORMAT TSV")
parts_final = main.q(f"SELECT table LIKE '%tags%' ? 'tags' : 'tr', count(), countIf(level = 0), sum(rows) FROM system.parts "
                     f"WHERE database = currentDatabase() AND active AND table IN ('{TAGS.strip('`')}', '{TR.strip('`')}') GROUP BY 1 FORMAT TSV")
lat = sorted(stats["lat"])
second_half = [s for s in samples if s["t"] - t_start > SECONDS / 2]


def med(xs):
    xs = sorted(xs)
    return xs[len(xs) // 2] if xs else -1


summary = {
    "variant": VARIANT, "table": TABLE, "seconds": round(wall, 1), "statements": stats["statements"],
    "rate": round(stats["statements"] / wall, 2), "series_per_statement": SLICE * BATCH, "samples": stats["samples"],
    "lat_ms_p50": round(lat[len(lat) // 2] * 1000), "lat_ms_p95": round(lat[int(len(lat) * 0.95)] * 1000),
    "tr_rows_emulated": stats["tr_rows"], "tr_inserts_emulated": stats["tr_inserts"],
    "parts_2nd_half": {k: {"median": med([s[k][0] for s in second_half if k in s]), "max": max([s[k][0] for s in second_half if k in s] or [-1]),
                           "level0_median": med([s[k][1] for s in second_half if k in s])} for k in ("tags", "tr")},
    "final_rows_ids": final.strip().split("\n"), "final_parts": parts_final.strip().split("\n"),
    "tags": TAGS, "tr": TR, "churn": CHURN, "slots": SLOTS, "db": DB, "tail": TAIL, "align": ALIGN, "end": t_start + wall,
    "start": t_start, "tail_start": stats.get("tail_start"), "replicated": REPLICATED,
}
with open(f"{TABLE}.parts.json", "w") as f:
    json.dump({"summary": summary, "samples": samples}, f)
print(json.dumps(summary))
