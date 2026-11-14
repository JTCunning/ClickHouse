"""The ids query of a TimeSeries selector (the `id IN (...)` subquery that StorageTimeSeriesSelector builds for a version 11
table), as master builds it and rewritten by hand. Imported by bench.py and probe.py; `python3 idsq.py <variant> <case>`
prints one query."""
import os
import subprocess
import sys

CH = "/root/nerve-workspace/tools/clickhouse/master/clickhouse"
PORT = "19100"
TABLE = os.environ.get("TABLE", "ts")  # ts (unmerged) or ts_merged (see gen.sh)
T0, T1 = 1700000000, 1700001800
NAME = "metric_0"
# Settings that StorageTimeSeriesSelector::applySettingsForSelectIDsQuery sets, plus the lab's fixed ones.
SETTINGS = ("join_algorithm = 'full_sorting_merge', optimize_read_in_order = 1, any_join_distinct_right_table_keys = 0, "
            "max_threads = 2, max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0")
# case: the matchers other than __name__, as matcherToAST writes them
CASES = {
    "wide: 3 matchers, all 60k": "match(arrayElement(tags, 'job'), '^job.*$'), notEquals(arrayElement(tags, 'region'), 'none'), "
                                 "match(arrayElement(tags, 'instance'), '^host-.*$')",
    "mid: job=job3, 6k": "equals(arrayElement(tags, 'job'), 'job3')",
    "narrow: instance=host-17, 10": "equals(arrayElement(tags, 'instance'), 'host-17')",
}


def client(query, query_id=None, fmt="TSV"):
    args = [CH, "client", "--port", PORT, "-q", query, "--format", fmt]
    if query_id:
        args += ["--query_id", query_id]
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


_uuid = None


def tables():
    global _uuid
    if _uuid is None:
        _uuid = client(f"SELECT uuid FROM system.tables WHERE database = 'default' AND name = '{TABLE}'").strip()
    return f"default.`.inner_id.tags.{_uuid}`", f"default.`.inner_id.timeranges.{_uuid}`"


def extra_settings(variant):
    """`groupio` = GROUP BY id with aggregation in the order of the table; `limitbyordq` = skip indexes in the index analysis."""
    return (", optimize_aggregation_in_order = 1" if "groupio" in variant else "") + (
        ", use_skip_indexes_on_data_read = 0" if "limitbyordq" in variant else "")


def ids_query(variant, case):
    """variant: <strictness>_<left>_<right>; strictness all|any; left: none|distinct|group|limitby|final; right: none|group|distinct."""
    strictness, left, right = variant.split("_")
    tags, tr = tables()
    h = f"sipHash64('{NAME}')"
    time = (f"greaterOrEquals(max_time, toDateTime64(toDecimal128('{T0}', 3), 3)), "
            f"lessOrEquals(min_time, toDateTime64(toDecimal128('{T1}', 3), 3))")
    idrange = (f"indexHint(and(greaterOrEquals(id, tuple({h}, toUUID('00000000-0000-0000-0000-000000000000'))), "
               f"lessOrEquals(id, tuple({h}, toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'))))), "
               f"`not`(notEquals(tupleElement(id, 1), {h}))")
    rsel = {"none": "SELECT id, 1 AS matched", "group": "SELECT id, 1 AS matched", "groupio": "SELECT id, 1 AS matched",
            "distinct": "SELECT DISTINCT id, 1 AS matched", "distinctord": "SELECT DISTINCT id, 1 AS matched"}[right]
    rq = f"{rsel} FROM {tr} WHERE and({time}, {idrange})" + (
        " GROUP BY id" if right in ("group", "groupio") else " ORDER BY id" if right == "distinctord" else "")
    matchers = f"and(equals(metric_name, '{NAME}'), {CASES[case]})"
    if left == "none":
        src, outer = tags, f"and({matchers}, or(equals(time_ranges_subquery.matched, 1), notEquals(tupleElement(id, 1), {h})))"
    elif left == "final":
        src, outer = f"{tags} FINAL", f"and({matchers}, or(equals(time_ranges_subquery.matched, 1), notEquals(tupleElement(id, 1), {h})))"
    else:
        inner = {
            "distinct": f"SELECT DISTINCT id, metric_name, tags FROM {tags} WHERE {matchers}",
            "groupio": f"SELECT id, any(m) AS metric_name, any(t) AS tags FROM (SELECT id, metric_name AS m, tags AS t FROM {tags} WHERE {matchers}) GROUP BY id",
            "group": f"SELECT id, any(m) AS metric_name, any(t) AS tags FROM (SELECT id, metric_name AS m, tags AS t FROM {tags} WHERE {matchers}) GROUP BY id",
            "limitby": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} LIMIT 1 BY id",
            "distinctord": f"SELECT DISTINCT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id",
            "limitbyord": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id LIMIT 1 BY id",
            # #18 recheck: limitbyord with the skip indexes of the tags read evaluated in the index analysis (as master's tags read
            # under the JOIN), set in the subquery (`limitbyordst`) or for the whole ids query (`limitbyordq`, see extra_settings)
            "limitbyordst": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id LIMIT 1 BY id SETTINGS use_skip_indexes_on_data_read = 0",
            "limitbyordq": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id LIMIT 1 BY id",
            "limitbynewest": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id, _part_index DESC LIMIT 1 BY id",
            "limitbyallord": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id LIMIT 1 BY id, metric_name, tags",
            "limitbyhashord": f"SELECT id, metric_name, tags FROM {tags} WHERE {matchers} ORDER BY id LIMIT 1 BY id, sipHash64(metric_name, tags)",
        }[left]
        src, outer = f"({inner}) AS tags_subquery", f"or(equals(time_ranges_subquery.matched, 1), notEquals(tupleElement(id, 1), {h}))"
    return (f"SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) AS series_id FROM {src} "
            f"{strictness.upper()} LEFT JOIN ({rq}) AS time_ranges_subquery USING (id) WHERE {outer}")


if __name__ == "__main__":
    print(ids_query(sys.argv[1], list(CASES)[int(sys.argv[2])]))
