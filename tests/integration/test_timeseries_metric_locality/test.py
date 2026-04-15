# pylint: disable=wrong-import-order
"""Integration tests for TimeSeries `metric_locality_id` on the data target.

Coverage note — `StorageTimeSeriesSelector::getConfiguration` accepts DATA targets whose
inner MergeTree has **no** physical `metric_locality_id` (it synthesizes the output type
and sets `data_inner_table_has_metric_locality_id = false`). For that legacy layout,
`readImpl` keeps the historical `id IN (SELECT … FROM tags)` filter; the exposed
`metric_locality_id` column is computed from a literal `__name__` when possible, and
zero-filled otherwise. This file does **not** add integration coverage for that path;
exercising it needs a fixture with an attached/legacy DATA schema. The tests below still
lock in **client/SQL forward compatibility** when the column **is** present on disk.
"""
import os
import sys

import pytest

_THIS_DIR = os.path.dirname(os.path.realpath(__file__))
_PROM_DIR = os.path.join(_THIS_DIR, "..", "test_prometheus_protocols")
sys.path.insert(0, _PROM_DIR)
sys.path.insert(0, os.path.join(_PROM_DIR, "pb2"))

from helpers.cluster import ClickHouseCluster
from prometheus_test_utils import convert_time_series_to_protobuf, send_protobuf_to_remote_write


def _primary_key_used_keys_lists(explain_text: str) -> list[list[str]]:
    """Collect `used_keys` from each PrimaryKey / PrimaryKeyExpand block in `EXPLAIN indexes=1` output."""
    lines = explain_text.splitlines()
    result: list[list[str]] = []
    i = 0
    while i < len(lines):
        if lines[i].strip() not in ("PrimaryKey", "PrimaryKeyExpand"):
            i += 1
            continue
        i += 1
        while i < len(lines):
            stripped = lines[i].strip()
            if stripped in (
                "PrimaryKey",
                "PrimaryKeyExpand",
                "Partition",
                "Partition Min-Max",
                "Statistics",
                "Skip",
                "None",
            ):
                break
            if stripped == "Keys:":
                i += 1
                keys: list[str] = []
                while i < len(lines):
                    key = lines[i].strip()
                    if not key:
                        i += 1
                        continue
                    if key in (
                        "Condition:",
                        "Parts:",
                        "Granules:",
                        "Search Algorithm:",
                        "Ranges:",
                    ):
                        break
                    if key.endswith(":") and key != "Keys:":
                        break
                    keys.append(key)
                    i += 1
                result.append(keys)
                break
            i += 1
    return result


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    time_series = []
    base_ts = 1700000000
    for i in range(5):
        time_series.append(
            ({"__name__": "locality_test_metric", "instance": str(i)}, {base_ts + i: float(i)})
        )
    time_series.append(({"__name__": "other_metric", "instance": "x"}, {base_ts: 1.0}))
    write_request = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", write_request)
    try:
        yield cluster
    finally:
        cluster.shutdown()


def test_time_series_data_describe_includes_metric_locality_id():
    desc = node.query("DESCRIBE TABLE timeSeriesData('default', 'prometheus')")
    assert "metric_locality_id" in desc


def test_time_series_tags_describe_excludes_metric_locality_id():
    desc = node.query("DESCRIBE TABLE timeSeriesTags('default', 'prometheus')")
    assert "metric_locality_id" not in desc


def test_time_series_metrics_describe_excludes_metric_locality_id():
    desc = node.query("DESCRIBE TABLE timeSeriesMetrics('default', 'prometheus')")
    assert "metric_locality_id" not in desc


def test_one_locality_value_per_metric_name():
    out = node.query(
        """
        SELECT metric_name, uniqExact(d.metric_locality_id)
        FROM timeSeriesTags('default', 'prometheus') AS t
        INNER JOIN timeSeriesData('default', 'prometheus') AS d ON t.id = d.id
        GROUP BY metric_name
        ORDER BY metric_name
        """
    )
    lines = [ln for ln in out.strip().split("\n") if ln]
    by_name = {}
    for ln in lines:
        parts = ln.split("\t")
        by_name[parts[0]] = int(parts[1])
    assert by_name["locality_test_metric"] == 1
    assert by_name["other_metric"] == 1


def test_time_series_metric_locality_id_matches_sql_sip_hash():
    assert (
        node.query(
            "SELECT timeSeriesMetricLocalityId('locality_test_metric') = toUInt32(sipHash64('locality_test_metric'))"
        ).strip()
        == "1"
    )


def test_time_series_selector_reads_metric_locality_id():
    """Selector exposes metric_locality_id (UInt32) and returns rows for ingested samples."""
    out = node.query(
        """
        SELECT
            toTypeName(metric_locality_id),
            uniqExact(metric_locality_id),
            count() > 0
        FROM timeSeriesSelector(
            'default',
            'prometheus',
            'locality_test_metric',
            toDateTime64('2000-01-01 00:00:00', 3),
            toDateTime64('2035-01-01 00:00:00', 3)
        )
        """
    )
    parts = out.strip().split("\t")
    assert parts[0] == "UInt32"
    assert int(parts[1]) == 1
    assert parts[2] == "1"


def test_explain_indexes_primary_key_uses_metric_locality_id_first():
    """Data inner table ORDER BY (metric_locality_id, id, timestamp): PK analysis must list metric_locality_id first."""
    plan = node.query(
        """
        EXPLAIN indexes = 1
        SELECT count()
        FROM timeSeriesSelector(
            'default',
            'prometheus',
            'locality_test_metric',
            toDateTime64('2000-01-01 00:00:00', 3),
            toDateTime64('2035-01-01 00:00:00', 3)
        )
        FORMAT TabSeparated
        """
    )
    key_lists = _primary_key_used_keys_lists(plan)
    assert any(keys and keys[0] == "metric_locality_id" for keys in key_lists), plan


def test_legacy_id_in_subquery_matches_time_series_selector_cardinality():
    """Older SQL that filters data with `id IN (SELECT tags.id …)` must return the same rows as timeSeriesSelector."""
    t0 = "toDateTime64('2000-01-01 00:00:00', 3)"
    t1 = "toDateTime64('2035-01-01 00:00:00', 3)"
    selector_count = node.query(
        f"""
        SELECT count()
        FROM timeSeriesSelector(
            'default',
            'prometheus',
            'locality_test_metric',
            {t0},
            {t1}
        )
        """
    ).strip()
    legacy_count = node.query(
        f"""
        SELECT count()
        FROM timeSeriesData('default', 'prometheus') AS d
        WHERE d.id IN (
            SELECT tags.id
            FROM timeSeriesTags('default', 'prometheus') AS tags
            WHERE tags.metric_name = 'locality_test_metric'
              AND tags.min_time <= {t1}
              AND tags.max_time >= {t0}
        )
          AND d.timestamp >= {t0}
          AND d.timestamp <= {t1}
        """
    ).strip()
    assert selector_count == legacy_count
    assert int(selector_count) > 0


def test_tuple_locality_semijoin_matches_time_series_selector_cardinality():
    """Explicit `(metric_locality_id, id) IN (SELECT locality, id …)` matches selector semantics for one metric."""
    t0 = "toDateTime64('2000-01-01 00:00:00', 3)"
    t1 = "toDateTime64('2035-01-01 00:00:00', 3)"
    selector_count = node.query(
        f"""
        SELECT count()
        FROM timeSeriesSelector(
            'default',
            'prometheus',
            'locality_test_metric',
            {t0},
            {t1}
        )
        """
    ).strip()
    tuple_count = node.query(
        f"""
        SELECT count()
        FROM timeSeriesData('default', 'prometheus') AS d
        WHERE (d.metric_locality_id, d.id) IN (
            SELECT
                timeSeriesMetricLocalityId(tags.metric_name),
                tags.id
            FROM timeSeriesTags('default', 'prometheus') AS tags
            WHERE tags.metric_name = 'locality_test_metric'
              AND tags.min_time <= {t1}
              AND tags.max_time >= {t0}
        )
          AND d.timestamp >= {t0}
          AND d.timestamp <= {t1}
        """
    ).strip()
    assert tuple_count == selector_count


def test_prometheus_query_and_range_run():
    node.query(
        "SELECT count() FROM prometheusQuery('default', 'prometheus', 'locality_test_metric', now())"
    )
    node.query(
        "SELECT count() FROM prometheusQueryRange('default', 'prometheus', 'locality_test_metric', now() - 3600, now(), INTERVAL 1 MINUTE)"
    )
