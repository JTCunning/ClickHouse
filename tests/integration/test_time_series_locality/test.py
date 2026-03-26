import os
import sys

import pytest

from helpers.cluster import ClickHouseCluster

# Reuse Prometheus Remote Write helpers from test_prometheus_protocols (pb2 + snappy).
_PROM_TEST_DIR = os.path.join(os.path.dirname(__file__), "..", "test_prometheus_protocols")
sys.path.insert(0, _PROM_TEST_DIR)
from prometheus_test_utils import (  # noqa: E402
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

PROMETHEUS_PORT = 9093
# Matches tests/integration/test_time_series_locality/configs/prometheus.xml handlers.
REMOTE_WRITE_PATH = {
    "ts_locality": "write_locality",
    "ts_legacy": "write_legacy",
    "ts_promote_a": "write_promote_a",
    "ts_promote_b": "write_promote_b",
}

# 2020-01-01 00:00:00 UTC (same instant as the previous SQL INSERT used for ordering).
_BASE_TS_SEC = 1577836800


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=["configs/prometheus.xml"],
            user_configs=["configs/users.d/allow_time_series.xml"],
        )
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _base_settings():
    return {"allow_experimental_time_series_table": 1}


def _insert_many_series(instance, table: str):
    """Insert distinct series via Remote Write (SQL INSERT is not implemented for Engine TimeSeries)."""
    time_series = []
    for i in range(32):
        labels = {
            "__name__": "http_requests_total",
            "instance": f"instance-{i}",
            "job": "api",
        }
        samples = {_BASE_TS_SEC + i: float(i)}
        time_series.append((labels, samples))
    protobuf = convert_time_series_to_protobuf(time_series)
    path = REMOTE_WRITE_PATH[table]
    send_protobuf_to_remote_write(instance.ip_address, PROMETHEUS_PORT, path, protobuf)


def test_time_series_locality_id_collapses_id_prefix_for_same_metric_and_tag_keys(
    started_cluster,
):
    """
    With allow_experimental_time_series_locality_id, the high 64 bits of the series id
    depend on metric_name and label key names, not label values. Many series that differ
    only by tag values therefore share the same id prefix — the property MergeTree uses
    for physical locality under ORDER BY (id, timestamp).

    With the legacy sipHash128 default, those bits are uncorrelated with metric/tags structure,
    so almost every series gets a distinct prefix.
    """
    instance = cluster.instances["node"]
    base = _base_settings()

    instance.query("DROP TABLE IF EXISTS ts_locality SYNC", settings=base)
    instance.query("DROP TABLE IF EXISTS ts_legacy SYNC", settings=base)

    locality_settings = {**base, "allow_experimental_time_series_locality_id": 1}
    legacy_settings = {**base, "allow_experimental_time_series_locality_id": 0}

    ddl = """CREATE TABLE {name} (
        `instance` LowCardinality(String),
        `job` LowCardinality(String)
    )
    ENGINE = TimeSeries
    SETTINGS tags_to_columns = {{'instance': 'instance', 'job': 'job'}}, store_min_time_and_max_time = 0"""

    instance.query(
        ddl.format(name="ts_locality"),
        settings=locality_settings,
    )
    instance.query(
        ddl.format(name="ts_legacy"),
        settings=legacy_settings,
    )

    _insert_many_series(instance, "ts_locality")
    _insert_many_series(instance, "ts_legacy")

    prefix_expr = "bitShiftRight(reinterpretAsUInt128(id), 64)"

    uniq_prefix_locality = int(
        instance.query(
            f"SELECT uniqExact({prefix_expr}) FROM timeSeriesTags(ts_locality)",
            settings=base,
        ).strip()
    )
    uniq_prefix_legacy = int(
        instance.query(
            f"SELECT uniqExact({prefix_expr}) FROM timeSeriesTags(ts_legacy)",
            settings=base,
        ).strip()
    )

    row_count_locality = int(
        instance.query(
            "SELECT count() FROM timeSeriesTags(ts_locality)",
            settings=base,
        ).strip()
    )
    row_count_legacy = int(
        instance.query(
            "SELECT count() FROM timeSeriesTags(ts_legacy)",
            settings=base,
        ).strip()
    )

    assert row_count_locality == 32
    assert row_count_legacy == 32
    assert uniq_prefix_locality == 1, (
        "expected a single locality prefix for the same metric and tag key set "
        f"when allow_experimental_time_series_locality_id is enabled, got {uniq_prefix_locality}"
    )
    assert uniq_prefix_legacy >= 30, (
        "expected legacy sipHash128 ids to spread across many distinct high-bit prefixes "
        f"when locality is disabled, got {uniq_prefix_legacy}"
    )


def test_time_series_locality_id_unchanged_when_promoting_tag_to_column(
    started_cluster,
):
    """
    A label can live only in the tags Map, or be duplicated as a promoted column via
    tags_to_columns. timeSeriesLocalityId merges map entries with promoted pairs so the
    128-bit id matches whenever the logical (metric, labels) is the same.
    """
    instance = cluster.instances["node"]
    base = _base_settings()
    locality_settings = {**base, "allow_experimental_time_series_locality_id": 1}

    instance.query("DROP TABLE IF EXISTS ts_promote_a SYNC", settings=base)
    instance.query("DROP TABLE IF EXISTS ts_promote_b SYNC", settings=base)

    ddl_a = """CREATE TABLE ts_promote_a (
        `instance` LowCardinality(String)
    )
    ENGINE = TimeSeries
    SETTINGS tags_to_columns = {'instance': 'instance'}, store_min_time_and_max_time = 0"""

    ddl_b = """CREATE TABLE ts_promote_b (
        `instance` LowCardinality(String),
        `job` LowCardinality(String)
    )
    ENGINE = TimeSeries
    SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}, store_min_time_and_max_time = 0"""

    instance.query(ddl_a, settings=locality_settings)
    instance.query(ddl_b, settings=locality_settings)

    _insert_many_series(instance, "ts_promote_a")
    _insert_many_series(instance, "ts_promote_b")

    rows_a = int(
        instance.query(
            "SELECT count() FROM timeSeriesTags(ts_promote_a)",
            settings=base,
        ).strip()
    )
    rows_b = int(
        instance.query(
            "SELECT count() FROM timeSeriesTags(ts_promote_b)",
            settings=base,
        ).strip()
    )
    assert rows_a == 32
    assert rows_b == 32

    ids_a = instance.query(
        "SELECT toString(id) FROM timeSeriesTags(ts_promote_a) ORDER BY toString(id)",
        settings=base,
    ).splitlines()
    ids_b = instance.query(
        "SELECT toString(id) FROM timeSeriesTags(ts_promote_b) ORDER BY toString(id)",
        settings=base,
    ).splitlines()
    assert ids_a == ids_b, (
        "promoting job from tags-only to tags_to_columns must not change locality ids"
    )
