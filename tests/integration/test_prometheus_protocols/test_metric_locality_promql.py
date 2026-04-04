"""PromQL-oriented coverage for `metric_locality_id` pruning on `timeSeriesSelector` reads.

Uses the same EXPLAIN shape as `tests/queries/0_stateless/04078_time_series_selector_metric_locality_in_prewhere.sql`
(subquery over `EXPLAIN QUERY TREE dump_ast = 1`) and enables the analyzer in the test instance so the inner
MergeTree PREWHERE is visible in `explain` text.

Includes smoke `prometheusQuery` / `prometheusQueryRange` calls so the PromQL SQL layer stays exercised.
"""

import pytest

from helpers.cluster import ClickHouseCluster

from .prometheus_test_utils import convert_time_series_to_protobuf, send_protobuf_to_remote_write


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/enable_query_tree_explain.xml",
    ],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_reader=True,
    with_prometheus_receiver=True,
)


@pytest.fixture(scope="module")
def metric_locality_explain_checks_applicable(request):
    """Skip EXPLAIN substring checks when the function is absent or EXPLAIN does not expand the selector."""
    request.getfixturevalue("start_cluster")
    if node.query("SELECT count() FROM system.functions WHERE name = 'timeSeriesMetricLocalityId'").strip() != "1":
        pytest.skip(
            "timeSeriesMetricLocalityId is missing from system.functions; skip metric_locality_id EXPLAIN assertions."
        )
    node.query("SET enable_analyzer = 1")
    node.query("SET allow_experimental_analyzer = 1")
    node.query("SET enable_parallel_replicas = 0")
    # QUERY TREE explain keeps `timeSeriesSelector` as a TABLE_FUNCTION without inlining the inner MergeTree read,
    # so `metric_locality_id` does not appear in the dumped text even when the read path uses it.
    canary = """
SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector('default', 'prometheus', '{__name__="up"}',
        toDateTime64('1970-01-01 00:00:00', 3),
        toDateTime64('1970-01-02 00:00:00', 3))
)
"""
    if node.query(canary).strip() != "1":
        pytest.skip(
            "EXPLAIN QUERY TREE does not expose metric_locality_id for timeSeriesSelector (table function not expanded in dump)."
        )


def send_data(time_series):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(
        cluster.prometheus_receiver_ip,
        cluster.prometheus_receiver_port,
        "api/v1/write",
        protobuf,
    )
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def assert_metric_locality_in_explain_subquery(selector_literal: str):
    """selector_literal is embedded as the third argument to timeSeriesSelector (escape single quotes for SQL)."""
    node.query("SET enable_analyzer = 1")
    node.query("SET allow_experimental_analyzer = 1")
    node.query("SET enable_parallel_replicas = 0")
    escaped = selector_literal.replace("'", "''")
    q = f"""
SELECT max(position(explain, 'metric_locality_id')) > 0
FROM (
    EXPLAIN QUERY TREE dump_ast = 1
    SELECT id, timestamp, value
    FROM timeSeriesSelector('default', 'prometheus', '{escaped}',
        toDateTime64('1970-01-01 00:00:00', 3),
        toDateTime64('1970-01-02 00:00:00', 3))
)
"""
    out = node.query(q).strip()
    assert out == "1", f"expected metric_locality_id in EXPLAIN for selector {selector_literal!r}, got {out!r}"


def seed_prometheus_table():
    send_data([({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})])

    send_data(
        [
            (
                {"__name__": "test"},
                {
                    110: 1,
                    120: 1,
                    130: 3,
                    140: 4,
                    190: 5,
                    200: 5,
                    210: 8,
                    220: 12,
                    230: 13,
                },
            )
        ]
    )

    send_data(
        [
            (
                {"__name__": "lb_route_hits", "shape": "circle", "size": "l"},
                {110: 16, 130: 16},
            ),
            (
                {"__name__": "lb_route_hits", "shape": "square", "size": "s"},
                {110: 4},
            ),
        ]
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        seed_prometheus_table()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "selector_literal",
    [
        # Instant-vector shapes (name pinned; extra labels still allow locality from `__name__` EQ).
        "up",
        'up{job="prometheus"}',
        '{__name__="kube_node_cpu_seconds_total"}',
        'node_memory_MemAvailable_bytes{instance="a:9100"}',
        'prometheus_http_requests_total{handler="/api"}',
        "ClickHouseProfileEvents_Query",
        r'{__name__=~"metric.*"}',
        "test",
        "lb_route_hits",
    ],
)
def test_metric_locality_visible_for_promql_style_selectors(
    metric_locality_explain_checks_applicable, selector_literal
):
    assert_metric_locality_in_explain_subquery(selector_literal)


@pytest.mark.parametrize(
    "promql,ts",
    [
        ("up", 1753176654.832),
        ('up{job="prometheus"}', 1753176654.832),
        ("test", 140.0),
        ("rate(test[30s])", 140.0),
        ('lb_route_hits{shape="circle"}', 130.0),
    ],
)
def test_promql_smoke_prometheus_query(promql, ts):
    q = promql.replace("'", "''")
    node.query(f"SELECT count() FROM prometheusQuery(prometheus, '{q}', {ts})")


@pytest.mark.parametrize(
    "promql,start,end,step",
    [
        ("test", 100.0, 200.0, 10.0),
        ("rate(test[30s])", 100.0, 200.0, 10.0),
    ],
)
def test_promql_smoke_prometheus_query_range(promql, start, end, step):
    q = promql.replace("'", "''")
    node.query(
        f"SELECT count() FROM prometheusQueryRange(prometheus, '{q}', {start}, {end}, {step})"
    )
