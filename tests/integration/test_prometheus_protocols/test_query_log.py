"""
Check that PromQL queries executed via the Prometheus HTTP API (e.g. /api/v1/query,
/api/v1/query_range) are fully logged to system.query_log: both QueryStart and
QueryFinish (or ExceptionWhileProcessing) must appear for each request.
"""
import pytest
import time

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
)


def execute_query_in_clickhouse_http_api(query, timestamp=None):
    return execute_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query",
        query,
        timestamp,
    )


def execute_range_query_in_clickhouse_http_api(query, start_time, end_time, step):
    return execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query_range",
        query,
        start_time,
        end_time,
        step,
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        # Send minimal data to ClickHouse so PromQL queries return something
        write_request = convert_time_series_to_protobuf(
            [({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})]
        )
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", write_request)
        yield cluster
    finally:
        cluster.shutdown()


def test_promql_queries_logged_to_query_log():
    """
    Run PromQL requests via the HTTP API, then verify system.query_log contains
    both QueryStart and QueryFinish for the same query_id (interface = 8 is PROMETHEUS).
    """
    # Run two PromQL requests (instant and range)
    execute_query_in_clickhouse_http_api("up", 1753176757.89)
    execute_range_query_in_clickhouse_http_api(
        "up", 1753176650, 1753176760, "5s"
    )

    # Allow query_log to flush
    time.sleep(1)

    # interface = 8 is ClientInfo::Interface::PROMETHEUS
    # Check that we have both QueryStart and QueryFinish for Prometheus interface
    start_rows = node.query(
        "SELECT query_id FROM system.query_log "
        "WHERE interface = 8 AND type = 'QueryStart' AND event_time >= now() - 10"
    )
    finish_rows = node.query(
        "SELECT query_id FROM system.query_log "
        "WHERE interface = 8 AND type = 'QueryFinish' AND event_time >= now() - 10"
    )

    start_ids = set(line.strip() for line in start_rows.strip().split("\n") if line.strip())
    finish_ids = set(line.strip() for line in finish_rows.strip().split("\n") if line.strip())

    # We ran 2 PromQL queries, so we expect at least 2 QueryStart and 2 QueryFinish
    assert len(start_ids) >= 2, (
        f"Expected at least 2 QueryStart entries for Prometheus (interface=8), got {len(start_ids)}. "
        f"Start query_ids: {start_ids}"
    )
    assert len(finish_ids) >= 2, (
        f"Expected at least 2 QueryFinish entries for Prometheus (interface=8), got {len(finish_ids)}. "
        f"Finish query_ids: {finish_ids}"
    )

    # Every query that started must have a finish entry (same query_id)
    missing_finishes = start_ids - finish_ids
    assert not missing_finishes, (
        f"QueryStart without QueryFinish (query_ids): {missing_finishes}. "
        "PromQL queries must be fully logged to system.query_log."
    )
