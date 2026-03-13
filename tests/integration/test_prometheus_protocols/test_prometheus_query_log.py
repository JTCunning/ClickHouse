"""
Integration tests that assert Prometheus HTTP handler operations (remote write,
remote read, Query API, and query_range API) are reflected in system.query_log
and in rows/bytes columns.
"""

import urllib.parse

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    get_response_to_http_api,
    get_response_to_remote_read,
    send_protobuf_to_remote_write,
    extract_data_from_http_api_response,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus.xml",
        "configs/config.d/query_log.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data_to_node():
    """Send minimal test data to the ClickHouse node via remote write so a later PromQL query returns data."""
    time_series = [({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data_to_node()
        yield cluster
    finally:
        cluster.shutdown()


def test_remote_write_appears_in_query_log_with_written_rows():
    """
    After a remote write to /write, there should be at least one row in
    system.query_log with type = 'QueryFinish' and written_rows > 0.
    """
    node.query("SYSTEM FLUSH LOGS query_log")
    count_before = int(
        node.query(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND written_rows > 0"
        ).strip()
    )

    # Send a small remote write to the node
    time_series = [
        (
            {"__name__": "tracking_test_metric", "job": "test"},
            {1753176700.0: 42},
        )
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)

    node.query("SYSTEM FLUSH LOGS query_log")

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND written_rows > 0",
        f"{count_before + 1}\n",
        retry_count=30,
        sleep_time=1,
    )


def test_remote_read_appears_in_query_log_with_read_rows():
    """
    After a Prometheus remote read request to /read, there should be at least one
    row in system.query_log with type = 'QueryFinish', read_rows > 0, and read_bytes > 0.
    """
    node.query("SYSTEM FLUSH LOGS query_log")
    count_before = int(
        node.query(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0"
        ).strip()
    )

    read_request = convert_read_request_to_protobuf("up", 1753176650, 1753176760)
    get_response_to_remote_read(node.ip_address, 9093, "read", read_request)

    node.query("SYSTEM FLUSH LOGS query_log")

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0",
        f"{count_before + 1}\n",
        retry_count=30,
        sleep_time=1,
    )


def test_query_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus Query API (/api/v1/query) request, there should be a row in
    system.query_log with type = 'QueryFinish', read_rows > 0, and read_bytes > 0.
    Query API uses executeQuery internally, so it is logged there; we assert by count.
    """
    timestamp = 1753176757.89
    promql = "up"

    count_before = int(
        node.query(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0"
        ).strip()
    )

    escaped_query = urllib.parse.quote_plus(promql, safe="")
    url = f"http://{node.ip_address}:9093/api/v1/query?query={escaped_query}&time={timestamp}"
    response = get_response_to_http_api(url)
    extract_data_from_http_api_response(response)

    node.query("SYSTEM FLUSH LOGS query_log")

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0",
        f"{count_before + 1}\n",
        retry_count=30,
        sleep_time=1,
    )


def test_query_range_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus query_range API (/api/v1/query_range) request, there should
    be a row in system.query_log with type = 'QueryFinish', read_rows > 0, and
    read_bytes > 0. query_range uses executeQuery internally, so it is logged there.
    """
    count_before = int(
        node.query(
            "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0"
        ).strip()
    )

    escaped_query = urllib.parse.quote_plus("up", safe="")
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={escaped_query}&start=1753176650&end=1753176760&step=15"
    )
    response = get_response_to_http_api(url)
    extract_data_from_http_api_response(response)

    node.query("SYSTEM FLUSH LOGS query_log")

    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0",
        f"{count_before + 1}\n",
        retry_count=30,
        sleep_time=1,
    )
