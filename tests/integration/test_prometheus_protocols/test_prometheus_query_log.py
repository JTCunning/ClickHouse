"""
Integration tests that assert Prometheus HTTP handler operations (remote write
and Query API) are reflected in system.query_log and in rows/bytes columns.

With the current implementation these tests are expected to fail, demonstrating
that inserts and selects via the Prometheus HTTP handler are not tracked in the
query log or in written_rows/read_rows as expected.
"""

import urllib.parse

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
    get_response_to_http_api,
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
    Fails with current implementation because remote write does not go through query_log.
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


def test_query_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus Query API request, there should be a row in system.query_log
    for that request with type = 'QueryFinish', read_rows > 0, and read_bytes > 0.
    We match by query pattern (PromQL "up") since the handler may not accept query_id in the URL.
    May fail if the implementation does not log the request or set read_rows/read_bytes.
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

    # Assert at least one new QueryFinish row with read metrics (our Query API call).
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0",
        f"{count_before + 1}\n",
        retry_count=30,
        sleep_time=1,
    )
