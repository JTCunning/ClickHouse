"""Tests for Prometheus HTTP API endpoints: /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values

Run locally with the in-tree server binary, e.g.:
  CLICKHOUSE_TESTS_SERVER_BIN_PATH=$PWD/build/programs/clickhouse pytest ...
"""

import urllib.parse

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """Send test data with known labels for testing series/labels/label-values endpoints."""
    # Label names must be in strict lexicographic order (TimeSeries / remote write validation).
    time_series = [
        (
            {"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "cpu_usage", "datacenter": "us-west", "host": "server2"},
            {1000: 0.3, 1015: 0.4, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "datacenter": "us-east", "host": "server1"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
        (
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100, 1015: 150, 1030: 200},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_json_from_api(path, params=None):
    """Make a GET request to the ClickHouse Prometheus API and return parsed JSON."""
    url = f"http://{node.ip_address}:9093{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    print(f"Requesting {url}")
    response = requests.get(url)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        # Wait for data to be available
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_returns_all_label_names():
    """GET /api/v1/labels should return all unique label names including __name__."""
    data = get_json_from_api("/api/v1/labels")
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_for_name():
    """GET /api/v1/label/__name__/values should return all metric names."""
    data = get_json_from_api("/api/v1/label/__name__/values")
    assert isinstance(data, list)
    assert "cpu_usage" in data
    assert "memory_usage" in data
    assert "http_requests_total" in data


def test_label_values_for_host():
    """GET /api/v1/label/host/values should return all host values."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" in data


def test_label_values_for_datacenter():
    """GET /api/v1/label/datacenter/values should return datacenter values."""
    data = get_json_from_api("/api/v1/label/datacenter/values")
    assert isinstance(data, list)
    assert "us-east" in data
    assert "us-west" in data


def test_series_returns_metric_labels():
    """GET /api/v1/series should return series with their full label sets."""
    data = get_json_from_api("/api/v1/series")
    assert isinstance(data, list)
    assert len(data) > 0

    # Each entry should be a dict with __name__ and other labels
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "memory_usage" in metric_names


def test_label_values_for_nonexistent_label():
    """GET /api/v1/label/nonexistent/values should return empty list."""
    data = get_json_from_api("/api/v1/label/nonexistent/values")
    assert isinstance(data, list)
    assert len(data) == 0


def test_series_with_promql_selector_host():
    """GET /api/v1/series with match[] selector filters by host."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{host="server1"}')],
    )
    assert isinstance(data, list)
    assert len(data) >= 1
    for entry in data:
        assert entry.get("host") == "server1"


def test_series_with_promql_selector_regex_and_ne():
    """GET /api/v1/series with match[] uses method and status matchers."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{method!="",status=~"2.."}')],
    )
    assert isinstance(data, list)
    assert len(data) >= 1
    names = {e["__name__"] for e in data if "__name__" in e}
    assert "http_requests_total" in names


def test_labels_with_match_selector():
    """labels endpoint narrows when match[] matches a subset of metrics."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", "cpu_usage")],
    )
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "method" not in data


def test_label_values_with_regex_match():
    """label values with match[] regex for host."""
    data = get_json_from_api(
        "/api/v1/label/host/values",
        params=[("match[]", '{host=~"server.*"}')],
    )
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" in data


def test_time_window_params_smoke():
    """start/end query params are accepted (time overlap uses min_time/max_time on tags)."""
    # Sample timestamps are ~1000s epoch; keep window around that range.
    data = get_json_from_api(
        "/api/v1/series",
        params=[("start", "500"), ("end", "2000")],
    )
    assert isinstance(data, list)


def test_sql_injection_safe():
    """Malicious-looking label names and match[] must not break the server."""
    bad_label = "foo'; DROP TABLE x;--"
    path = f"/api/v1/label/{urllib.parse.quote(bad_label, safe='')}/values"
    data = get_json_from_api(path)
    assert isinstance(data, list)
    assert data == []

    data2 = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", "foo' OR '1'='1")],
    )
    assert isinstance(data2, list)

