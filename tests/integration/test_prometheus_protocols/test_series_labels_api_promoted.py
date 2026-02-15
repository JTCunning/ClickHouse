"""Prometheus series/labels API with tags_to_columns (promoted tag columns)."""

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


def get_json_from_api(path):
    url = f"http://{node.ip_address}:9093{path}"
    response = requests.get(url)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("DROP TABLE IF EXISTS prometheus SYNC")
        node.query(
            "CREATE TABLE prometheus (host_col LowCardinality(String)) "
            "ENGINE=TimeSeries() SETTINGS tags_to_columns = {'host': 'host_col'}"
        )
        time_series = [
            (
                {"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"},
                {1000: 0.5, 1015: 0.6, 1030: 0.7},
            ),
        ]
        protobuf = convert_time_series_to_protobuf(time_series)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_includes_promoted_tag_name():
    data = get_json_from_api("/api/v1/labels")
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_host_from_promoted_column():
    data = get_json_from_api("/api/v1/label/host/values")
    assert "server1" in data


def test_series_includes_host_from_promoted_column():
    data = get_json_from_api("/api/v1/series")
    assert len(data) >= 1
    entry = next(e for e in data if e.get("__name__") == "cpu_usage")
    assert entry.get("host") == "server1"
    assert entry.get("datacenter") == "us-east"
