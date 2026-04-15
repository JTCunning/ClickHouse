import pytest
import requests

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    extract_data_from_http_api_response,
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_reader=True,
    with_prometheus_receiver=True,
)


def send_to_clickhouse(time_series):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


# Same rows as input to FORMAT Prometheus and FORMAT OpenMetrics (OpenMetrics adds # UNIT when unit is
# non-empty and always ends with # EOF).
TIMESERIES_TEXT_EXPORT_QUERY = """
SELECT
    CAST(tags.metric_name AS String) AS name,
    data.value AS value,
    coalesce(CAST(metrics.help AS String), '') AS help,
    coalesce(CAST(metrics.type AS String), '') AS type,
    mapFilter((k, v) -> (k != '__name__'), tags.tags) AS labels,
    toUnixTimestamp64Milli(data.timestamp) AS timestamp,
    coalesce(CAST(metrics.unit AS String), '') AS unit
FROM timeSeriesData(prometheus) AS data
INNER JOIN timeSeriesTags(prometheus) AS tags ON data.id = tags.id
LEFT JOIN timeSeriesMetrics(prometheus) AS metrics ON tags.metric_name = metrics.metric_family_name
WHERE tags.metric_name = 'post_body_metric'
ORDER BY data.timestamp
"""


def _strip_openmetrics_only_lines(text: str) -> str:
    """Drop OpenMetrics-only lines so the remainder matches FORMAT Prometheus for the same SELECT."""
    lines = []
    for line in text.split("\n"):
        if line == "# EOF" or line.startswith("# UNIT "):
            continue
        lines.append(line)
    return "\n".join(lines).rstrip("\n") + "\n"


def _normalize_exposition_newlines(text: str) -> str:
    """Single trailing newline; Prometheus may emit an extra blank line after the last sample."""
    return text.rstrip() + "\n"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_to_clickhouse(
            [({"__name__": "post_body_metric", "job": "test"}, {1000.0: 1.0, 1001.0: 2.0})]
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_query_instant_get_matches_post_urlencoded():
    host, port = node.ip_address, 9093
    query = "post_body_metric"
    t = 1000
    get_data = execute_query_via_http_api(host, port, "/api/v1/query", query, timestamp=t)
    url = f"http://{host}:{port}/api/v1/query"
    post_resp = requests.post(
        url,
        data={"query": query, "time": str(t)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    post_data = extract_data_from_http_api_response(post_resp)
    assert get_data == post_data


def test_query_range_get_matches_post_urlencoded():
    host, port = node.ip_address, 9093
    query = "post_body_metric"
    start_s, end_s, step = 999, 1002, "1"
    get_data = execute_range_query_via_http_api(
        host, port, "/api/v1/query_range", query, start_s, end_s, step
    )
    url = f"http://{host}:{port}/api/v1/query_range"
    post_resp = requests.post(
        url,
        data={
            "query": query,
            "start": str(start_s),
            "end": str(end_s),
            "step": step,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    post_data = extract_data_from_http_api_response(post_resp)
    assert get_data == post_data


def test_timeseries_export_prometheus_and_openmetrics_text():
    """TEXT formats on the same TimeSeries-backed SELECT: Prometheus vs OpenMetrics (# UNIT, # EOF)."""
    prometheus_text = node.query(f"{TIMESERIES_TEXT_EXPORT_QUERY}\nFORMAT Prometheus")
    openmetrics_text = node.query(f"{TIMESERIES_TEXT_EXPORT_QUERY}\nFORMAT OpenMetrics")

    assert openmetrics_text.rstrip().endswith("# EOF")
    assert _normalize_exposition_newlines(
        _strip_openmetrics_only_lines(openmetrics_text)
    ) == _normalize_exposition_newlines(prometheus_text)
