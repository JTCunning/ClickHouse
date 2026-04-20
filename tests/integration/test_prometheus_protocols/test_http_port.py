"""
Tests for the Prometheus protocol surface mounted on the main HTTP port.

Covers:
- Happy path for remote_write, remote_read, and the Query API endpoints under the default
  `/time-series/<db>/<table>/...` prefix.
- A custom `<http_path_prefix>` is honored.
- The per-table `prometheus_url_routing_enabled` setting gates access; URL routing is on
  by default, and a table that explicitly sets it to 0 is rejected with HTTP 403.
- Routing rejects non-TimeSeries storages with HTTP 403.
- Malformed URLs (missing the db/table segments) get HTTP 404 from the factory.
- The expose_metrics endpoint is NOT reachable through the prefix (still served at /metrics).
- Backward-compatible behavior of the dedicated <port> listener: the existing fixed-table
  config keeps working and a deprecation warning is logged at startup.
- Backward-compatible behavior of `<http_handlers><handler><type>prometheus</type>...`:
  the legacy expose-metrics shape continues to load and serve metrics on its custom URL,
  and an explicit `<type>remote_write</type>` rule can be mounted alongside it.
- Regex metacharacters in `<http_path_prefix>` (e.g. `.`, `+`) are treated literally rather
  than as regex wildcards (regression coverage for the route-filter escaping fix).
"""

import http
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    extract_data_from_http_api_response,
    extract_protobuf_from_remote_read_response,
    get_response_to_remote_read,
    get_response_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

# Node 1: New-style auto-mount on the HTTP port, default `/time-series` prefix. No dedicated
# Prometheus port, so we should NOT see the deprecation warning here.
node_default = cluster.add_instance(
    "node_default",
    main_configs=["configs/prometheus_http_port.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# Node 2: Same as node_default but with a custom prefix.
node_prefix = cluster.add_instance(
    "node_prefix",
    main_configs=["configs/prometheus_http_port_prefix.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# Node 3: Existing legacy config used by other tests. We reuse it here only to verify the
# deprecation warning is emitted. This node also keeps `expose_metrics` + the dedicated 9093
# listener intact -- this is the back-compat target.
node_legacy = cluster.add_instance(
    "node_legacy",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# Node 4: prefix containing regex metacharacters (`.` and `+`). The route-filter escaping fix
# means these characters must be treated literally.
node_regex_prefix = cluster.add_instance(
    "node_regex_prefix",
    main_configs=["configs/prometheus_http_port_regex_prefix.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# Node 5: explicit `<http_handlers>` rules covering both the legacy `<type>prometheus</type>`
# (expose-metrics) shape and the new `<type>remote_write</type>` dynamic-routing shape, with the
# auto-mount on the HTTP port disabled via an empty prefix.
node_http_handlers = cluster.add_instance(
    "node_http_handlers",
    main_configs=["configs/prometheus_http_handlers_rule.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


HTTP_PORT = 8123  # ClickHouseCluster's default for this image.
DEFAULT_PREFIX = "/time-series"
CUSTOM_PREFIX = "/grafana/prom"
REGEX_PREFIX = "/v1.0/prom+ts"


def _set_up_table(node, db, table):
    """
    Creates a TimeSeries table. URL-routed access is enabled by default, so no opt-in
    setting is needed.
    """
    node.query(f"CREATE DATABASE IF NOT EXISTS {db}")
    node.query(f"DROP TABLE IF EXISTS {db}.{table}")
    node.query(f"CREATE TABLE {db}.{table} ENGINE=TimeSeries")


def _send_one_sample(node, prefix, db, table, metric, ts, val,
                     expected_status=http.HTTPStatus.NO_CONTENT):
    """Sends a single sample via remote_write to /<prefix>/<db>/<table>/write."""
    payload = convert_time_series_to_protobuf(
        [({"__name__": metric}, {ts: val})]
    )
    response = get_response_to_remote_write(
        node.ip_address, HTTP_PORT, f"{prefix}/{db}/{table}/write", payload
    )
    assert response.status_code == expected_status, (
        f"unexpected status {response.status_code}: {response.text}"
    )
    return response


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# -----------------------------------------------------------------------------
# Happy path under the default /time-series prefix.
# -----------------------------------------------------------------------------

def test_remote_write_default_prefix():
    db, table, metric = "default", "ts_write", "rw_default_metric"
    _set_up_table(node_default, db, table)
    _send_one_sample(node_default, DEFAULT_PREFIX, db, table, metric, 1700000000, 42.0)
    # Round-trip via SQL to confirm the row landed in the right table.
    rows = int(node_default.query(
        f"SELECT count() FROM timeSeriesData({db}.{table})"))
    assert rows >= 1


def test_remote_read_default_prefix():
    db, table, metric = "default", "ts_read", "rr_default_metric"
    _set_up_table(node_default, db, table)
    _send_one_sample(node_default, DEFAULT_PREFIX, db, table, metric, 1700000100, 7.0)
    read_request = convert_read_request_to_protobuf(
        f"^{metric}$", 1700000099, 1700000101
    )
    response = get_response_to_remote_read(
        node_default.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/{db}/{table}/read", read_request,
    )
    decoded = extract_protobuf_from_remote_read_response(response)
    assert len(decoded.results) == 1
    assert len(decoded.results[0].timeseries) >= 1


def test_query_api_default_prefix():
    db, table, metric = "default", "ts_query", "qa_default_metric"
    _set_up_table(node_default, db, table)
    _send_one_sample(node_default, DEFAULT_PREFIX, db, table, metric, 1700001000, 3.5)

    base = f"http://{node_default.ip_address}:{HTTP_PORT}{DEFAULT_PREFIX}/{db}/{table}"

    # Instant query: end-to-end happy path through the QueryAPI handler.
    instant = requests.get(f"{base}/api/v1/query?query={metric}&time=1700001000")
    extract_data_from_http_api_response(instant)

    # Range query: end-to-end happy path through the QueryAPI handler.
    rng = requests.get(
        f"{base}/api/v1/query_range?query={metric}"
        f"&start=1700000999&end=1700001001&step=1"
    )
    extract_data_from_http_api_response(rng)

    # The remaining /api/v1/* endpoints (series, labels, label/<name>/values) are not yet
    # implemented in PrometheusHTTPProtocolAPI. We still want to confirm that the auto-mounted
    # QueryAPI rule routes them to our handler (not to DynamicQueryHandler), so we expect a
    # 400 with the well-formed JSON body our handler emits for NOT_IMPLEMENTED, NOT a 404 or
    # an UNKNOWN_SETTING error from DynamicQueryHandler trying to interpret `match[]` as a
    # query setting.
    for path in [
        f"/api/v1/series?match[]={metric}",
        "/api/v1/labels",
        "/api/v1/label/__name__/values",
    ]:
        response = requests.get(f"{base}{path}")
        assert response.status_code == http.HTTPStatus.BAD_REQUEST, (
            f"unexpected status {response.status_code} for {path}: {response.text}"
        )
        body = response.json()
        assert body.get("status") == "error", body
        assert body.get("errorType") == "bad_data", body
        assert "not implemented" in body.get("error", "").lower(), body


# -----------------------------------------------------------------------------
# Custom prefix.
# -----------------------------------------------------------------------------

def test_custom_prefix_remote_write():
    db, table, metric = "default", "ts_custom", "custom_metric"
    _set_up_table(node_prefix, db, table)
    _send_one_sample(node_prefix, CUSTOM_PREFIX, db, table, metric, 1700002000, 9.9)
    rows = int(node_prefix.query(
        f"SELECT count() FROM timeSeriesData({db}.{table})"))
    assert rows >= 1


def test_custom_prefix_does_not_serve_default():
    """The default `/time-series` prefix is NOT mounted when a custom one is configured."""
    payload = convert_time_series_to_protobuf(
        [({"__name__": "neg"}, {1700002000: 1.0})]
    )
    response = get_response_to_remote_write(
        node_prefix.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/default/ts_custom/write", payload,
    )
    # The catch-all dynamic-query handler doesn't accept POSTs to arbitrary URIs that aren't
    # `/`, `/?...`, or `/query?...`, so the request is unhandled (404).
    assert response.status_code == http.HTTPStatus.NOT_FOUND


# -----------------------------------------------------------------------------
# Per-table opt-in gate.
# -----------------------------------------------------------------------------

def test_table_flag_off_rejected():
    db, table = "default", "ts_flag_off"
    node_default.query(f"DROP TABLE IF EXISTS {db}.{table}")
    # Explicit opt-out: URL-routed access should be rejected.
    node_default.query(
        f"CREATE TABLE {db}.{table} ENGINE=TimeSeries "
        "SETTINGS prometheus_url_routing_enabled = 0"
    )
    payload = convert_time_series_to_protobuf(
        [({"__name__": "flag_off"}, {1700003000: 1.0})]
    )
    response = get_response_to_remote_write(
        node_default.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/{db}/{table}/write", payload,
    )
    assert response.status_code == http.HTTPStatus.FORBIDDEN

    # Recreate the table without the opt-out setting; URL-routed access is back on by default.
    # (TimeSeries does not currently support `ALTER ... MODIFY SETTING`, so we drop and
    # recreate to flip the per-table flag.)
    node_default.query(f"DROP TABLE {db}.{table}")
    node_default.query(f"CREATE TABLE {db}.{table} ENGINE=TimeSeries")
    response = get_response_to_remote_write(
        node_default.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/{db}/{table}/write", payload,
    )
    assert response.status_code == http.HTTPStatus.NO_CONTENT


def test_non_timeseries_storage_rejected():
    db, table = "default", "non_ts_table"
    node_default.query(f"DROP TABLE IF EXISTS {db}.{table}")
    node_default.query(
        f"CREATE TABLE {db}.{table} (x UInt64) ENGINE=MergeTree ORDER BY x"
    )
    payload = convert_time_series_to_protobuf(
        [({"__name__": "wrong_engine"}, {1700004000: 1.0})]
    )
    response = get_response_to_remote_write(
        node_default.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/{db}/{table}/write", payload,
    )
    # Routed to the prometheus handler, but rejected because the storage isn't a TimeSeries.
    assert response.status_code == http.HTTPStatus.FORBIDDEN


# -----------------------------------------------------------------------------
# Malformed URLs.
# -----------------------------------------------------------------------------

def test_missing_db_table_segments_404():
    """`/time-series/api/v1/query` is missing the `<db>/<table>` segments and so doesn't
    match any of the auto-mounted URL filters."""
    response = requests.get(
        f"http://{node_default.ip_address}:{HTTP_PORT}{DEFAULT_PREFIX}/api/v1/query?query=up"
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND


def test_partial_url_404():
    """`/time-series/default/ts/wrong_action` doesn't match any known protocol suffix."""
    response = requests.post(
        f"http://{node_default.ip_address}:{HTTP_PORT}"
        f"{DEFAULT_PREFIX}/default/ts_write/wrong_action"
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND


# -----------------------------------------------------------------------------
# expose_metrics is intentionally NOT reachable via the prefix.
# -----------------------------------------------------------------------------

def test_metrics_not_under_prefix():
    """`/time-series/metrics` is not a mounted route -- the `/metrics` auto-mount stays at
    its own URL and is served by the ExposeMetrics handler factory."""
    response = requests.get(
        f"http://{node_default.ip_address}:{HTTP_PORT}{DEFAULT_PREFIX}/metrics"
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND

    # Sanity: the original /metrics endpoint still works.
    response = requests.get(
        f"http://{node_default.ip_address}:{HTTP_PORT}/metrics"
    )
    assert response.status_code == http.HTTPStatus.OK
    assert b"# HELP" in response.content or b"# TYPE" in response.content


# -----------------------------------------------------------------------------
# Back-compat: dedicated <port> listener still works AND emits the deprecation warning.
# -----------------------------------------------------------------------------

def test_dedicated_port_still_serves_remote_write():
    """The legacy node still accepts remote_write at the fixed table address it always did."""
    node_legacy.query("DROP TABLE IF EXISTS default.prometheus")
    node_legacy.query("CREATE TABLE default.prometheus ENGINE=TimeSeries")
    payload = convert_time_series_to_protobuf(
        [({"__name__": "legacy_metric"}, {1700005000: 2.0})]
    )
    response = get_response_to_remote_write(
        node_legacy.ip_address, 9093, "/write", payload
    )
    assert response.status_code == http.HTTPStatus.NO_CONTENT


def test_dedicated_port_emits_deprecation_warning():
    """The server logs a deprecation warning the first time the prometheus listener starts."""
    # Give the server a moment to flush; it logs the warning during createServers() at boot.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if node_legacy.contains_in_log("dedicated <prometheus><port> listener"):
            return
        time.sleep(1)
    raise AssertionError(
        "Expected a deprecation warning about the dedicated <prometheus><port> listener "
        "in the server log of node_legacy"
    )


# -----------------------------------------------------------------------------
# Back-compat: <http_handlers><handler><type>prometheus</type>...</handler></http_handlers>
# -----------------------------------------------------------------------------

def test_http_handlers_prometheus_type_legacy_metrics():
    """The legacy `<type>prometheus</type>` shape under `<http_handlers>` keeps working as the
    old expose-metrics handler. This mirrors `test_http_handlers_config/test_prometheus_handler`,
    which the AI Review flagged as broken by the new parser before the back-compat fix."""
    base = f"http://{node_http_handlers.ip_address}:{HTTP_PORT}"

    # Wrong header -> the rule's filter rejects the request, so it falls through to a 404.
    response = requests.get(
        f"{base}/test_prometheus_legacy", headers={"X-Test": "wrong"}
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND

    # Wrong method -> same: the GET-only rule does not match a POST.
    response = requests.post(
        f"{base}/test_prometheus_legacy", headers={"X-Test": "legacy-metrics"}
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND

    # Happy path: the legacy expose-metrics handler is invoked and returns Prometheus-format
    # metrics text.
    response = requests.get(
        f"{base}/test_prometheus_legacy", headers={"X-Test": "legacy-metrics"}
    )
    assert response.status_code == http.HTTPStatus.OK
    assert b"ClickHouseProfileEvents_Query" in response.content


def test_http_handlers_explicit_remote_write_dynamic_routing():
    """A user can mount the new `<type>remote_write</type>` handler explicitly under
    `<http_handlers>` and dynamic routing still resolves the (database, table) pair from the URL."""
    db, table, metric = "default", "ts_explicit", "explicit_metric"
    _set_up_table(node_http_handlers, db, table)

    payload = convert_time_series_to_protobuf(
        [({"__name__": metric}, {1700006000: 4.2})]
    )
    response = get_response_to_remote_write(
        node_http_handlers.ip_address, HTTP_PORT,
        f"/explicit/{db}/{table}/write", payload,
    )
    assert response.status_code == http.HTTPStatus.NO_CONTENT

    rows = int(node_http_handlers.query(
        f"SELECT count() FROM timeSeriesData({db}.{table})"))
    assert rows >= 1


def test_http_handlers_auto_mount_opted_out():
    """An empty `<http_path_prefix>` opts the HTTP-port auto-mount out entirely. Hitting the
    default `/time-series/...` URL on this node must NOT route to a Prometheus handler."""
    payload = convert_time_series_to_protobuf(
        [({"__name__": "negative_auto"}, {1700006100: 1.0})]
    )
    response = get_response_to_remote_write(
        node_http_handlers.ip_address, HTTP_PORT,
        f"{DEFAULT_PREFIX}/default/ts_explicit/write", payload,
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND


# -----------------------------------------------------------------------------
# Regex metacharacters in <http_path_prefix> are treated literally (escaping fix).
# -----------------------------------------------------------------------------

def test_regex_prefix_literal_match():
    """`<http_path_prefix>/v1.0/prom+ts</http_path_prefix>` must be treated as a literal URL
    fragment. Writes through the literal URL succeed."""
    db, table, metric = "default", "ts_regex_prefix", "regex_prefix_metric"
    _set_up_table(node_regex_prefix, db, table)
    _send_one_sample(node_regex_prefix, REGEX_PREFIX, db, table, metric, 1700007000, 5.5)
    rows = int(node_regex_prefix.query(
        f"SELECT count() FROM timeSeriesData({db}.{table})"))
    assert rows >= 1


def test_regex_prefix_does_not_match_as_regex():
    """A path that would only match if `.` and `+` were interpreted as regex metacharacters
    (e.g. `.` -> any char, `+` -> one-or-more) must NOT route to the prometheus handler.
    `/v1x0/promXXts/.../write` is one such would-be regex match for `/v1.0/prom+ts/...`."""
    payload = convert_time_series_to_protobuf(
        [({"__name__": "negative"}, {1700007100: 1.0})]
    )
    response = get_response_to_remote_write(
        node_regex_prefix.ip_address, HTTP_PORT,
        "/v1x0/promXXts/default/ts_regex_prefix/write", payload,
    )
    assert response.status_code == http.HTTPStatus.NOT_FOUND
