import pytest
import requests

from helpers.cluster import ClickHouseCluster

USER_CFG = ["configs/allow_experimental_time_series_table.xml"]
PINNED_CFG = ["configs/prometheus_handlers_pinned_table_on_main_http_port.xml"]
DEFERRED_CFG = ["configs/prometheus_handlers_deferred_table_on_main_http_port.xml"]
XML_DB_ONLY_CFG = ["configs/prometheus_handlers_xml_database_only_on_main_http_port.xml"]
OTHERDB_CFG = ["configs/prometheus_handlers_otherdb_database_on_main_http_port.xml"]

cluster = ClickHouseCluster(__file__)
node_pinned = cluster.add_instance(
    "node_pinned",
    main_configs=PINNED_CFG,
    user_configs=USER_CFG,
    stay_alive=True,
)
node_deferred = cluster.add_instance(
    "node_deferred",
    main_configs=DEFERRED_CFG,
    user_configs=USER_CFG,
    stay_alive=True,
)
node_xml_db = cluster.add_instance(
    "node_xml_db",
    main_configs=XML_DB_ONLY_CFG,
    user_configs=USER_CFG,
    stay_alive=True,
)
node_otherdb = cluster.add_instance(
    "node_otherdb",
    main_configs=OTHERDB_CFG,
    user_configs=USER_CFG,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for instance in (node_pinned, node_deferred, node_xml_db, node_otherdb):
            instance.query(
                "CREATE TABLE IF NOT EXISTS default.prometheus ENGINE = TimeSeries"
            )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def started_node_pinned(started_cluster):
    return node_pinned


@pytest.fixture(scope="module")
def started_node_deferred(started_cluster):
    return node_deferred


@pytest.fixture(scope="module")
def started_node_xml_db(started_cluster):
    return node_xml_db


@pytest.fixture(scope="module")
def started_node_otherdb(started_cluster):
    return node_otherdb


def _http_url(node, path):
    return "http://{}:8123{}".format(node.ip_address, path)


def _prometheus_headers(database=None, table=None):
    headers = {}
    if database is not None:
        headers["X-ClickHouse-Database"] = database
    if table is not None:
        headers["X-ClickHouse-Table"] = table
    return headers


def test_remote_write_uses_header_table_when_xml_omits_table(started_node_deferred):
    response = requests.post(
        _http_url(started_node_deferred, "/api/v1/write"),
        data=b"",
        headers=_prometheus_headers(database="default", table="prometheus"),
        timeout=10,
    )
    assert response.status_code in (200, 204), (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )


def test_remote_read_uses_header_table_when_xml_omits_table(started_node_deferred):
    response = requests.post(
        _http_url(started_node_deferred, "/api/v1/read"),
        data=b"",
        headers=_prometheus_headers(database="default", table="prometheus"),
        timeout=10,
    )
    assert response.status_code == 200, (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )


def test_query_api_uses_header_table_when_xml_omits_table(started_node_deferred):
    response = requests.get(
        _http_url(started_node_deferred, "/api/v1/query"),
        params={"query": "up"},
        headers=_prometheus_headers(database="default", table="prometheus"),
        timeout=10,
    )
    assert response.status_code == 200, (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )
    assert '"status":"success"' in response.text.replace(" ", ""), (
        "query_api on :8123 did not return PromQL JSON: {!r}".format(
            response.text[:200]
        )
    )


def test_full_fallback_to_default_prometheus(started_node_deferred):
    for path in ("/api/v1/write", "/api/v1/read"):
        response = requests.post(
            _http_url(started_node_deferred, path),
            data=b"",
            timeout=10,
        )
        expected_statuses = (200, 204) if path.endswith("/write") else (200,)
        assert response.status_code in expected_statuses, (
            "path {} got status {}, body[:200]={!r}".format(
                path, response.status_code, response.text[:200]
            )
        )

    response = requests.get(
        _http_url(started_node_deferred, "/api/v1/query"),
        params={"query": "up"},
        timeout=10,
    )
    assert response.status_code == 200, (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )
    assert '"status":"success"' in response.text.replace(" ", ""), (
        "expected PromQL JSON, got body[:200]={!r}".format(response.text[:200])
    )


def test_xml_table_wins_over_headers(started_node_pinned):
    response = requests.post(
        _http_url(started_node_pinned, "/api/v1/write"),
        data=b"",
        headers=_prometheus_headers(table="nonexistent"),
        timeout=10,
    )
    assert response.status_code in (200, 204), (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )


def test_header_database_with_xml_database_only(started_node_xml_db):
    response = requests.post(
        _http_url(started_node_xml_db, "/api/v1/write"),
        data=b"",
        headers=_prometheus_headers(table="prometheus"),
        timeout=10,
    )
    assert response.status_code in (200, 204), (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )


def test_header_database_overrides_xml_database(started_node_otherdb):
    response = requests.post(
        _http_url(started_node_otherdb, "/api/v1/write"),
        data=b"",
        headers=_prometheus_headers(database="default", table="prometheus"),
        timeout=10,
    )
    assert response.status_code in (200, 204), (
        "got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )


def test_header_with_dotted_table_name_is_treated_as_literal_table(started_node_deferred):
    response = requests.post(
        _http_url(started_node_deferred, "/api/v1/write"),
        data=b"",
        headers=_prometheus_headers(table="default.prometheus"),
        timeout=10,
    )
    assert response.status_code >= 400, (
        "expected failure for literal dotted table name, got status {}, body[:200]={!r}".format(
            response.status_code, response.text[:200]
        )
    )
