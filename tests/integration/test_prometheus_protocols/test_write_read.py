import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_native_histograms_to_protobuf,
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_read,
    receive_protobuf_from_remote_read,
    send_protobuf_to_remote_write,
)
import re
import requests
import time


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    with_prometheus_reader=True,
    handle_prometheus_remote_write=(9093, "/write"),
    handle_prometheus_remote_read=(9093, "/read"),
)


# Waits until Prometheus scrapes some data and sends it to ClickHouse via the RemoteWrite protocol.
def wait_for_scraped_data():
    start_time = time.monotonic()
    assert_eq_with_retry(
        node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
    )
    elapsed = time.monotonic() - start_time
    data_num_rows = int(node.query("SELECT count() FROM timeSeriesData(prometheus)"))
    tags_num_rows = int(node.query("SELECT count() FROM timeSeriesTags(prometheus)"))
    metrics_num_rows = int(
        node.query("SELECT count() FROM timeSeriesMetrics(prometheus)")
    )
    print(f"After waiting {elapsed} seconds got numbers of rows:")
    print(
        f"data: {data_num_rows} rows, tags: {tags_num_rows} rows, metrics: {metrics_num_rows} rows"
    )


# Sends lots of data to ClickHouse via the RemoteWrite protocol.
def send_big_data(metric_name="big_data", start_time=1724112000, end_time=1724115600, count=75000):
    time_series = []
    step = (end_time - start_time) / count
    for i in range(0, count):
        timestamp = start_time + i * step
        value = i
        time_series.append(({"__name__": metric_name}, {timestamp: value}))
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


# Executes a query in the "prometheus_reader" service. This service uses the RemoteRead protocol to get data from ClickHouse.
def execute_query_in_prometheus_reader(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_ip["reader"],
        cluster.prometheus_port["reader"],
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in the "prometheus_writer" service. This service sends data to ClickHouse via the RemoteWrite protocol.
def execute_query_in_prometheus_writer(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_ip["writer"],
        cluster.prometheus_port["writer"],
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in both prometheus services - the results should be the same regardless of
# whether the data comes through ClickHouse or now.
def execute_query_in_prometheus(query, timestamp):
    r1 = execute_query_in_prometheus_reader(query, timestamp)
    r2 = execute_query_in_prometheus_writer(query, timestamp)
    assert r1 == r2
    return r1


# Executes a prometheus query in ClickHouse
def execute_query_in_clickhouse(query, timestamp):
    return node.query(
        f"SELECT * FROM prometheusQuery(prometheus, '{query}', {timestamp})"
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        wait_for_scraped_data()
        send_big_data()
        yield cluster
    finally:
        cluster.shutdown()


def test_handle_normal_scrape():
    query = "up"
    evaluation_time = time.time()
    result = execute_query_in_prometheus(query, evaluation_time)
    print(f"result={result}")
    pattern = '\\{"resultType": "vector", "result": \\[\\{"metric": \\{"__name__": "up", "instance": "localhost:9090", "job": "prometheus"}, "value": \\[[0-9]+(\\.[0-9]*)?, "1"]}]}'
    assert re.match(pattern, result)
    chresult = execute_query_in_clickhouse(query, evaluation_time)
    print(f"chresult={chresult}")
    chpattern = "\\[\\('__name__','up'\\),\\('instance','localhost:9090'\\),\\('job','prometheus'\\)]\t[^\t]*\t1\n"
    assert re.match(chpattern, chresult)


def test_remote_read_auth():
    read_request = convert_read_request_to_protobuf(
        "^up$", time.time() - 300, time.time()
    )
    print(f"read_request={read_request}")

    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "read_auth_ok",
        read_request,
    )
    print(f"read_response = {read_response}")
    assert len(read_response.results) > 0
    assert len(read_response.results[0].timeseries) > 0
    assert len(read_response.results[0].timeseries[0].samples) > 0

    auth_fail_response = get_response_to_remote_read(
        node.ip_address,
        9093,
        "read_auth_fail",
        read_request,
    )
    assert auth_fail_response.status_code == requests.codes.forbidden


def test_remote_write_native_histograms():
    # Two cumulative samples of an integer native histogram (delta-encoded buckets)
    # and one float native histogram (absolute bucket counts).
    protobuf = convert_native_histograms_to_protobuf(
        [
            (
                {"__name__": "native_hist_int"},
                [
                    {
                        "timestamp": 1724116000000,
                        "schema": 3,
                        "count_int": 150,
                        "sum": 1000.0,
                        "zero_threshold": 0.001,
                        "zero_count_int": 50,
                        "positive_spans": [(1, 2), (6, 1)],
                        # Deltas 40, 0, -20 decode to absolute counts 40, 40, 20
                        # at bucket indexes 1, 2, 9.
                        "positive_deltas": [40, 0, -20],
                    },
                    {
                        "timestamp": 1724116060000,
                        "schema": 3,
                        "count_int": 1550,
                        "sum": 9000.0,
                        "zero_threshold": 0.001,
                        "zero_count_int": 550,
                        "positive_spans": [(1, 2), (6, 1)],
                        "positive_deltas": [440, -100, -220],
                    },
                ],
            ),
            (
                {"__name__": "native_hist_float"},
                [
                    {
                        "timestamp": 1724116000000,
                        "schema": 0,
                        "count_float": 15.5,
                        "sum": 100.0,
                        "zero_threshold": 0.001,
                        "zero_count_float": 1.5,
                        "positive_spans": [(0, 2)],
                        "positive_counts": [10.0, 3.0],
                        "negative_spans": [(2, 1)],
                        "negative_counts": [1.0],
                    },
                ],
            ),
        ]
    )
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)

    result = node.query(
        "SELECT schema, count, sum, zero_threshold, zero_count,"
        " positive_bucket_indexes, positive_bucket_counts,"
        " negative_bucket_indexes, negative_bucket_counts"
        " FROM timeSeriesHistograms(prometheus) AS histograms"
        " JOIN timeSeriesTags(prometheus) AS tags ON histograms.id = tags.id"
        " WHERE tags.metric_name = 'native_hist_int'"
        " ORDER BY timestamp"
    )
    assert result == (
        "3\t150\t1000\t0.001\t50\t[1,2,9]\t[40,40,20]\t[]\t[]\n"
        "3\t1550\t9000\t0.001\t550\t[1,2,9]\t[440,340,120]\t[]\t[]\n"
    )

    result = node.query(
        "SELECT schema, count, sum, zero_threshold, zero_count,"
        " positive_bucket_indexes, positive_bucket_counts,"
        " negative_bucket_indexes, negative_bucket_counts"
        " FROM timeSeriesHistograms(prometheus) AS histograms"
        " JOIN timeSeriesTags(prometheus) AS tags ON histograms.id = tags.id"
        " WHERE tags.metric_name = 'native_hist_float'"
    )
    assert result == "0\t15.5\t100\t0.001\t1.5\t[0,1]\t[10,3]\t[2]\t[1]\n"


def test_remote_read_big_data():
    read_request = convert_read_request_to_protobuf(
        "^big_data$", 1724112000, 1724115600
    )

    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "read_auth_ok",
        read_request)

    assert len(read_response.results) == 1
    assert len(read_response.results[0].timeseries) == 1
    assert len(read_response.results[0].timeseries[0].samples) == 75000
