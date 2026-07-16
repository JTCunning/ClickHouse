"""Prometheus remote-write helpers.

Adapted from ClickHouse's integration tests
(tests/integration/test_prometheus_protocols/prometheus_test_utils.py), so the
benchmark talks to the server exactly the way the upstream tests do. Only the
remote-write helpers the loader needs are kept, plus a millisecond-native
WriteRequest builder.
"""

import os
import sys

import requests
import snappy

# Vendored protobuf stubs (prompb + gogoproto) live next to this file.
sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "pb2"))
import prompb.remote_pb2 as remote_pb2  # noqa: E402
import prompb.types_pb2 as types_pb2  # noqa: E402


def build_write_request(series):
    """Build a remote_pb2.WriteRequest from an iterable of
    (labels: dict[str, str], samples: list[(timestamp_ms: int, value: float)]).

    Samples are emitted in the given order (the generator provides them sorted
    by timestamp, which keeps per-series appends in order).
    """
    write_request = remote_pb2.WriteRequest()
    for labels, samples in series:
        ts = types_pb2.TimeSeries()
        for name, value in labels.items():
            ts.labels.append(types_pb2.Label(name=name, value=value))
        for timestamp_ms, value in samples:
            ts.samples.append(types_pb2.Sample(timestamp=int(timestamp_ms), value=float(value)))
        write_request.timeseries.append(ts)
    return write_request


def serialize_remote_write(write_request_proto):
    """Serialize + snappy-compress a WriteRequest into the on-wire body."""
    return snappy.compress(write_request_proto.SerializeToString())


def post_remote_write(url, compressed_body):
    """POST an already-serialized+compressed remote-write body. Returns the response."""
    return requests.post(
        url,
        data=compressed_body,
        headers={
            "Content-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": "timeseries-benchmark",
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
    )


def check_remote_write_response(response):
    # The spec says the success status SHOULD be 204 No Content; some receivers
    # answer 200. Accept either.
    if response.status_code not in (requests.codes.no_content, requests.codes.ok):
        raise Exception(
            f"remote-write got unexpected status {response.status_code}: {response.text}"
        )
