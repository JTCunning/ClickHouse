#!/usr/bin/env python3
"""Synthetic Prometheus remote-write 1.0 load generator, standard library only.

Protobuf is encoded by hand (WriteRequest / TimeSeries / Label / Sample), and the
body is wrapped as a *literal-only* raw snappy block (valid snappy that needs no
compressor), so decompression on the server costs about a memcpy.

Usage:
  rw.py URL --series N --samples-per-series S --labels L --universe U --requests R
        [--keep-alive] [--repeat-ts]

Each request sends N series (taken round-robin from a universe of U series), each
with S samples 15 s apart. A series' timestamps move forward on every visit, unless
--repeat-ts (then the same samples are re-sent: dedup of samples, not only of tags).
Prints one line per request: seconds, then the summary (median/min/max, samples/s).
"""
import argparse, http.client, statistics, struct, sys, time, urllib.parse


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def field_bytes(num, payload):
    return varint((num << 3) | 2) + varint(len(payload)) + payload


def label(name, value):
    return field_bytes(1, name.encode()) + field_bytes(2, value.encode())


def sample(value, ts_ms):
    return b"\x09" + struct.pack("<d", value) + b"\x10" + varint(ts_ms)


def snappy_literal(data):
    out = bytearray(varint(len(data)))
    pos = 0
    while pos < len(data):
        chunk = data[pos:pos + 65536]
        n = len(chunk) - 1
        if n < 60:
            out.append(n << 2)
        elif n < 256:
            out += bytes([60 << 2, n])
        else:
            out += bytes([61 << 2]) + struct.pack("<H", n)
        out += chunk
        pos += len(chunk)
    return bytes(out)


def series_labels(idx, num_labels):
    # metric names: 100 families; per-series labels with mixed cardinality
    labels = [("__name__", f"metric_{idx % 100}"),
              ("instance", f"host-{idx // 100 % 1000}:9100"),
              ("job", f"job_{idx % 7}")]
    for k in range(num_labels - 3):
        labels.append((f"label_{k}", f"value_{(idx * (k + 3)) % (10 ** (1 + k % 4))}"))
    return sorted(labels)


def build_request(series_ids, samples_per_series, num_labels, ts0_ms, label_cache):
    body = bytearray()
    for sid in series_ids:
        lb = label_cache.get(sid)
        if lb is None:
            lb = b"".join(field_bytes(1, label(n, v)) for n, v in series_labels(sid, num_labels))
            label_cache[sid] = lb
        ss = b"".join(field_bytes(2, sample(float(sid + j), ts0_ms + j * 15000)) for j in range(samples_per_series))
        body += field_bytes(1, lb + ss)
    return snappy_literal(bytes(body)), len(body)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("url")
    p.add_argument("--series", type=int, default=2000)
    p.add_argument("--samples-per-series", type=int, default=1)
    p.add_argument("--labels", type=int, default=10)
    p.add_argument("--universe", type=int, default=20000)
    p.add_argument("--requests", type=int, default=20)
    p.add_argument("--repeat-ts", action="store_true")
    p.add_argument("--skip", type=int, default=0, help="requests excluded from the summary (warm-up)")
    p.add_argument("--ts0", type=int, default=1793000000000)
    p.add_argument("--id-offset", type=int, default=0, help="first series id (gives parallel senders disjoint series)")
    a = p.parse_args()

    u = urllib.parse.urlparse(a.url)
    conn = http.client.HTTPConnection(u.hostname, u.port, timeout=600)
    headers = {"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy",
               "X-Prometheus-Remote-Write-Version": "0.1.0"}
    label_cache = {}
    shards = max(1, a.universe // a.series)
    times = []
    raw_bytes = 0
    for r in range(a.requests):
        shard = r % shards
        visit = 0 if a.repeat_ts else r // shards
        ids = range(a.id_offset + shard * a.series, a.id_offset + shard * a.series + a.series)
        ts0 = a.ts0 + visit * a.samples_per_series * 15000
        body, raw = build_request(ids, a.samples_per_series, a.labels, ts0, label_cache)
        t = time.perf_counter()
        conn.request("POST", u.path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        dt = time.perf_counter() - t
        if resp.status not in (200, 204):
            print("HTTP", resp.status, data[:500], file=sys.stderr)
            sys.exit(1)
        print(f"req {r} {dt:.4f}s", flush=True)
        if r >= a.skip:
            times.append(dt)
            raw_bytes = raw
    med = statistics.median(times)
    samples = a.series * a.samples_per_series
    print(f"SUMMARY series={a.series} sps={a.samples_per_series} labels={a.labels} universe={a.universe} "
          f"n={len(times)} median={med*1000:.1f}ms min={min(times)*1000:.1f}ms max={max(times)*1000:.1f}ms "
          f"samples/s={samples/med:,.0f} series/s={a.series/med:,.0f} body_raw={raw_bytes/1024:.0f}KiB")


if __name__ == "__main__":
    main()
