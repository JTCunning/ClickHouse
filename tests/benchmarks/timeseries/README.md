# TimeSeries Benchmark

A PromQL benchmark over the [TimeSeries](https://clickhouse.com/docs/engines/table-engines/special/time_series) table engine. It models a Kubernetes monitoring workload: the data generator emits a deterministic, kube-prometheus-stack-shaped corpus (cAdvisor, kube-state-metrics, and node-exporter metrics plus pre-computed mixin recording-rule series), and the 24 queries are taken verbatim from kube-prometheus-stack mixin recording rules and Compute-Resources dashboards.

The queries run through the `prometheusQuery` / `prometheusQueryRange` table functions via `clickhouse client`, like the other benchmarks in this directory. The corpus and query set are shared with the cross-system [timeseries-benchmark](https://github.com/JTCunning/timeseries-benchmark) harness, which runs the same PromQL against Prometheus, Mimir, and VictoriaMetrics over the HTTP API and compares results across systems.

## The corpus

Everything is derived from a seed (default 1729) through stable hashes: the same knobs always produce bit-identical samples. The time window is fixed and historical:

* start `1704067200` (2024-01-01 00:00:00 UTC), scrape interval 15 s, 30 days = 172800 steps, window end `1706659185`.

The query files hardcode absolute evaluation times inside this window, so load the corpus with the default `--start/--step/--days`.

Cardinality scales with `--scale` (pods per namespace):

| scale | series | samples (30 d) | approx. disk (default codecs) |
| ----- | ------ | -------------- | ----------------------------- |
| 1     | 6112   | ~1.06 billion  | ~5.9 GB (measured)            |
| 4     | 23032  | ~3.98 billion  | ~21 GB                        |
| 16    | 90712  | ~15.7 billion  | ~83 GB (measured)             |

## Server configuration

Loading uses the Prometheus remote-write protocol, which requires the `prometheus_api_v1` HTTP handler. Add this to the server configuration (e.g. `/etc/clickhouse-server/config.d/prometheus_api.xml`) and restart:

```xml
<clickhouse>
    <http_handlers>
        <rule>
            <url_prefix>/prometheus/api/v1</url_prefix>
            <handler>
                <type>prometheus_api_v1</type>
                <table>default.bench</table>
            </handler>
        </rule>
        <defaults/>
    </http_handlers>
</clickhouse>
```

The same handler also exposes the Prometheus HTTP query API (`/prometheus/api/v1/query`, `/query_range`) over the table, which is how the cross-system harness talks to ClickHouse; this benchmark only needs the write endpoint.

## Creating the table

The TimeSeries engine is experimental, so creating the table needs `allow_experimental_time_series_table` (the queries additionally need `allow_experimental_time_series_aggregate_functions`; both are in `settings.json`):

```bash
clickhouse client --allow_experimental_time_series_table=1 --queries-file init.sql
```

## Loading the data

`generate_and_load.py` generates remote-write frames on the fly (nothing is materialized on disk) and POSTs them to the handler in order. Dependencies: `protobuf`, `python-snappy`, `requests`.

```bash
./generate_and_load.py --scale 1 --url http://localhost:8123/prometheus/api/v1/write
```

Frame generation runs in parallel worker processes (`--workers`, default half the CPUs); POSTs are serial. As a reference point, scale=16 (~15.7 billion samples) loaded at ~2.4 million samples/sec on an 8-vCPU machine, about 1.75 hours; scale=1 takes proportionally less. `--dry-run` prints corpus stats without loading.

## The queries

16 instant queries evaluate 60 s before the window end (so `[5m]` rate windows are full interiors); 8 range queries cover four horizons. Do not mix horizons into one aggregate; they measure different things (hot head vs. cold full-window scans).

| files | tier | what it measures |
| ----- | ---- | ---------------- |
| `query_01`-`query_02` | t1 selectors | matcher + regex selector primitives |
| `query_03`-`query_08` | t2 recording rules | joins (`group_left`), `label_replace`, `or` |
| `query_09`-`query_16` | t3 dashboard panels | cluster Compute-Resources instant panels |
| `query_17`-`query_18` | t4 range 1 h, step 30 s | hot / recent head |
| `query_19`-`query_20` | t4 range 24 h, step 60 s | warm |
| `query_21`-`query_22` | t5 range 7 d, step 5 m | cold-path raw scans |
| `query_23`-`query_24` | t5 range 30 d, step 15 m | full-window cold scans |

Each file's header comment carries the original tier name, evaluation times, and the mixin source. Run them like the other benchmarks, applying `settings.json`:

```bash
clickhouse client --allow_experimental_time_series_table=1 --allow_experimental_time_series_aggregate_functions=1 --queries-file queries/query_01.sql
```

# List of known problems

* The TimeSeries engine and the PromQL surface (`prometheusQuery`, `prometheusQueryRange`, and the time-series aggregate functions) are experimental.

* `query_23` (`t5_30d_cpu_usage_by_namespace`, a 30-day `rate` scan) exceeds ~9.3 GiB of query memory at scale=16. Either run it with more memory (`--max_memory_usage=0`) or expect a `MEMORY_LIMIT_EXCEEDED` failure under a ~10 GB cap.

* An optional storage variant with `CODEC(DoubleDelta)` timestamps and `CODEC(Gorilla)` values compresses this corpus ~34x smaller than the default codecs (2.4 GB vs 83 GB at scale=16). It is a storage A/B experiment, not part of the query benchmark; see the [external harness](https://github.com/JTCunning/timeseries-benchmark) for the table definition.
