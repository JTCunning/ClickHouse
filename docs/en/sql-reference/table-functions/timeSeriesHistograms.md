---
description: 'timeSeriesHistograms returns the histograms table used by table `db_name.time_series_table`
  whose table engine is the TimeSeries engine.'
sidebar_label: 'timeSeriesHistograms'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesHistograms
title: 'timeSeriesHistograms'
doc_type: 'reference'
---

`timeSeriesHistograms(db_name.time_series_table)` - Returns the [histograms](../../engines/table-engines/integrations/time-series.md#histograms-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](../../engines/table-engines/integrations/time-series.md) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries HISTOGRAMS histograms_table
```

The function also works if the _histograms_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries HISTOGRAMS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesHistograms(db_name.time_series_table);
SELECT * FROM timeSeriesHistograms('db_name.time_series_table');
SELECT * FROM timeSeriesHistograms('db_name', 'time_series_table');
```

## Computing quantiles from native histograms {#computing-quantiles}

Rows in the histograms table store [Prometheus native histogram](https://prometheus.io/docs/specs/native_histograms/) samples with the Prometheus bucket convention: at schema `s`, positive bucket `i` covers `(2^((i-1)/2^s), 2^(i/2^s)]`. The [quantileExponentialHistogram](../aggregate-functions/reference/quantileExponentialHistogram.md) aggregate function uses the lower-bound-closed grid `[2^(j/2^s), 2^((j+1)/2^s))`, so a stored bucket index `i` maps to the aggregate function's index `i - 1`, and the zero bucket maps to the sentinel index `-1`.

For cumulative (counter) histograms, subtract the earliest sample of a time window from the latest one per bucket, then aggregate:

```sql
WITH
    (
        SELECT argMin((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(db_name.time_series_table)
        WHERE id = ... AND timestamp BETWEEN ... AND ...
    ) AS first_sample,
    (
        SELECT argMax((positive_bucket_indexes, positive_bucket_counts, zero_count), timestamp)
        FROM timeSeriesHistograms(db_name.time_series_table)
        WHERE id = ... AND timestamp BETWEEN ... AND ...
    ) AS last_sample
SELECT quantileExponentialHistogram(<schema>, 0.9)(bucket_index, bucket_count)
FROM
(
    SELECT bucket.1 - 1 AS bucket_index, sum(bucket.2) AS bucket_count
    FROM
    (
        SELECT arrayJoin(arrayZip(last_sample.1, last_sample.2)) AS bucket
        UNION ALL
        SELECT arrayJoin(arrayZip(first_sample.1, arrayMap(c -> -c, first_sample.2)))
    )
    GROUP BY bucket.1
    UNION ALL
    SELECT -1, last_sample.3 - first_sample.3
);
```

This recipe covers positive buckets and the zero bucket for a single series with a fixed schema and no counter resets within the window. PromQL `histogram_quantile` over native histograms through the `/query` endpoints is not implemented yet; it works for classic `_bucket` histograms only.
