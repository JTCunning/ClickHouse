CREATE TABLE default.prometheus_uuid
(
    `metric_name` String,
    `tags` Map(String, String),
    `samples` Array(Tuple(
        DateTime64(3),
        Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SETTINGS version = 11, recent_samples_ttl_seconds = 345600, samples_bucket_step_seconds = 3600, recent_samples_bucket_step_seconds = 900
SAMPLES INNER COLUMNS
(
    `id` Tuple(
        UInt64,
        UUID),
    `samples` SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(
        timestamp DateTime64(3),
        value Float64))),
    `bucket` DateTime64(3) CODEC(Delta, T64, ZSTD(3)),
    `min_time` SimpleAggregateFunction(min, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
    `max_time` SimpleAggregateFunction(max, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
    CODEC `samples.timestamp` (Delta, T64, ZSTD(3)),
    CODEC `samples.value` (ALP, ZSTD(3))
)
SAMPLES INNER
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(toDateTime(bucket, 'UTC'))
ORDER BY (id, bucket)
SETTINGS index_granularity = 256, index_granularity_bytes = 524288
RECENT SAMPLES INNER COLUMNS
(
    `id` Tuple(
        UInt64,
        UUID),
    `samples` SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(
        timestamp DateTime64(3),
        value Float64))),
    `bucket` DateTime64(3) CODEC(Delta, T64, ZSTD(3)),
    `min_time` SimpleAggregateFunction(min, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
    `max_time` SimpleAggregateFunction(max, DateTime64(3)) CODEC(Delta, T64, ZSTD(3)),
    CODEC `samples.timestamp` (Delta, T64, ZSTD(3)),
    CODEC `samples.value` (ALP, ZSTD(3))
)
RECENT SAMPLES INNER
ENGINE = AggregatingMergeTree
PARTITION BY toStartOfInterval(toDateTime(bucket, 'UTC'), toIntervalHour(5))
ORDER BY (id, bucket)
TTL toDateTime(bucket) + toIntervalSecond(346500)
SETTINGS index_granularity = 256, index_granularity_bytes = 131072, ttl_only_drop_parts = 1
TAGS INNER COLUMNS
(
    `id` Tuple(
        UInt64,
        UUID) DEFAULT tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags))),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    INDEX tags_idx tags TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 100000000
)
TAGS INNER
ENGINE = ReplacingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS index_granularity = 8192
TIME RANGES INNER COLUMNS
(
    `id` Tuple(
        UInt64,
        UUID),
    `min_time` SimpleAggregateFunction(min, DateTime64(3)),
    `max_time` SimpleAggregateFunction(max, DateTime64(3))
)
TIME RANGES INNER
ENGINE = AggregatingMergeTree
ORDER BY id
METRIC FAMILIES INNER COLUMNS
(
    `metric_family` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRIC FAMILIES INNER
ENGINE = ReplacingMergeTree
ORDER BY metric_family
