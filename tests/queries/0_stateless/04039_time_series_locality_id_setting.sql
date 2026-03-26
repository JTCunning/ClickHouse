SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS 04039_ts_legacy;
DROP TABLE IF EXISTS 04039_ts_locality;

SET allow_experimental_time_series_locality_id = 0;
CREATE TABLE 04039_ts_legacy (
    `instance` LowCardinality(String),
    `job` LowCardinality(String)
)
ENGINE = TimeSeries
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}, store_min_time_and_max_time = 0;

SET allow_experimental_time_series_locality_id = 1;
CREATE TABLE 04039_ts_locality (
    `instance` LowCardinality(String),
    `job` LowCardinality(String)
)
ENGINE = TimeSeries
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}, store_min_time_and_max_time = 0;

-- Explicit ids: DEFAULT id is not expanded per row inside a single INSERT block for inner tags,
-- so we materialize the same expressions TimeSeries uses (sipHash128 vs timeSeriesLocalityId).
INSERT INTO FUNCTION timeSeriesTags('04039_ts_legacy') (id, metric_name, instance, job, tags)
SELECT
    reinterpretAsUUID(sipHash128('http_requests_total', concat('instance-', toString(number)), 'api', map())),
    'http_requests_total',
    concat('instance-', toString(number)),
    'api',
    map()
FROM numbers(32);

INSERT INTO FUNCTION timeSeriesData('04039_ts_legacy') (id, timestamp, value)
SELECT
    id,
    toDateTime64('2020-01-01 00:00:00', 3) + toIntervalSecond(toUInt32OrZero(replaceRegexpOne(toString(instance), '^instance-', ''))),
    toFloat64(replaceRegexpOne(toString(instance), '^instance-', ''))
FROM timeSeriesTags('04039_ts_legacy');

INSERT INTO FUNCTION timeSeriesTags('04039_ts_locality') (id, metric_name, instance, job, tags)
SELECT
    reinterpretAsUUID(timeSeriesLocalityId(
        'http_requests_total',
        'instance',
        concat('instance-', toString(number)),
        'job',
        'api',
        map())),
    'http_requests_total',
    concat('instance-', toString(number)),
    'api',
    map()
FROM numbers(32);

INSERT INTO FUNCTION timeSeriesData('04039_ts_locality') (id, timestamp, value)
SELECT
    id,
    toDateTime64('2020-01-01 00:00:00', 3) + toIntervalSecond(toUInt32OrZero(replaceRegexpOne(toString(instance), '^instance-', ''))),
    toFloat64(replaceRegexpOne(toString(instance), '^instance-', ''))
FROM timeSeriesTags('04039_ts_locality');

-- More contiguous in primary-key space ⇔ fewer distinct high-64-bit slices of the serialized id (same integration test idea).
WITH
    (SELECT uniqExact(bitShiftRight(reinterpretAsUInt128(id), 64)) FROM timeSeriesData('04039_ts_legacy')) AS high_u64_legacy,
    (SELECT uniqExact(bitShiftRight(reinterpretAsUInt128(id), 64)) FROM timeSeriesData('04039_ts_locality')) AS high_u64_locality
SELECT (high_u64_locality = 1) AND (high_u64_legacy > high_u64_locality);

DROP TABLE IF EXISTS 04039_ts_legacy;
DROP TABLE IF EXISTS 04039_ts_locality;
