-- Data for the timeSeriesFromGrid typed-write emulation (2026-11-22).
-- fg_dense / fg_null: 8000 rows x 300 Nullable(Float64) values (the VECTOR_GRID shape of a PromQL
-- range query 100..3090 step 10), with no NULL and with 50% NULL (sipHash-random positions).
DROP TABLE IF EXISTS fg_dense; DROP TABLE IF EXISTS fg_null;
CREATE TABLE fg_dense (v Array(Nullable(Float64))) ENGINE = Memory;
CREATE TABLE fg_null (v Array(Nullable(Float64))) ENGINE = Memory;
INSERT INTO fg_dense SELECT arrayMap(s -> toNullable((sipHash64(number, s) % 1000000) / 1000), range(300)) FROM numbers(8000);
INSERT INTO fg_null SELECT arrayMap(s -> if(sipHash64(number, s, 1) % 2 = 0, NULL, (sipHash64(number, s) % 1000000) / 1000), range(300)) FROM numbers(8000);
-- The TimeSeries table of tests/performance/promql_set_operator_presence_mask.xml (create + fill copied verbatim).
DROP TABLE IF EXISTS promql_set_operator_presence_mask_ts;
CREATE TABLE promql_set_operator_presence_mask_ts ENGINE = TimeSeries;
INSERT INTO promql_set_operator_presence_mask_ts (metric_name, tags, samples)
SELECT
    if(number < 4000, 'foo', 'bar'),
    map('instance', toString(number % 4000)),
    arrayMap(step -> (toDateTime64(100 + step * 10, 3), (sipHash64(number, step) % 1000000) / 1000), range(300))
FROM numbers_mt(8000)
UNION ALL
SELECT
    if(number < 4000, 'foo_sparse', 'bar_sparse'),
    map(
        'instance', toString(intDiv(number, 4) % 1000),
        'replica', toString(number % 4)),
    arrayMap(
        step -> (toDateTime64(100 + step * 10, 3), (sipHash64(number, step) % 1000000) / 1000),
        arrayFilter(step -> sipHash64(number, step) % 2 = 0, range(300)))
FROM numbers_mt(8000)
SETTINGS max_insert_threads = 8;
