-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated` does not drop `TimeSeries` inner tables synchronously; deferred DROPs are rejected.

-- A selector that matches the whole metric filters the samples table by the primary-key range of the metric
-- and the equality on the first id component instead of `id IN <tags subquery>`. The tags subquery is executed
-- separately so that the tags of the series are registered for the PromQL evaluation. A selector with a matcher
-- that filters some series out keeps the id set.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_whole;
DROP TABLE IF EXISTS ts_recent;
DROP TABLE IF EXISTS ts_orphan;

CREATE TABLE ts_whole ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_whole (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT '-- bare selector: no id set in the plan, same rows as a direct read of the samples table';

SELECT plan LIKE '%IN subquery%' AS has_id_set, plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_whole, 'foo', 0, 1000)));

SELECT timestamp, value FROM timeSeriesSelector(ts_whole, 'foo', 0, 1000) ORDER BY timestamp, value;

SELECT '(direct read for comparison)';

SELECT timestamp, value FROM timeSeriesSamples(ts_whole) WHERE tupleElement(id, 1) = sipHash64('foo') ORDER BY timestamp, value;

SELECT '-- selector with a matcher that filters series out: the id set stays';

SELECT plan LIKE '%IN subquery%' AS has_id_set, plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_whole, 'foo{env="prod"}', 0, 1000)));

SELECT timestamp, value FROM timeSeriesSelector(ts_whole, 'foo{env="prod"}', 0, 1000) ORDER BY timestamp, value;

SELECT '-- the tags of the series are registered: PromQL evaluations over a bare selector group by tags';

SELECT * FROM prometheusQuery(ts_whole, 'foo', 250) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts_whole, 'sum(foo)', 250) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts_whole, 'sum by (env) (foo)', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts_whole, 'rate(foo[200s])', 300, 300, 100) ORDER BY ALL;

SELECT '-- the recent samples table is read the same way';

CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000 TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_recent (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(now64(3) - INTERVAL 3 MINUTE, 1.), (now64(3) - INTERVAL 2 MINUTE, 2.)]),
    ('foo', map('env', 'dev'), [(now64(3) - INTERVAL 2 MINUTE, 10.)]),
    ('bar', map('env', 'dev'), [(now64(3) - INTERVAL 2 MINUTE, 100.)]);

SELECT plan LIKE '%.inner_id.recentsamples.%' AS reads_recent, plan LIKE '%IN subquery%' AS has_id_set, plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_recent, 'foo', now() - INTERVAL 10 MINUTE, now())));

SELECT count(), sum(value) FROM timeSeriesSelector(ts_recent, 'foo', now() - INTERVAL 10 MINUTE, now());
SELECT tags, value FROM prometheusQuery(ts_recent, 'sum by (env) (foo)', now()) ORDER BY ALL;

SELECT '-- a sample without a row in the tags table is returned by the selector and rejected by the PromQL evaluation';

CREATE TABLE ts_orphan ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_orphan (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(200, 3), 2.)]);

-- A row written directly into the samples table without a row in the tags table.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_orphan) (id, timestamp, value) VALUES
    ((sipHash64('foo'), toUUID('11111111-1111-1111-1111-111111111111')), toDateTime64(150, 3), 100.);

SELECT count(), sum(value) FROM timeSeriesSelector(ts_orphan, 'foo', 0, 1000);
SELECT count(), sum(value) FROM timeSeriesSelector(ts_orphan, 'foo{env="prod"}', 0, 1000);

-- The lookback window at 300 covers the orphan sample at 150.
SELECT * FROM prometheusQuery(ts_orphan, 'sum(foo)', 300); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery(ts_orphan, 'sum(foo{env="prod"})', 300) ORDER BY ALL;

DROP TABLE ts_orphan;
DROP TABLE ts_recent;
DROP TABLE ts_whole;
