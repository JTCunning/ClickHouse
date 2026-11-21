-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `recent_samples_ttl_seconds` has no upper bound. Choosing the recent samples table for a selector over a table with a TTL
-- of 2^63 seconds or more compared the timestamps with `now - TTL` computed in Int64, a signed overflow (UBSan).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_ttl;
CREATE TABLE ts_ttl ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 9223372036854775808;
SELECT count() FROM timeSeriesSelector(currentDatabase(), 'ts_ttl', 'm', now() - 600, now(), 'samples');
DROP TABLE ts_ttl;
