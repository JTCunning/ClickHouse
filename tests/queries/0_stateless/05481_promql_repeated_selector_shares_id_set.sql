-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test counts the sets a query builds.

-- Selectors with the same matchers and time range in one PromQL query share one set of series ids:
-- it is built from the tags table once. Selectors with other matchers or another time range build their own.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map('job', concat('j', toString(number % 3)), 'pod', concat('p', toString(number))),
       arrayMap(i -> (toDateTime64(1000000 + i * 15, 3), toFloat64(number * 100 + i)), range(20))
FROM numbers(10);

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05481_same_twice';

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05481_same_three_times';

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} - m{pod="p1"} offset 1m', toDateTime64(1000285, 3))
SETTINGS log_comment = '05481_other_time_range';

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + ignoring(pod) m{pod="p4"}', toDateTime64(1000285, 3))
SETTINGS log_comment = '05481_other_matchers';

-- Without the primary-key analysis the sets are built in the pipeline of the query, where a set being built isn't waited for.
SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS use_index_for_in_with_subqueries = 0, max_threads = 1;

SELECT tags, value FROM prometheusQuery(ts, 'm{pod="p1"} + m{pod="p1"} + m{pod="p1"}', toDateTime64(1000285, 3))
SETTINGS use_index_for_in_with_subqueries = 0, max_threads = 2;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05481\_%'
ORDER BY event_time_microseconds;

DROP TABLE ts;
