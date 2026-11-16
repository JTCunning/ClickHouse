-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A regular expression made of literals, groups, optional groups and alternatives is a list of literal strings,
-- so it selects by the primary key like an equality matcher.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('req', {'job':'api'}, [(100, 1.)]),
    ('req_total', {'job':'api'}, [(100, 2.)]),
    ('req_totals', {'job':'api'}, [(100, 3.)]),
    ('xreq_total', {'job':'api'}, [(100, 4.)]),
    ('resp_ok', {'job':'api'}, [(100, 5.)]),
    ('resp_err', {'job':'api'}, [(100, 6.)]),
    ('resp_x', {'job':'api'}, [(100, 7.)]),
    ('req', {'job':'a'}, [(100, 8.)]),
    ('req', {'job':'ab'}, [(100, 9.)]);

SELECT '-- optional group in the metric name';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"req(_total)?", job="api"}', 300) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"req(?:_total)?", job="api"}', 300) ORDER BY value);

SELECT '-- group between literals';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"resp_(ok|err)"}', 300) ORDER BY value);

SELECT '-- product of groups';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"re(q|sp)_(total|ok)"}', 300) ORDER BY value);

SELECT '-- nested groups';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"re((q)|(sp))_((tot)(al)|ok)", job="api"}', 300) ORDER BY value);

SELECT '-- optional group in a label matcher';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, 'req{job=~"a(b)?"}', 300) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, 'req{job!~"a(b)?"}', 300) ORDER BY value);

SELECT '-- syntax outside the supported subset is evaluated as a regular expression';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(ts, '{__name__=~"req(_total)*", job="api"}', 300) ORDER BY value);
SELECT '-- the primary key is used';
SELECT sum(length(samples)) FROM timeSeriesSelector(ts, '{__name__=~"req(_total)?"}', 0, 2000, 'samples') SETTINGS force_primary_key = 1;
SELECT sum(length(samples)) FROM timeSeriesSelector(ts, '{__name__=~"re(q|sp)_(total|ok)"}', 0, 2000, 'samples') SETTINGS force_primary_key = 1;

DROP TABLE ts;
