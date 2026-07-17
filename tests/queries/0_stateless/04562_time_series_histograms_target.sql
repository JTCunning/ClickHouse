-- Tags: no-replicated-database
-- ^^ DETACH/ATTACH of a TimeSeries table hangs in DatabaseReplicated mode
-- because DDL goes through the replicated log requiring replica sync
-- (same reason as 04146_timeseries_attach_detach).

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_04562;
DROP TABLE IF EXISTS ts_04562_renamed;
DROP TABLE IF EXISTS ts_04562_copy;
DROP TABLE IF EXISTS ts_04562_ext;
DROP TABLE IF EXISTS histograms_target_04562;
DROP TABLE IF EXISTS histograms_bad_target_04562;

SELECT 'default inner histograms table';
CREATE TABLE ts_04562 ENGINE = TimeSeries;
DESCRIBE TABLE timeSeriesHistograms(ts_04562);

SELECT 'insert and select through the table function';
-- The insert qualifies the database explicitly: with async inserts the query is executed
-- in a global context where the current database is not the test database.
INSERT INTO TABLE FUNCTION timeSeriesHistograms({CLICKHOUSE_DATABASE:Identifier}.ts_04562) VALUES
    ('9e39c66f-fbf2-4e0f-9078-df9d1e9930cb', '2024-01-01 00:00:00.000', 3, 100, 1234.5, 0.001, 5, [1, 2, 10], [40, 30, 20], [-3], [5], 0);
SELECT schema, count, sum, zero_threshold, zero_count, positive_bucket_indexes, positive_bucket_counts, negative_bucket_indexes, negative_bucket_counts, reset_hint
    FROM timeSeriesHistograms(ts_04562);

SELECT 'detach and attach keep the histograms target';
DETACH TABLE ts_04562;
ATTACH TABLE ts_04562;
SELECT count() FROM timeSeriesHistograms(ts_04562);

SELECT 'rename keeps the histograms target';
RENAME TABLE ts_04562 TO ts_04562_renamed;
SELECT count() FROM timeSeriesHistograms(ts_04562_renamed);

SELECT 'create table as gets a histograms target too';
CREATE TABLE ts_04562_copy AS ts_04562_renamed;
SELECT count() FROM timeSeriesHistograms(ts_04562_copy);
DROP TABLE ts_04562_copy;

SELECT 'truncate empties the histograms target';
TRUNCATE TABLE ts_04562_renamed;
SELECT count() FROM timeSeriesHistograms(ts_04562_renamed);
DROP TABLE ts_04562_renamed;

SELECT 'external histograms target';
CREATE TABLE histograms_target_04562
(
    id UUID,
    timestamp DateTime64(3),
    schema Int8,
    count Float64,
    sum Float64,
    zero_threshold Float64,
    zero_count Float64,
    positive_bucket_indexes Array(Int64),
    positive_bucket_counts Array(Float64),
    negative_bucket_indexes Array(Int64),
    negative_bucket_counts Array(Float64),
    reset_hint Int8
)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_04562_ext ENGINE = TimeSeries HISTOGRAMS histograms_target_04562;
INSERT INTO histograms_target_04562 (id, schema, count) VALUES ('9e39c66f-fbf2-4e0f-9078-df9d1e9930cb', 4, 7);
SELECT schema, count FROM timeSeriesHistograms(ts_04562_ext);
DROP TABLE ts_04562_ext;
DROP TABLE histograms_target_04562;

SELECT 'external histograms target must have the required columns';
CREATE TABLE histograms_bad_target_04562 (id UUID, timestamp DateTime64(3)) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_04562_ext ENGINE = TimeSeries HISTOGRAMS histograms_bad_target_04562; -- { serverError THERE_IS_NO_COLUMN }
DROP TABLE histograms_bad_target_04562;

SELECT 'external histograms target must have the required column types';
CREATE TABLE histograms_bad_target_04562
(
    id UUID,
    timestamp DateTime64(3),
    schema String,
    count Float64,
    sum Float64,
    zero_threshold Float64,
    zero_count Float64,
    positive_bucket_indexes Array(Int64),
    positive_bucket_counts Array(Float64),
    negative_bucket_indexes Array(Int64),
    negative_bucket_counts Array(Float64),
    reset_hint Int8
)
ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_04562_ext ENGINE = TimeSeries HISTOGRAMS histograms_bad_target_04562; -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE histograms_bad_target_04562;
