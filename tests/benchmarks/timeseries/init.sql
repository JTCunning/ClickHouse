-- TimeSeries benchmark schema.
--
-- A single TimeSeries table with default inner-table settings and codecs.
-- Requires allow_experimental_time_series_table = 1 (see settings.json), e.g.:
--
--     clickhouse client --allow_experimental_time_series_table=1 --queries-file init.sql
CREATE TABLE bench ENGINE = TimeSeries;
