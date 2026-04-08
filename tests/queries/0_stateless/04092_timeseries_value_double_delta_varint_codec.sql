SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS 04092_ts_ddvi_zstd;
CREATE TABLE 04092_ts_ddvi_zstd
(
    `timestamp` DateTime64(3) CODEC(DoubleDelta, LZ4),
    `value` Float64 CODEC(DoubleDeltaVarInt, ZSTD(1))
)
ENGINE = TimeSeries;

DROP TABLE 04092_ts_ddvi_zstd;

DROP TABLE IF EXISTS 04092_ts_ddvi_lz4;
CREATE TABLE 04092_ts_ddvi_lz4
(
    `timestamp` DateTime64(3) CODEC(DoubleDelta, LZ4),
    `value` Float64 CODEC(DoubleDeltaVarInt, LZ4)
)
ENGINE = TimeSeries;

DROP TABLE 04092_ts_ddvi_lz4;
