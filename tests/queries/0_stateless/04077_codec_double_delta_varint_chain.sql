DROP TABLE IF EXISTS t_double_delta_varint;
CREATE TABLE t_double_delta_varint (x UInt64 CODEC(DoubleDelta, VarInt, ZSTD(1))) ENGINE = Memory;
INSERT INTO t_double_delta_varint VALUES (0), (15000), (30000), (45000), (60000);
SELECT x FROM t_double_delta_varint ORDER BY x;
DROP TABLE t_double_delta_varint;
