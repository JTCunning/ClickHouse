-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the test needs compact parts with one compressed block per granule of a fixed size.

-- Compact parts compress every granule separately, so the ALP codec sees compressed blocks of exactly `index_granularity` values,
-- and runs its search for the encoding parameters on every block. The search stops evaluating a parameter pair as soon as the pair
-- cannot beat the best one found so far; it must still choose the same parameters as a full evaluation of every pair.

SET enable_alp_codec = 1;

SELECT '# Round trip';

-- Values of three kinds, so that some blocks are encoded, some have exceptions and some fall back to `ALP(RD)` or stay raw.
DROP TABLE IF EXISTS alp_small_blocks;
CREATE TABLE alp_small_blocks (i UInt32, d Float64 CODEC(ALP), s Float64 CODEC(ALP(STD)), f Float32 CODEC(ALP))
ENGINE = MergeTree ORDER BY i
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1Gi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO alp_small_blocks
SELECT number, x, x, toFloat32(x)
FROM (SELECT number, multiIf(number % 3 = 0, (10000 + number % 256) / 100, number % 3 = 1, toFloat64(number * 7), intHash64(number) / 3e17) AS x FROM numbers(20000));
SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 'alp_small_blocks' AND active;
SELECT count(),
    sum(bin(d) != bin(x)), sum(bin(s) != bin(x)), sum(bin(f) != bin(toFloat32(x)))
FROM (SELECT d, s, f, multiIf(i % 3 = 0, (10000 + i % 256) / 100, i % 3 = 1, toFloat64(i * 7), intHash64(i) / 3e17) AS x FROM alp_small_blocks);
DROP TABLE alp_small_blocks;

SELECT '# Compressed size of 256-value blocks';

-- 3-decimal and 2-decimal values in a narrow range, so that 256-value blocks compress. Several parameter pairs encode a sample of
-- such values without exceptions, but only some of them round-trip every value, so the compressed size shows whether the search
-- chose the same pairs as a full evaluation. A compact part reports no per-column sizes, hence one table per column.
DROP TABLE IF EXISTS alp_small_blocks_d3;
DROP TABLE IF EXISTS alp_small_blocks_d2;
CREATE TABLE alp_small_blocks_d3 (v Float64 CODEC(ALP)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1Gi', min_compress_block_size = 65536, max_compress_block_size = 1048576;
CREATE TABLE alp_small_blocks_d2 AS alp_small_blocks_d3;
INSERT INTO alp_small_blocks_d3 SELECT (100000 + intHash32(number) % 4096) / 1000 FROM numbers(25600);
INSERT INTO alp_small_blocks_d2 SELECT (10000 + intHash32(number) % 2000) / 100 FROM numbers(25600);
SELECT table, part_type, marks, data_compressed_bytes, data_uncompressed_bytes
FROM system.parts WHERE database = currentDatabase() AND table LIKE 'alp\\_small\\_blocks\\_%' AND active
ORDER BY table;
SELECT
    (SELECT sum(cityHash64(bin(v))) FROM alp_small_blocks_d3) = (SELECT sum(cityHash64(bin((100000 + intHash32(number) % 4096) / 1000))) FROM numbers(25600)),
    (SELECT sum(cityHash64(bin(v))) FROM alp_small_blocks_d2) = (SELECT sum(cityHash64(bin((10000 + intHash32(number) % 2000) / 100))) FROM numbers(25600));
DROP TABLE alp_small_blocks_d3;
DROP TABLE alp_small_blocks_d2;
