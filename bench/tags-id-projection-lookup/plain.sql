DROP TABLE IF EXISTS tags_u;
CREATE TABLE tags_u (`id` Tuple(UInt64, UUID), `metric_name` LowCardinality(String),
  PROJECTION tags_by_id (SELECT id ORDER BY id) WITH SETTINGS (index_granularity = 1, index_granularity_bytes = 8192))
ENGINE = ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';
SYSTEM STOP MERGES tags_u;
INSERT INTO tags_u SELECT tuple(tupleElement(id, 1), CAST(tupleElement(id, 2), 'UUID')), metric_name FROM tags_p WHERE _part IN (SELECT name FROM system.parts WHERE table='tags_p' AND active AND rows > 100000) SETTINGS max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, max_threads = 1;
SELECT count(), (SELECT count() FROM system.parts WHERE table='tags_u' AND active) FROM tags_u;
CREATE TEMPORARY TABLE tmp (id Tuple(UInt64, UUID)) ENGINE = Memory;
INSERT INTO tmp SELECT id FROM tags_u WHERE cityHash64(id) % 35 = 0 LIMIT 8500;
INSERT INTO tmp SELECT tuple(sipHash64('new'), reinterpretAsUUID(sipHash128(toString(number)))) FROM numbers(8500);
SELECT count() FROM tags_u WHERE id IN (SELECT id FROM tmp) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 2;
SELECT count() FROM tags_u WHERE id IN (SELECT id FROM tmp) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 2;
SELECT count() FROM tags_u WHERE id IN (SELECT id FROM tmp) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 2;
SELECT count() FROM tags_u WHERE id IN (SELECT id FROM tmp) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 2;
SELECT count() FROM tags_u WHERE id IN (SELECT id FROM tmp) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, max_threads = 2;
