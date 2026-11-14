DROP TABLE IF EXISTS tags_p;
CREATE TABLE tags_p (`id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String),
  PROJECTION tags_by_id (SELECT id ORDER BY id) WITH SETTINGS (index_granularity = 1, index_granularity_bytes = 8192))
ENGINE = ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';
SYSTEM STOP MERGES tags_p;
INSERT INTO tags_p (metric_name, tags) SELECT 'm' || toString(number % 50), map('instance', 'host-' || toString(number), 'job', 'job' || toString(number % 7)) FROM numbers(300000) SETTINGS max_insert_block_size = 100000, min_insert_block_size_rows = 100000;
SELECT count(), uniqExact(id) FROM tags_p;
SELECT name, rows, marks FROM system.projection_parts WHERE table = 'tags_p' AND active;
SELECT name, rows, marks FROM system.parts WHERE table = 'tags_p' AND active;
