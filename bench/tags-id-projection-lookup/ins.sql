DROP TABLE IF EXISTS ins_v11; DROP TABLE IF EXISTS ins_v12;
CREATE TABLE ins_v11 (`id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), INDEX tags_idx tags TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 100000000) ENGINE = ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192;
CREATE TABLE ins_v12 (`id` Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags)))), `metric_name` LowCardinality(String), `tags` Map(LowCardinality(String), String), INDEX tags_idx tags TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 100000000,
  PROJECTION tags_by_id (SELECT id ORDER BY id) WITH SETTINGS (index_granularity = 1, index_granularity_bytes = 8192)) ENGINE = ReplacingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS index_granularity = 8192, deduplicate_merge_projection_mode = 'rebuild';
SYSTEM STOP MERGES ins_v11; SYSTEM STOP MERGES ins_v12;
CREATE TEMPORARY TABLE src (metric_name String, tags Map(String, String)) ENGINE = Memory;
INSERT INTO src SELECT 'm' || toString(number % 50), map('instance', 'host-' || toString(number), 'job', 'job' || toString(number % 7), 'pod', 'pod-' || toString(number % 1000)) FROM numbers(17000);
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v11 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
INSERT INTO ins_v12 (metric_name, tags) SELECT * FROM src;
