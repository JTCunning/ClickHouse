INSERT INTO tags_p (metric_name, tags) SELECT 'm' || toString(number % 50), map('instance', 'host-' || toString(number), 'job', 'job' || toString(number % 7)) FROM numbers(300000, 2700000) SETTINGS max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000;
SELECT count(), sum(rows) FROM system.parts WHERE table = 'tags_p' AND active;
SELECT sum(rows), sum(marks), formatReadableSize(sum(data_compressed_bytes)), formatReadableSize(sum(primary_key_bytes_in_memory)) FROM system.projection_parts WHERE table = 'tags_p' AND active;
SELECT formatReadableSize(sum(data_compressed_bytes)), formatReadableSize(sum(primary_key_bytes_in_memory)) FROM system.parts WHERE table = 'tags_p' AND active;
