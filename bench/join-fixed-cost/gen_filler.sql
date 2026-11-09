-- 100,000 filler series of other metrics (4 samples each) so the tags table has a realistic size
INSERT INTO default.bench (metric_name, tags, samples)
WITH cityHash64(number) AS h
SELECT concat('container_metric_', toString(number % 40)),
       map('job','cadvisor','cluster','bench','instance', concat('node-', toString(h % 10), ':9100'),
           'namespace', concat('ns', toString(intDiv(h,10) % 20)), 'pod', concat('pod-', toString(number))),
       arrayMap(i -> (toDateTime64(1704067200 + i * 15, 3), toFloat64(i)), range(4))
FROM numbers(100000) SETTINGS max_threads = 2;
