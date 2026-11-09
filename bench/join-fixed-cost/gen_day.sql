INSERT INTO default.bench (metric_name, tags, samples)
WITH
    number AS s,
    intDiv(s, 32) AS node,
    intDiv(s % 32, 8) AS cpu,
    ['user','system','idle','iowait','nice','irq','softirq','steal'][(s % 8) + 1] AS mode,
    [0.20, 0.06, 0.66, 0.03, 0.005, 0.002, 0.008, 0.015][(s % 8) + 1] AS frac
SELECT 'node_cpu_seconds_total',
       map('job', 'node-exporter', 'cluster', 'bench', 'instance', concat('node-', toString(node), ':9100'),
           'cpu', toString(cpu), 'mode', mode),
       arrayMap(i -> (toDateTime64(1704067200 + i * 15, 3), toFloat64(i * 15 * frac + (cityHash64(s, i) % 100) / 1000.)),
                range({d:UInt32} * 5760, ({d:UInt32} + 1) * 5760))
FROM numbers(320)
SETTINGS max_threads = 2, max_insert_threads = 1;
