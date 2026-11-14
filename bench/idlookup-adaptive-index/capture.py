#!/usr/bin/env python3
"""Captures the ids query of a TimeSeries selector as master builds it (the `id IN (SELECT timeSeriesStoreTags(...) ...)` subquery
of the `Will execute query` debug line of StorageTimeSeriesSelector) and writes it to q/<name>.sql.

  capture.py <port> <table> <t0> <t1> <name>=<selector> ...
"""
import os, subprocess, sys

CH = '/root/nerve-workspace/tools/clickhouse/master/clickhouse'
D = os.path.dirname(os.path.abspath(__file__))


def ids_subquery(log):
    text = log.split('StorageTimeSeriesSelector: Will execute query:', 1)[1]
    start = text.index('(SELECT timeSeriesStoreTags(')
    depth = 0
    for i in range(start, len(text)):
        depth += {'(': 1, ')': -1}.get(text[i], 0)
        if depth == 0:
            return text[start + 1:i]
    raise RuntimeError('unbalanced')


def main():
    port, table, t0, t1 = sys.argv[1:5]
    os.makedirs(os.path.join(D, 'q'), exist_ok=True)
    for arg in sys.argv[5:]:
        name, sel = arg.split('=', 1)
        promql = f'count({sel})'.replace("'", "\\'")
        q = f"SELECT count() FROM prometheusQueryRange('{table}', '{promql}', {t0}, {t1}, 30) FORMAT Null"
        r = subprocess.run([CH, 'client', '--port', port, '--send_logs_level', 'debug', '-q', q], capture_output=True, text=True,
                           stdin=subprocess.DEVNULL, check=True)
        sql = ids_subquery(r.stderr)
        open(os.path.join(D, 'q', f'{name}.sql'), 'w').write(sql + '\n')
        print(f'{name}: {len(sql)} bytes')


if __name__ == '__main__':
    main()
