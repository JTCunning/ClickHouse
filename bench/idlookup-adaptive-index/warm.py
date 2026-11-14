#!/usr/bin/env python3
"""#20 on a local-disk server: the ids query of each selector x variant, interleaved, in the state `warm` (nothing dropped) or
`dropped` (ClickHouse caches dropped and primary keys unloaded before every run; the OS page cache stays, so it is a local-disk
cold, not an object-storage one). Prints one summary line per selector x variant (medians) and writes every run to OUT.

  warm.py <port> <state> <rounds> <qdir> <sel,...> [variant,...]
"""
import json, os, statistics, subprocess, sys, time, uuid

CH = '/root/nerve-workspace/tools/clickhouse/master/clickhouse'
BASE = {'join_algorithm': 'full_sorting_merge', 'optimize_read_in_order': '1', 'any_join_distinct_right_table_keys': '0',
        'merge_tree_min_bytes_for_concurrent_read': '4194304', 'max_threads': '16', 'max_threads_min_free_memory_per_thread': '0',
        'use_query_condition_cache': '0', 'max_memory_usage': '2500000000', 'log_queries': '1'}
VARIANTS = {
    'base': {},
    'qcc': {'use_query_condition_cache': '1'},
    'noidx': {'use_skip_indexes': '0'},
    'nosp': {'use_statistics_for_part_pruning': '0'},
    'nojo': {'query_plan_optimize_join_order_limit': '0'},
    'nostat': {'use_statistics': '0'},
    'combo': {'use_skip_indexes': '0', 'use_statistics_for_part_pruning': '0', 'query_plan_optimize_join_order_limit': '0'},
}
EVENTS = ['OSCPUVirtualTimeMicroseconds', 'RealTimeMicroseconds', 'LoadedStatistics', 'LoadedStatisticsMicroseconds',
          'FilteringMarksWithPrimaryKeyMicroseconds', 'FilteringMarksWithSecondaryKeysMicroseconds', 'TextIndexReadGranulesMicroseconds',
          'SelectedParts', 'SelectedMarks', 'SelectedRows']
DROPS = ['MARK CACHE', 'UNCOMPRESSED CACHE', 'TEXT INDEX CACHES', 'QUERY CONDITION CACHE', 'PRIMARY INDEX CACHE', 'INDEX MARK CACHE',
         'INDEX UNCOMPRESSED CACHE', 'STATISTICS CACHE', 'PAGE CACHE']


def run(port, sql, settings=None, qid=None):
    args = [CH, 'client', '--port', port, '-q', sql]
    for k, v in (settings or {}).items():
        args.append(f'--{k}={v}')
    if qid:
        args += ['--query_id', qid]
    return subprocess.run(args, check=True, capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout


def main():
    port, state, rounds, qdir, sels = sys.argv[1:6]
    variants = sys.argv[6].split(',') if len(sys.argv) > 6 else ['base', 'qcc', 'noidx', 'nosp', 'nojo', 'nostat', 'combo']
    out = os.environ.get('OUT', os.path.join(qdir, '..', f'results-warm-{state}.jsonl'))
    recs = []
    for r in range(int(rounds)):
        for sel in sels.split(','):
            q = open(os.path.join(qdir, f'{sel}.sql')).read().strip()
            for v in variants:
                s = dict(BASE, **VARIANTS[v])
                for kind, sql in (('query', q), ('plan', f'EXPLAIN indexes = 1 {q}')):
                    if state == 'dropped':
                        for c in DROPS:
                            run(port, f'SYSTEM DROP {c}')
                        run(port, 'SYSTEM UNLOAD PRIMARY KEY')
                    elif r == 0 and kind == 'query':
                        run(port, f'{q} FORMAT Null', s)  # prime
                    qid = f'w20-{sel}-{v}-{kind}-{uuid.uuid4().hex[:6]}'
                    run(port, f'{sql} FORMAT Null', s, qid)
                    recs.append({'sel': sel, 'variant': v, 'kind': kind, 'round': r, 'qid': qid, 'state': state})
    run(port, 'SYSTEM FLUSH LOGS')
    ids = ','.join(f"'{x['qid']}'" for x in recs)
    rows = run(port, f"SELECT query_id, query_duration_ms, read_rows, ProfileEvents FROM system.query_log WHERE type = 'QueryFinish' "
                     f"AND query_id IN ({ids}) FORMAT JSONEachRow")
    by = {}
    for line in rows.splitlines():
        j = json.loads(line)
        by[j['query_id']] = j
    with open(out, 'a') as f:
        for x in recs:
            j = by[x['qid']]
            x.update(wall_ms=int(j['query_duration_ms']), read_rows=int(j['read_rows']),
                     events={k: int(j['ProfileEvents'].get(k, 0)) for k in EVENTS})
            f.write(json.dumps(x) + '\n')
    med = lambda xs: statistics.median(xs) if xs else 0
    print(f"{'sel':4} {'variant':7} {'wall':>6} {'min-max':>11} {'cpu':>6} {'thread':>7} {'plan':>5} {'stats':>5} {'skidx':>6} {'parts':>5} {'marks':>6}")
    for sel in sels.split(','):
        for v in variants:
            qs = [x for x in recs if x['sel'] == sel and x['variant'] == v and x['kind'] == 'query']
            ps = [x for x in recs if x['sel'] == sel and x['variant'] == v and x['kind'] == 'plan']
            w = [x['wall_ms'] for x in qs]
            e = lambda k: med([x['events'][k] for x in qs])
            print(f"{sel:4} {v:7} {med(w):6.0f} {min(w):5d}-{max(w):<5d} {e('OSCPUVirtualTimeMicroseconds')/1000:6.0f} "
                  f"{e('RealTimeMicroseconds')/1000:7.0f} {med([x['wall_ms'] for x in ps]):5.0f} {e('LoadedStatistics'):5.0f} "
                  f"{e('FilteringMarksWithSecondaryKeysMicroseconds')/1000:6.1f} {e('SelectedParts'):5.0f} {e('SelectedMarks'):6.0f}")


if __name__ == '__main__':
    main()
