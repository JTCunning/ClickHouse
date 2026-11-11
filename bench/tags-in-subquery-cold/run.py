#!/usr/bin/env python3
"""Runs the target PromQL range query in a cache state and attributes every S3 GET to (table, file kind).

  run.py --label cold --state cold --latency-ms 40 --reps 5 [--settings 'k=v,k=v'] [--query query.sql]
States:
  cold      all caches dropped, primary keys unloaded: a node that has never read these parts
  meta      marks, primary keys and statistics stay loaded, the filesystem cache and the text index caches are dropped
            (James's post-prewarm worst case: same plan, data not resident)
  warm      nothing dropped (the run before primes it)
Writes one JSON line per run to results.jsonl in the data directory.
"""
import argparse, collections, json, os, re, time, urllib.error, urllib.parse, urllib.request, uuid

D = '/root/nerve-workspace/tools/clickhouse/data/tags-in-subquery-cold'
URL = 'http://127.0.0.1:8323/'
EVENTS = ['QueryPlanBuildMicroseconds', 'QueryPlanOptimizeMicroseconds', 'S3GetObject', 'ReadBufferFromS3InitMicroseconds',
          'ReadBufferFromS3Microseconds', 'ReadBufferFromS3Bytes', 'CachedReadBufferReadFromSourceBytes',
          'CachedReadBufferPredownloadedFromSourceBytes', 'CachedReadBufferReadFromCacheBytes', 'CachedReadBufferCacheWriteBytes',
          'SynchronousRemoteReadWaitMicroseconds', 'AsynchronousRemoteReadWaitMicroseconds', 'RemoteFSPrefetches',
          'WaitMarksLoadMicroseconds', 'MarkCacheHits', 'MarkCacheMisses', 'TextIndexReaderTotalMicroseconds',
          'TextIndexReadDictionaryBlocks', 'TextIndexReadPostings', 'TextIndexReadSparseIndexBlocks',
          'FilteringMarksWithPrimaryKeyMicroseconds', 'FilteringMarksWithSecondaryKeysMicroseconds', 'SelectedParts', 'SelectedMarks',
          'SelectedRows', 'OSCPUVirtualTimeMicroseconds', 'RealTimeMicroseconds', 'LoadedPrimaryIndexFiles', 'LoadedStatisticsMicroseconds',
          'PrimaryIndexCacheHits', 'PrimaryIndexCacheMisses', 'CreatedReadBufferOrdinary']


def ch(sql, settings=None, qid=None):
    params = {}
    if qid:
        params['query_id'] = qid
    params.update(settings or {})
    req = urllib.request.Request(URL + '?' + urllib.parse.urlencode(params), data=sql.encode())
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()[:500])


def keymap():
    """object key -> (table, part, file) from the s3 disk's local metadata files."""
    uuids = dict(l.split('\t') for l in ch("SELECT toString(uuid), name FROM system.tables WHERE database = 'default' FORMAT TSV").split('\n') if l)
    m = {}
    base = os.path.join(D, 'server/data/disks/s3/store')
    for root, _, files in os.walk(base):
        rel = os.path.relpath(root, base).split(os.sep)
        if len(rel) < 3:
            continue
        table = uuids.get(rel[1], rel[1])
        table = re.sub(r'^\.inner_id\.(\w+)\..*$', r'\1', table)
        for fn in files:
            try:
                lines = open(os.path.join(root, fn)).read().split('\n')
                n = int(lines[1].split('\t')[0])
                for l in lines[2:2 + n]:
                    m[l.split('\t')[1]] = (table, rel[2], urllib.parse.unquote(fn))
            except (ValueError, IndexError, UnicodeDecodeError):
                pass
    return m


def kind(fn):
    if fn.startswith('skp_idx_'):
        return 'text index ' + ('marks' if 'mrk' in fn else fn.split('.', 1)[1])
    if 'mrk' in fn:
        return 'marks'
    if fn == 'primary.cidx' or fn == 'primary.idx':
        return 'primary index'
    if fn.startswith('statistics'):
        return 'statistics'
    if fn.endswith('.bin'):
        return 'column data'
    return 'other'


def ckey(object_key):
    # the cache key is the hex of the reversed sipHash128 of the object key (computed by the server in main)
    return CKEYS[object_key]


CKEYS = {}


def drop(state):
    cmds = ['FILESYSTEM CACHE', 'UNCOMPRESSED CACHE', 'TEXT INDEX CACHES', 'QUERY CONDITION CACHE', 'PAGE CACHE', 'INDEX UNCOMPRESSED CACHE', 'QUERY CACHE']
    if state == 'cold':
        cmds += ['MARK CACHE', 'PRIMARY INDEX CACHE', 'INDEX MARK CACHE', 'STATISTICS CACHE']
    for c in cmds:
        ch('SYSTEM DROP ' + c)
    if state == 'cold':
        ch('SYSTEM UNLOAD PRIMARY KEY')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--label', required=True)
    ap.add_argument('--state', choices=['cold', 'meta', 'warm'], required=True)
    ap.add_argument('--keep', default='', help="meta only: comma list of 'table: kind' prefixes whose cached files stay (e.g. 'tags: text index')")
    ap.add_argument('--evict', default='', help="warm only: comma list of 'table: kind' prefixes evicted from the filesystem cache")
    ap.add_argument('--latency-ms', type=float, default=40)
    ap.add_argument('--reps', type=int, default=5)
    ap.add_argument('--settings', default='')
    ap.add_argument('--query', default=os.path.join(D, 'query.sql'))
    ap.add_argument('--out', default=os.path.join(D, 'results.jsonl'))
    ap.add_argument('--pre', default='', help='SQL run after the cache drop and before the timed query; its wall time is recorded as pre_ms')
    a = ap.parse_args()
    settings = dict(kv.split('=', 1) for kv in a.settings.split(',') if kv)
    settings.setdefault('max_threads', '16')
    settings.setdefault('max_threads_min_free_memory_per_thread', '0')
    q = open(a.query).read().strip() + ' FORMAT Null'
    km = keymap()
    rows = ch("SELECT k, lower(hex(reverse(sipHash128(k)))) FROM (SELECT arrayJoin(" + json.dumps(list(km)).replace('"', "'") + ") AS k) FORMAT TSV",
              {'max_query_size': '100000000'})
    CKEYS.update(l.split('\t') for l in rows.split('\n') if l)
    open(os.path.join(D, 's3root/.latency_ms'), 'w').write(str(a.latency_ms))
    keep = [k.strip() for k in a.keep.split(',') if k.strip()]
    evict = [k.strip() for k in a.evict.split(',') if k.strip()]
    for rep in range(a.reps):
        if a.state == 'cold' or (a.state == 'meta' and not keep):
            if a.state == 'meta':
                ch(q, settings)
            drop(a.state)
        else:
            ch(q, settings)
            ch(q, settings)
            sel = keep if a.state == 'meta' else evict
            cached = set(ch("SELECT DISTINCT key FROM system.filesystem_cache FORMAT TSV").split())
            for key, (tbl, part, fn) in km.items():
                if CKEYS.get(key) not in cached:
                    continue
                cls = f'{tbl}: {kind(fn)}'
                hit = any(cls.startswith(p) for p in sel)
                if (a.state == 'meta' and not hit) or (a.state == 'warm' and hit):
                    ch(f"SYSTEM DROP FILESYSTEM CACHE 's3_cache' KEY {ckey(key)}")
            if a.state == 'meta':
                for c in ['UNCOMPRESSED CACHE', 'QUERY CONDITION CACHE', 'PAGE CACHE', 'INDEX UNCOMPRESSED CACHE', 'QUERY CACHE']:
                    ch('SYSTEM DROP ' + c)
                if not any(p.startswith('tags: text index') for p in keep):
                    ch('SYSTEM DROP TEXT INDEX CACHES')
            elif any(p.startswith('tags: text index') for p in evict):
                ch('SYSTEM DROP TEXT INDEX CACHES')
        pre_ms = 0
        if a.pre:
            tp = time.time()
            ch(a.pre)
            pre_ms = round((time.time() - tp) * 1000)
        time.sleep(1.5)  # let background downloads of the previous run settle
        logpath = os.path.join(D, 'get.jsonl')
        start_off = os.path.getsize(logpath) if os.path.exists(logpath) else 0
        qid = f'{a.label}-{rep}-{uuid.uuid4().hex[:6]}'
        t0 = time.time()
        ch(q, settings, qid)
        t1 = time.time()
        time.sleep(1.5)
        ch('SYSTEM FLUSH LOGS')
        row = json.loads(ch(f"SELECT query_duration_ms, read_rows, read_bytes, memory_usage, ProfileEvents FROM system.query_log "
                            f"WHERE query_id = '{qid}' AND type = 'QueryFinish' FORMAT JSONEachRow"))
        ev = {k: int(row['ProfileEvents'].get(k, 0)) for k in EVENTS}
        gets = collections.Counter()
        gbytes = collections.Counter()
        span = {}
        in_query = 0
        with open(logpath) as f:
            f.seek(start_off)
            for line in f:
                g = json.loads(line)
                if g['t'] < t0 - 0.05:
                    continue
                if g['t'] <= t1:
                    in_query += 1
                tbl, part, fn = km.get(g['key'], ('?', '?', '?'))
                cls = f'{tbl}: {kind(fn)}'
                off = round((g['t'] - t0) * 1000)
                span[cls] = [min(span.get(cls, [off, off])[0], off), max(span.get(cls, [off, off])[1], off)]
                gets[f'{tbl}: {kind(fn)}'] += 1
                gbytes[f'{tbl}: {kind(fn)}'] += g['bytes']
        rec = {'label': a.label, 'state': a.state, 'keep': keep, 'evict': evict, 'latency_ms': a.latency_ms, 'rep': rep, 'settings': settings,
               'wall_ms': int(row['query_duration_ms']), 'pre_ms': pre_ms, 'read_rows': int(row['read_rows']), 'read_bytes': int(row['read_bytes']),
               'memory': int(row['memory_usage']), 'events': ev, 'gets_in_query': in_query,
               'gets_by_kind': dict(gets), 'get_bytes_by_kind': dict(gbytes), 'get_span_ms': span, 'query_id': qid}
        with open(a.out, 'a') as f:
            f.write(json.dumps(rec) + '\n')
        print(f"{a.label} rep {rep}: wall {rec['wall_ms']} ms (pre {pre_ms} ms), plan-opt {ev['QueryPlanOptimizeMicroseconds']/1000:.0f} ms, "
              f"GETs {ev['S3GetObject']} (log {in_query}/{sum(gets.values())}), from source {ev['CachedReadBufferReadFromSourceBytes']/1e6:.1f} MB "
              f"(predownload {ev['CachedReadBufferPredownloadedFromSourceBytes']/1e6:.1f}), read_bytes {rec['read_bytes']/1e6:.1f} MB", flush=True)


if __name__ == '__main__':
    main()
