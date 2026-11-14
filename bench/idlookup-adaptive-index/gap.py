#!/usr/bin/env python3
"""#20: where the time goes between the end of the ids query's planning and `Creating set` (the 0.57/0.67 s gap of the capture).
Runs the full PromQL query once in a cache state of the cold S3 lab with trace logs sent to the client and the real-time
profiler on, then prints the log timeline of the ids subquery and the profiler samples of each phase, symbolized from the
binary.

  gap.py <state> <selector> [settings k=v,...]
"""
import collections, os, re, subprocess, sys, uuid

sys.path.insert(0, '/root/nerve-workspace/research/bench/tags-in-subquery-cold')
import run as lab  # noqa: E402  (the cold lab's cache-state helpers)

CH = '/root/nerve-workspace/tools/clickhouse/master/clickhouse'
T0, T1 = open(os.path.join(lab.D, 'time_range.txt')).read().split()
GENERIC = re.compile(r'^(poll|epoll|__|pthread|std::|syscall|clock_nanosleep|nanosleep|start_thread|clone|operator new|memcpy|'
                     r'DB::ThreadPool|ThreadPool|ThreadFromGlobal|DB::ThreadStatus|DB::SemaphoreGuard|DB::Exception|'
                     r'DB::PipelineExecutor::executeStep|DB::ExecutionThreadContext::executeTask|DB::ExecutingGraph|Poco::|'
                     r'DB::CurrentThread|DB::QueryProfiler|DB::Timer|DB::signal|DB::SignalHandler|DB::MemoryTracker|'
                     r'DB::\(anonymous namespace\)::sig|DB::ThreadPoolCallbackRunner|DB::threadPoolCallbackRunner|'
                     r'boost::|DB::AsyncTaskExecutor|DB::Fiber|DB::ExecutionThreadContext|DB::PipelineExecutor|'
                     r'DB::ConcurrencyControl|DB::CallbackRunner|DB::TaskTracker|DB::IProcessor::|DB::ISimpleTransform::)')


def main():
    state, sel = sys.argv[1], sys.argv[2]
    extra = dict(kv.split('=', 1) for kv in (sys.argv[3].split(',') if len(sys.argv) > 3 else []) if kv)
    q = (f"SELECT count() FROM prometheusQueryRange('ts', 'count({sel})', {int(T1) - 10800}, {T1}, 30)").replace('"', '\\"')
    q = q.replace("\\'", "'")
    open(os.path.join(lab.D, 's3root/.latency_ms'), 'w').write('50')
    s = {'max_threads': '16', 'max_threads_min_free_memory_per_thread': '0', 'use_query_condition_cache': '0',
         'query_profiler_real_time_period_ns': '5000000', 'query_profiler_cpu_time_period_ns': '0', 'log_processors_profiles': '1'}
    s.update(extra)
    if state == 'meta':
        lab.ch(q + ' FORMAT Null', {k: v for k, v in s.items() if not k.startswith('query_profiler')})
    if state in ('cold', 'meta'):
        lab.drop(state)
    qid = f'gap20-{uuid.uuid4().hex[:8]}'
    args = [CH, 'client', '--port', '9301', '--send_logs_level', 'trace', '--query_id', qid, '-q', q + ' FORMAT Null']
    args += [f'--{k}={v}' for k, v in s.items()]
    log = subprocess.run(args, capture_output=True, text=True, stdin=subprocess.DEVNULL).stderr.splitlines()
    rx = re.compile(r'\] (\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d\.\d{6}) \[ (\d+) \] \{[^}]*\} <(\w+)> ([^:]+): (.*)')
    ev = []
    for line in log:
        m = rx.search(line)
        if m:
            ts, tid, lvl, logger, msg = m.groups()
            h, mi, sec = ts.split(' ')[1].split(':')
            ev.append((int(h) * 3600 + int(mi) * 60 + float(sec), tid, logger, msg))
    t_start = ev[0][0]
    # phases: the ids query is the one after "StorageTimeSeriesSelector: Will execute query"
    marks = {}
    for t, tid, logger, msg in ev:
        key = None
        if logger == 'StorageTimeSeriesSelector' and msg.startswith('Will execute query'):
            key = 'selector query built'
        elif 'Creating set' in msg:
            key = 'Creating set'
        elif msg.startswith('Created Set'):
            key = 'Created Set'
        elif 'Optimized join order' in msg or 'Optimizing join order' in msg:
            key = 'join order'
        if key and key not in marks:
            marks[key] = t
    last_plan = max((t for t, tid, logger, msg in ev
                     if marks.get('selector query built', 0) <= t <= marks.get('Creating set', 1e18)
                     and re.search(r'(Selected \d+/\d+ parts|marks by primary key|has dropped|Statistics|statistics|join order|optimizeJoin)', msg)),
                    default=None)
    marks['last planning line before Creating set'] = last_plan
    print('timeline (ms from query start):')
    for k, v in sorted(marks.items(), key=lambda kv: kv[1] or 0):
        print(f'  {((v or t_start) - t_start) * 1000:8.1f}  {k}')
    if last_plan and 'Creating set' in marks:
        win = [(t, logger, msg) for t, tid, logger, msg in ev if last_plan < t < marks['Creating set']]
        print(f'gap: {(marks["Creating set"] - last_plan) * 1000:.0f} ms, {len(win)} log lines in it; by logger:',
              dict(collections.Counter(l for _, l, _ in win).most_common(8)))
        for t, logger, msg in win[:6]:
            print(f'    {(t - t_start) * 1000:8.1f} {logger}: {msg[:140]}')
    subprocess.run([CH, 'client', '--port', '9301', '-q', 'SYSTEM FLUSH LOGS'], check=True, stdin=subprocess.DEVNULL)
    # profiler samples per phase, symbolized: innermost frame that is not generic
    rows = subprocess.run([CH, 'client', '--port', '9301', '--allow_introspection_functions=1', '-q', f"""
        SELECT toUnixTimestamp64Micro(event_time_microseconds) / 1e6 AS t, thread_id,
               arrayMap(a -> demangle(addressToSymbol(a)), trace) AS st
        FROM system.trace_log WHERE query_id = '{qid}' AND trace_type = 'Real' FORMAT JSONEachRow"""],
        capture_output=True, text=True, check=True, stdin=subprocess.DEVNULL).stdout.splitlines()
    import json
    t_end = ev[-1][0]
    off = None
    phases = collections.defaultdict(collections.Counter)
    for line in rows:
        j = json.loads(line)
        t, frames = float(j['t']), j['st']
        frame = next((f for f in frames if f and not GENERIC.match(f)), frames[0] if frames else '?')
        frame = re.sub(r'\(.*', '', frame)[:110]
        # log timestamps are local wall time of the server; trace_log is UTC epoch: align by the day offset
        if off is None:
            off = t - (t % 86400) - (t_start - t_start % 86400)
        lt = t - off
        lt = lt % 86400
        if marks.get('selector query built') and lt < marks['selector query built']:
            ph = '0 before ids query'
        elif last_plan and lt <= last_plan:
            ph = '1 ids query planning'
        elif marks.get('Creating set') and lt <= marks['Creating set']:
            ph = '2 gap (planning end .. Creating set)'
        elif marks.get('Created Set') and lt <= marks['Created Set']:
            ph = '3 set build'
        else:
            ph = '4 after'
        phases[ph][frame] += 1
    for ph in sorted(phases):
        tot = sum(phases[ph].values())
        print(f'{ph}: {tot} samples (5 ms, all threads)')
        for fr, n in phases[ph].most_common(int(os.environ.get('TOP', '7'))):
            print(f'    {n:5d} {fr}')
    print('query_id', qid)


if __name__ == '__main__':
    main()
