import json, sys, statistics as st, collections
rows = collections.defaultdict(list)
for l in open(sys.argv[1]):
    r = json.loads(l); q, v, s = r['label'].split('/'); rows[(s, q, v)].append(r)
med = lambda xs: st.median(xs) if xs else 0
print('state q   v  n  wall  p.min p.max  cpu_ms  LoadedStat  SelParts SelMarks GETs  plan-opt')
for (s, q, v), rs in sorted(rows.items()):
    w = [r['wall_ms'] for r in rs]; e = lambda k: med([r['events'].get(k, 0) for r in rs])
    print(f"{s:5} {q} {v} {len(rs):2} {med(w):6.0f} {min(w):5} {max(w):5} {e('OSCPUVirtualTimeMicroseconds')/1000:7.0f} {e('LoadedStatistics'):6.0f} {e('SelectedParts'):8.0f} {e('SelectedMarks'):8.0f} {med([r['gets_in_query'] for r in rs]):5.0f} {e('QueryPlanOptimizeMicroseconds')/1000:7.0f}")
