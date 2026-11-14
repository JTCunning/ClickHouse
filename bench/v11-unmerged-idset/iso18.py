"""The tags side of the branch alone: SELECT count() FROM (tags subquery), skip indexes at read time (branch) vs in the index analysis (fix)."""
import json, sys, uuid
sys.path.insert(0, "/root/nerve-workspace/research/bench/v11-unmerged-idset")
import idsq
TABLE, ROUNDS = sys.argv[1], int(sys.argv[2])
idsq.TABLE = TABLE
tags, _ = idsq.tables()
rows = []
for ci in (2, 1):
    case = list(idsq.CASES)[ci]
    m = f"and(equals(metric_name, '{idsq.NAME}'), {idsq.CASES[case]})"
    base = f"SELECT count() FROM (SELECT id, metric_name, tags FROM {tags} WHERE {m} ORDER BY id LIMIT 1 BY id{{s}}) SETTINGS {idsq.SETTINGS}"
    vs = {"on-read (branch)": base.format(s=""), "index analysis (fix)": base.format(s=" SETTINGS use_skip_indexes_on_data_read = 0")}
    for q in vs.values():
        idsq.client(q)
    for r in range(ROUNDS):
        for k, q in (list(vs.items()) if r % 2 == 0 else list(vs.items())[::-1]):
            qid = f"iso-{uuid.uuid4().hex[:10]}"
            idsq.client(q, qid)
            rows.append((case.split(":")[0], k, qid))
idsq.client("SYSTEM FLUSH LOGS")
res = {}
for c, k, qid in rows:
    ms, parts = idsq.client(f"SELECT query_duration_ms, ProfileEvents['SelectedParts'] FROM system.query_log WHERE query_id = '{qid}' AND type = 'QueryFinish'").split()
    res.setdefault((c, k), []).append((int(ms), int(parts)))
for (c, k), v in res.items():
    ms = sorted(x[0] for x in v)
    print(f"{TABLE} {c:7} {k:22} median {ms[len(ms)//2]} ms ({ms[0]}-{ms[-1]}), SelectedParts {v[0][1]}, n={len(v)}")
