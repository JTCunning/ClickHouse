# Re-creates promql_selector_pk_range for both id types and prints ProfileEvents that differ for its first query.
import re, sys, json, subprocess
import xml.etree.ElementTree as ET
sys.argv = ["x"]
import bench
from bench import q, with_settings, optimize, unescape, PLAIN
xml = subprocess.run(["git", "-C", bench.REPO, "show", "origin/master:tests/performance/promql_selector_pk_range.xml"], capture_output=True, text=True, check=True).stdout
root = ET.fromstring(xml)
create = " ".join(e.text.strip() for e in root.iter("create_query")); table = "promql_pk_range_ts"
fills = [unescape(e.text) for e in root.iter("fill_query")]
queries = [unescape(e.text) for e in root.iter("query") if table in e.text]
for t in (table, table + "_plain"):
    q(f"DROP TABLE IF EXISTS {t} SYNC")
q(create); q(create.replace(table, table + "_plain") + PLAIN)
for t in (table, table + "_plain"):
    for f in fills:
        q(with_settings(f.replace(table, t), "max_insert_threads = 1, max_threads = 2, max_block_size = 512, max_insert_block_size = 512"))
    optimize(t)
for i in range(3):
    for t, k in ((table, "lc"), (table + "_plain", "plain")):
        q(with_settings(queries[0].replace(table, t), f"max_threads = 2, use_query_cache = 0, log_comment = 'pe_{k}_{i}'"))
q("SYSTEM FLUSH LOGS")
print(q("""SELECT k, v_lc, v_plain, round(v_lc / greatest(v_plain, 1), 2) r FROM (
  SELECT k, anyIf(v, c = 'pe_lc_2') v_lc, anyIf(v, c = 'pe_plain_2') v_plain FROM (
    SELECT log_comment c, arrayJoin(mapKeys(ProfileEvents)) k, ProfileEvents[k] v FROM system.query_log
    WHERE type = 'QueryFinish' AND log_comment IN ('pe_lc_2', 'pe_plain_2')) GROUP BY k)
WHERE (v_lc > 1.2 * v_plain OR v_plain > 1.2 * v_lc) AND greatest(v_lc, v_plain) > 1000 ORDER BY greatest(v_lc, v_plain) DESC LIMIT 40""", "PrettyCompactMonoBlock"))
