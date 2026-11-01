# Derives, from sub_<k>.sql (written by gen.py), the variants used by sweep.py/sweep2.py:
#  - sub_<k>.sql gets "AS x" on its SELECT column so the outer query can consume it (count() lets the analyzer drop timeSeriesStoreTags)
#  - nojoin_<k>.sql: the same subquery without the time ranges join
#  - ids_<k>.sql: the same subquery selecting id instead of timeSeriesStoreTags (no collector)
import re
for k in range(4):
    s=open(f"sub_{k}.sql").read()
    a=s.index(" ALL LEFT JOIN ("); b=s.index(" AS time_ranges_subquery USING (id)")+len(" AS time_ranges_subquery USING (id)")
    nj=s[:a]+s[b:]
    nj=re.sub(r", or\(equals\(time_ranges_subquery\.matched, 1\), notEquals\(tupleElement\(id, 1\), sipHash64\('[^']*'\)\)\)\)$", ")", nj)
    nj=re.sub(r", equals\(time_ranges_subquery\.matched, 1\)\)$", ")", nj)
    nj=re.sub(r"WHERE and\((.*)\)$", r"WHERE \1", nj)
    ids=re.sub(r"^SELECT timeSeriesStoreTags\(id, tags, '__name__', metric_name\)", "SELECT id", s)
    for name, text in (("sub", s), ("nojoin", nj), ("ids", ids)):
        text=re.sub(r"^SELECT (timeSeriesStoreTags\(id, tags, '__name__', metric_name\)|id) FROM", r"SELECT \1 AS x FROM", text, count=1)
        open(f"{name}_{k}.sql","w").write(text)
