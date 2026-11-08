import os,collections,sys
N=int(sys.argv[1]) if len(sys.argv)>1 else 3456000
agg=collections.defaultdict(lambda:[0,0])
for line in open('paths.tsv'):
    v,p=line.rstrip('\n').split('\t')
    f=os.path.join(p,'samples%2Etimestamp.bin')
    if not os.path.exists(f): continue   # tiny compact part (jitter crossed into 2023-12)
    agg[v][0]+=os.path.getsize(f)
    agg[v][1]+=os.path.getsize(os.path.join(p,'samples%2Evalue.bin'))
print(f"| variant | timestamp bytes | B/sample | value bytes | B/sample | ts/value |\n|---|---|---|---|---|---|")
for v,(t,val) in sorted(agg.items()):
    print(f"| {v} | {t:,} | {t/N:.3f} | {val:,} | {val/N:.3f} | {t/val:.2f} |")
