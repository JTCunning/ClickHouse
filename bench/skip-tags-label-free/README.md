# skip-tags-label-free (handoff #8)
Emulates a label-free selector mode for `sum(rate(x[5m]))` with hand-written SQL on a prebuilt master server (no build).
Dataset and results: `reports/2026-11-01-skip-tags-label-free.md`. `t_end` holds the T_END the data was loaded with.
- `bench.py <metric>`: master vs hand-written master SQL vs id-only lookup, interleaved; prints JSON lines.
- `bench_drop.py <metric>`: cost split of the `__name__` drop step.
