# Parse Prometheus remote-write requests into a protobuf arena

**Branch:** [`lilred-perf-remote-write-arena`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-remote-write-arena) @ `59469d38b474` (base master `521c4bcb7b94`) · **Status:** built (James, 2026-11-21, #24 issuecomment-7059357140): pass; ready to submit · **Report:** [reports/2026-11-21-remote-write-arena.md](../reports/2026-11-21-remote-write-arena.md) · **A/B plan:** [ab-plans/remote-write-arena.md](../ab-plans/remote-write-arena.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Prometheus remote write parses each request into a protobuf arena instead of allocating every series and label on the heap: writing to a `TimeSeries` table needs about 10% less CPU per sample (2,000 series per request with 10 or 20 labels and one sample each, background merges stopped).

### Description

`PrometheusRequestHandler` parsed every remote-write request into a `prometheus::WriteRequest` on the heap. The request has a message for every series, label and sample, and two strings per label, so a series with 10 labels takes about 35 allocations while it is parsed (the series, 10 labels, 20 strings, and the label array as it grows) and as many frees when the request is destroyed, each through the allocator and the memory tracker: about 70,000 of each for a request of 2,000 such series. In a CPU profile of master under a steady load of such requests, parsing and destroying the request took 12.9% of all the server's CPU samples.

Now the request is parsed into a `google::protobuf::Arena` that lives for the duration of the request. The messages and the string objects are carved out of blocks that grow to 32 KiB (a larger array gets a block of its own), and destroying the request frees those blocks instead of every message; the arena still runs the destructor of each string. Unchanged: the parsing itself, what `PrometheusRemoteWriteProtocol::write` receives (it reads the request through the same const accessors), and the lifetime of the request, which ended with the handler scope before too. Label values longer than 22 bytes (the inline capacity of libc++'s `std::string`) still take one heap allocation each.

Release build (clang 21, RelWithDebInfo), AMD EPYC 9R14, servers and load generator pinned to 16 cores, master `521c4bcb7b94` and this PR as two servers; the same requests (2,000 series of one sample each, warm tags cache) go to both in rotating order, fresh servers per case; server CPU is the process's utime + stime per batch of 50 requests divided by the samples written, median (min-max) of 15 batches per server:

| case | master, µs per sample | this PR | change |
|---|---|---|---|
| 10 labels, merges stopped | 14.80 (14.60-15.10) | 13.40 (13.00-14.20) | -9.5%, p < 0.001 |
| 20 labels, merges stopped | 19.20 (19.00-19.60) | 17.30 (17.00-17.70) | -9.9%, p < 0.001 |
| 10 labels, every value but the metric name longer than 22 bytes, merges stopped | 15.60 (15.40-16.10) | 14.10 (13.70-14.90) | -9.6%, p < 0.001 |
| 10 labels, merges running | 29.10 (23.30-35.30) | 28.00 (22.60-33.90) | -3.8% (unpaired p = 0.35); lower in 15 of 15 paired rounds (p < 0.001) |

A/A (master on both servers): +2.4% on the second server, which ran this PR in every case, so the offset works against it. In a CPU profile of the 10-label case, parsing and destroying the request went from 12.9% (539 of 4,163 samples) to 4.0% (151 of 3,794) of the server's CPU. The tags and samples tables of both servers hold the same rows after every case (hash of both tables).

`test_write_read.py::test_remote_write_truncated_request` sends a request (snappy and zstd) that ends in the middle of its second series: the first series is already in the arena when the parse fails, and the server must answer 400, keep serving and write nothing; it passes on master and on this PR, and the ASan integration job in CI checks the teardown. The successful paths are covered by the rest of `test_write_read.py` (a Prometheus server writing its own scrapes, `send_big_data`, zstd) and the remote-write sends of `test_query_api.py` and `test_evaluation.py`: 73 passed on this PR.
<!-- upstream:end -->

**Note for James (not part of the PR):** add `Related: <the issue you open from the draft on #24>` at the top of the PR body before submitting. The same change applies to the remote-write 2.0 request of your `jtcunning/remote-write-v2-*` branches and #119734; this branch only touches 1.0, so it conflicts with them textually only in the handler.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `521c4bcb7b94` (independent reviewer, 2 rounds):
- **Round 1:** the arena change reviewed as correct against the vendored protobuf 36.x (construction, arena strings copied out of the decompression buffers, destruction, lifetimes, exceptions). Nits taken: the comment and description (block sizes, string destructors still run, the one-element repeated field is inline), a qualified changelog entry, the label-length and CPU-method notes for the A/B, and an error-path test (`test_remote_write_truncated_request`).
- **Round 2: ⚠️ Request changes**, for the pending numbers and one pre-existing [Major] on this request path: a zstd body whose frame is cut short is answered 204 and its data is dropped (checked on master: 4 of 4 cut points of a 200-series request got 204, nothing written). Not part of this change; a draft upstream issue is on #24 for James to open, and this PR gets a `Related:` line for it. Nit taken: a large array gets an arena block of its own (`59469d38b474`).

</details>
