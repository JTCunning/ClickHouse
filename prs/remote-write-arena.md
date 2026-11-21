# [unbuilt] Parse Prometheus remote-write requests into a protobuf arena

**Branch:** [`lilred-perf-remote-write-arena`](https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred-perf-remote-write-arena) @ `59469d38b474` (base master `521c4bcb7b94`) · **Status:** unbuilt, ready to build · **Report:** [reports/2026-11-21-remote-write-arena.md](../reports/2026-11-21-remote-write-arena.md) · **A/B plan:** [ab-plans/remote-write-arena.md](../ab-plans/remote-write-arena.md)

<!-- upstream:start (the PR body is everything between these markers; the title is the heading above) -->
### Changelog category (leave one):
- Experimental Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):
Prometheus remote write parses each request into a protobuf arena instead of allocating every series and label on the heap: writing to a `TimeSeries` table needs up to [pending build]% less CPU per sample (series with 10 labels and one sample each).

### Description

`PrometheusRequestHandler` parsed every remote-write request into a `prometheus::WriteRequest` on the heap. The request has a message for every series, label and sample, and two strings per label, so by the message layout a series with 10 labels takes about 35 allocations (the series, 10 labels, 20 strings, and the label array as it grows) while it is parsed, and as many frees when the request is destroyed, each through the allocator and the memory tracker: about 70,000 of each for a request of 2,000 such series. In a CPU profile of master under a steady load of such requests (one sample per series, warm tags cache), parsing took 54 and destroying the request 73 of 1,149 samples; 96 of those were in the allocator or the memory tracker: 8.4% of all the server's CPU, background merges included, and 17% of the CPU outside background merges.

Now the request is parsed into a `google::protobuf::Arena` that lives for the duration of the request. The messages and the string objects are carved out of blocks that grow to 32 KiB (a larger array gets a block of its own; a few hundred blocks for such a request), and destroying the request frees those blocks instead of every message; the arena still runs the destructor of each string. Unchanged: the parsing itself, what `PrometheusRemoteWriteProtocol::write` receives (it reads the request through the same const accessors), and the lifetime of the request, which ended with the handler scope before too. Label values longer than 22 bytes (the inline capacity of libc++'s `std::string`) still take one heap allocation each, so the saving is largest for short labels.

Release build, [pending build: host], master `521c4bcb7b94` and this PR, two servers, the same requests sent to both in rotating order; server CPU is the process's utime + stime around each batch of 50 requests divided by the samples written, median of 15 batches per server:

| case (one sample per series, warm tags cache) | master | this PR |
|---|---|---|
| 2,000 series × 10 labels, merges stopped: server CPU per sample | [pending build] | [pending build] |
| 2,000 series × 20 labels, merges stopped | [pending build] | [pending build] |
| 2,000 series × 10 labels, values longer than 22 bytes, merges stopped | [pending build] | [pending build] |
| 2,000 series × 10 labels, merges running | [pending build] | [pending build] |

[pending build: A/A spread.] The tables of both servers hold the same rows after the run (hash of the tags and samples tables).

`test_write_read.py::test_remote_write_truncated_request` sends a request (snappy and zstd) that ends in the middle of its second series: the first series is already in the arena when the parse fails, and the server must answer 400, keep serving and write nothing (the sanitizer builds check the teardown). The successful paths are covered by the rest of `test_write_read.py` (a Prometheus server writing its own scrapes, `send_big_data`, zstd) and the remote-write sends of `test_query_api.py` and `test_evaluation.py`.
<!-- upstream:end -->

**Note for James (not part of the PR):** add `Related: <the issue you open from the draft on #24>` at the top of the PR body before submitting. the same change applies to the remote-write 2.0 request of your `jtcunning/remote-write-v2-*` branches and #119734; this branch only touches 1.0, so it conflicts with them textually only in the handler.

<details><summary>Self-review (lil red, preflight)</summary>

Preflight against the review prompt at master `521c4bcb7b94` (independent reviewer, 2 rounds):
- **Round 1:** the arena change reviewed as correct against the vendored protobuf 36.x (construction, arena strings copied out of the decompression buffers, destruction, lifetimes, exceptions). Nits taken: the comment and description (block sizes, string destructors still run, the one-element repeated field is inline), a qualified changelog entry, the label-length and CPU-method notes for the A/B, and an error-path test (`test_remote_write_truncated_request`).
- **Round 2: ⚠️ Request changes**, for the pending numbers and one pre-existing [Major] on this request path: a zstd body whose frame is cut short is answered 204 and its data is dropped (checked on master: 4 of 4 cut points of a 200-series request got 204, nothing written). Not part of this change; a draft upstream issue is on #24 for James to open, and this PR gets a `Related:` line for it. Nit taken: a large array gets an arena block of its own (`59469d38b474`).

</details>
