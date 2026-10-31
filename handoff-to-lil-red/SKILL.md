---
name: handoff-to-lil-red
description: Hand a ClickHouse PromQL / TimeSeries performance lead to lil red (James's Nerve agent, GitHub @lilredbot) as a GitHub issue on JTCunning/ClickHouse. Use when James says "hand this to lil red", "send this to lil red", "file a handoff", "lil red should dig into this", or at the end of a research session that found something worth measuring or fixing.
---

# Hand work off to lil red

lil red is James Cunningham's Nerve agent (GitHub `@lilredbot`). It works on simple but meaningful performance improvements in ClickHouse's PromQL support and TimeSeries engine.

- **Where it works:** James's public fork `JTCunning/ClickHouse`, on `lilred/*` branches only. It never touches upstream, `master` or James's branches.
- **How a handoff works:**
  1. You open a GitHub issue on that fork that mentions `@lilredbot`.
  2. lil red picks it up within minutes and replies "Picked up as task …".
  3. It posts the report on the same issue.

## Setup (once)

- `gh` must be authenticated as James (`jtcunning`). Check with `gh auth status`.
- The repository is `JTCunning/ClickHouse`.

## Steps

1. **Collect** from this session what lil red needs to start without asking. Fill in every section of the template below; write "unknown" rather than guessing.
2. **Privacy check: the fork is public.** Remove customer names, internal hostnames and URLs, dashboards, Slack links, private benchmark data and anything under NDA.
   - If the handoff can't be written without them, use the **private route** instead: paste the filled template into lil red's web chat. Open it from the Nerve Cloud console: **Agents → lil red → Open agent**.
3. **Write** the body to `/tmp/lil-red-handoff.md`, then create the issue:
   ```bash
   gh issue create -R JTCunning/ClickHouse \
     --title "handoff: <short title>" \
     --body-file /tmp/lil-red-handoff.md \
     --label handoff
   ```
   The body **must** end with the line `@lilredbot please pick this up.` The mention is what notifies lil red.
4. **Show James** the issue URL. Follow-up comments by `jtcunning` on the issue reach lil red too: more context, "also try X", or measurements from a local build of a `lilred/*` branch.

## Issue body template

```markdown
## Goal
<one sentence: what should get faster, and for which workload>

## Why it matters
<who hits it, how often, user-visible effect>

## What I found
<observations and numbers so far: ClickHouse version/commit, dataset size, query, timings, profiles>

## Repro
<schema / CREATE TABLE … ENGINE = TimeSeries, data generation SQL, the PromQL or SQL query, settings>

## Code pointers
<files, functions, upstream issues/PRs, or branches in JTCunning/ClickHouse you suspect: e.g. src/AggregateFunctions/TimeSeries/…, #125759>

## Ideas / hypothesis
<what you think the cause or fix is; "none yet" is fine>

## Constraints
<what not to touch, e.g. no on-disk format changes; deadlines>

## Done when
<a report only? a fix branch with a test and a perf case? a specific speedup?>

## Priority
<now / this week / whenever>

@lilredbot please pick this up.
```

## What lil red delivers

- **A plain-language report:** what happens, numbers, cause, proposed fix, overlap with upstream. It's posted on the issue and stored at `reports/` on the branch `lilred/research`: https://github.com/JTCunning/ClickHouse/tree/lilred/research
- **When there's a simple fix:**
  - a branch `lilred/perf-<slug>`, based on upstream master
  - a compare link: `https://github.com/ClickHouse/ClickHouse/compare/master...JTCunning:ClickHouse:lilred/perf-<slug>`
  - an upstream-ready PR description at `prs/<slug>.md` on `lilred/research`, written to pass the clickhouse-gh review bot

  lil red opens no PRs. You copy the branch and the description when you want to.
- **Builds:** lil red can't build ClickHouse yet, so "after" numbers stay pending until a build exists. If you build a branch, post the numbers as a comment on the issue.
