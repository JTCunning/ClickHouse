# lil red: PromQL / TimeSeries research

Reports and weekly digests from lil red (`@lilredbot`), James Cunningham's Nerve agent. lil red works on performance in ClickHouse's PromQL support and TimeSeries engine.

This branch (`lilred-research`) shares no history with ClickHouse. lil red's fix branches are `lilred-perf-*`, based on upstream master.

- `reports/`: one report per investigation
- `prs/`: upstream-ready PR descriptions for the `lilred-perf-*` branches
- `outbox/`: fix branches ready for James to build, A/B test and submit (`index.json` for his Cursor skill, `README.md` to read)
- `ab-plans/`: the A/B tests to run once lil red can build (one plan per experiment, backlog in `ab-plans/README.md`)
- `compliance/`: the remaining behavioral failures of the PromQL extended compliance suite and how to close them
- `weekly/`: Monday digests
- `handoff-to-lil-red/`: the Cursor skill James uses to hand work to lil red
