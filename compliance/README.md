# PromQL extended compliance: behavioral failures

Which upstream Prometheus engine tests ClickHouse still gets wrong, and how to close them. Updated by lil red's Sunday study runs. Analysis and fix plans only, no fix branches yet.

**Source:** CI job `PromQL Compliance`, suite `extended_support` (the pinned upstream Prometheus `promqltest` files in `tests/integration/test_prometheus_protocols/promqltest/`). Result JSON: `https://clickhouse-builds.s3.amazonaws.com/REFs/master/<sha>/promql_compliance/promql_compliance_result.json`.

**Latest:** master `958c7e7b54ef` (2026-11-01) · upstream Prometheus `3da2514eff5c` · compliance suite 539/539 · extended support 682/732 passed (93.2%), **16 behavioral failures**, 34 unsupported · excluded: 149 native-histogram, 248 assertion evals.

"Behavioral" follows CI's own split (`compare_eval` in `promqltest_loader.py`): an error that says not implemented / not supported, code 48 or HTTP 501 is "unsupported" (a dialect gap) and is out of scope here. Everything else counts: a wrong value, a wrong series set or order, or an error where Prometheus returns a result.

| # | failure (as CI reports it) | count | evals (upstream file:line) | status | analysis |
|---|---|---|---|---|---|
| 1 | ClickHouse error: 500 Internal Server Error: Code: 768. DB::Exception: Multiple series have the same tags {'env': '1'}, duplicate series in  | 5 | (to find) | todo | |
| 2 | value mismatch for {'test': 'NaN sample'}: 0.8 vs 0.6 | 2 | (to find) | todo | |
| 3 | value mismatch for {}: inf vs 9.98846567431158e+307 | 2 | (to find) | todo | |
| 4 | value mismatch for {}: -inf vs -9.98846567431158e+307 | 2 | (to find) | todo | |
| 5 | series set mismatch: 2 vs 1 | 2 | (to find) | todo | |
| 6 | value mismatch for {'test': 'NaN sample'}: 0.19999999999999996 vs nan | 1 | (to find) | todo | |
| 7 | value mismatch for {}: inf vs nan | 1 | (to find) | todo | |
| 8 | series set mismatch: 1 vs 1 | 1 | (to find) | todo | |

**Unsupported (out of scope):** Function end is not implemented. (NOT_IMPLEMENTED) ×10; Function sort_by_label is not implemented. (NOT_IMPLEMENTED) ×8; Function start is not implemented. (NOT_IMPLEMENTED) ×8; Function sort_by_label_desc is not implemented. (NOT_IMPLEMENTED) ×3; Aggregation operator 'quantile' with a non-constant scalar parameter is not supported. (NOT_IM ×1; Function 'quantile_over_time' does not support a time-varying first argument (the quantile) to ×1; Function 'quantile_over_time' does not support a time-varying first argument (the quantile) to ×1; Function double_exponential_smoothing is not implemented. (NOT_IMPLEMENTED) ×1; Aggregation operator 'limit_ratio' is not implemented. (CANNOT_EXECUTE_PROMQL_QUERY) ×1.

Statuses: `todo` → `analysed` (cause and fix plan written) → `in PR #N` / `fixed on master <sha>`.

