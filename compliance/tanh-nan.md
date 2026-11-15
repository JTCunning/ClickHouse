# `tanh(NaN)` returns -1 (1 eval)

**Status:** analysed 2026-11-15 at master `6ea176fa3035`, upstream Prometheus `3da2514eff5c`. The cause is in the SQL function `tanh`, not in the PromQL layer. Nobody is working on it.

## Failing eval

| file:line | query | Prometheus | ClickHouse |
|---|---|---|---|
| trig_functions.test:48 | `tanh(trig)` for `trig{l="NaN"} NaN` | `NaN` | `-1` |

The other two series (10 and 20) pass. Reproduced with `research/bench/compliance/run_extended.py --file trig_functions.test` on the master binary (26.11.1.2253, x86-64 with AVX2).

## Cause

PromQL `tanh` maps one-to-one to the SQL function `tanh` (`src/Storages/TimeSeries/PrometheusQueryToSQL/applyOneArgumentMathFunction.cpp:75`). So the SQL function itself is wrong:

```
SELECT tanh(nan), tanh(materialize(nan)), tanh(toFloat32(nan)), sigmoid(nan), exp(nan), log(nan)
-- -1, -1, -1, 0, nan, nan
```

In official builds `tanh` calls the vectorized `NFastOps::Tanh` from the `fastops` library (`src/Functions/tanh.cpp`, `USE_FASTOPS` branch; `contrib/fastops`). The kernel computes tanh(x) = 2 / (2^(k·x) + 1) − 1 (`fastops/core/FastIntrinsics.h:817`, `S_Tanh`). Its 2^y helper first clamps y to the representable range with a vector min and max (`Pow2V`, `FastIntrinsics.h:738–742`). On x86 the vector min (`_mm256_min_pd`, `fastops/core/SIMDFunctions.h`) returns its second operand when either one is NaN, and the clamp passes the input first: `Min(x, max_exponent)`. So a NaN input becomes the upper bound, 2^y overflows, and the result is 2 / ∞ − 1 = −1. NaN should pass through: Prometheus uses Go's `math.Tanh`, which returns NaN for NaN, as libm does.

`sigmoid` uses the same library and returns 0 for NaN. `exp` and `log` also use `fastops`, but they return NaN correctly. Only `tanh` matters for PromQL; `sigmoid` has no PromQL name.

The eval passed in the 2026-11-01 report (`958c7e7b`) and has failed since `7b5f2bf` (2026-11-07). Neither `tanh.cpp`, the `fastops` submodule nor the test changed in between. `fastops` has separate AVX2 and NEON kernels since #98230, so the CI machine's architecture may decide the result. That is a guess; it was not checked on ARM.

## Fix plan

One mechanism: after the vectorized kernel, put NaN inputs back.

In `src/Functions/tanh.cpp`, in `Impl::execute` after `NFastOps::Tanh<>(src, size, dst)`:

```cpp
/// fastops clamps the input with a vector min/max, which turns NaN into a number (tanh(NaN) = -1).
for (size_t i = 0; i < size; ++i)
    dst[i] = isNaN(src[i]) ? src[i] : dst[i];
```

The same three lines go into `src/Functions/sigmoid.cpp`. The loop has no branch and compiles to a compare and a blend, so it costs one extra pass over a block already in cache. The non-`fastops` branch (`2 / (1 + exp(-2x)) - 1`) already returns NaN. Effort S: two files, about 10 lines, no settings, no format change.

Other options, and why not:

- Wrap NaN in the PromQL layer (`if(isNaN(x), x, tanh(x))` in `applyOneArgumentMathFunction.cpp`). Smaller, but SQL `tanh(nan)` stays wrong for everyone else, and every new user of `tanh` would need the same wrapper.
- Fix the clamp in `fastops` itself (swap the min/max operands so NaN propagates). That fixes the root, but it needs a PR to the `ClickHouse/fastops` fork and a submodule bump, and it changes a kernel shared by `exp`, `log`, `sigmoid` and `tanh`.

## How to prove it

- `trig_functions.test:48` passes in the extended suite (expected 681 → 682 passed).
- A new stateless test: `tanh` and `sigmoid` of `nan`, `-nan`, `materialize(nan)`, `toFloat32(nan)` and `inf`/`-inf`, plus a mixed block (`arrayJoin([0., nan, 1., -inf])`) so a NaN in the middle of a vector is checked. Also a PromQL `tanh` over a NaN sample.
- Performance guard: `tests/performance/math_sigmoid_tanh.xml` stays within noise.

## Overlap

None found (2026-11-15): no issue or PR about `tanh` or NaN in `fastops`. The nearest work is [#109717](https://github.com/ClickHouse/ClickHouse/pull/109717) (open, "Always use vectorized kernels for exp2/exp10/log2/log10/pow/sin/cos/tan"), which touches other math functions, not `tanh` or `sigmoid`.
