# Benchmark Report

Scientificloud · Mborobak · 2026-11-18 · first run of the [benchmark suite](../benchmarks/README.md) (scope §8.3)

Raw data: [`benchmarks/results/2026-11-18/`](../benchmarks/results/2026-11-18/) (`summary.md`, `raw.json`, `plans.json`, `environment.json`).

## 1. Setup

| Item | Value |
|------|-------|
| IRIS | 2026.2 (Build 221U), ARM64 container on the same Mac, loopback network; `AutoParallel` on |
| Client | Apple M4, 10 cores; Python 3.11.16; `intersystems-irispython` 5.4.0; Mborobak at commit `8bf271d` |
| Data | Seeded generator (seed 42), `Orders` with 10k / 100k / 1M rows and `Customers` with 1/10 as many; customer skew 1.1 (Zipf-like); mixed-case strings; indexes on `status`, `customer_id`, `country`; tables tuned |
| Runs | 2 warm-up + 5 measured runs per variant; medians of total client time |
| Scenarios | 9 in 6 families: filter (3), projection, aggregation (2), join, sort, chain |

Four variants per scenario: hand-written SQL with IRIS string rules (`native`), the same with `%EXACT` where Mborobak adds it (`native_exact`), Mborobak with IRIS string rules (`mb_iris`) and Mborobak's default PySpark rules (`mb`). **Every Mborobak result matched its hand-written SQL** at every size.

This is a single-machine run with only 5 measured runs: differences under about 15% between variants with the same plan are within the noise seen here (for example `join_agg_sort` native: 527 ms vs. 300 ms with an identical plan, from parallel-execution variance).

## 2. Results at 1M orders

| Scenario | native | native_exact | mb | Overhead (mb vs native_exact) | Rows returned |
|----------|-------:|-------------:|---:|------------------------------:|--------------:|
| filter_count_string | 25.7 | 83.9 | 87.0 | +4% | 1 |
| lookup_count_indexed_string | 0.8 | 1.4 | 1.5 | +4% | 1 |
| filter_fetch_numeric | 142.2 | 142.2 | 217.9 | **+53%** | 99,538 |
| projection_fetch | 860.6 | 850.0 | 2136.4 | **+151%** | 1,000,000 |
| agg_by_int | 175.7 | 177.6 | 242.3 | **+36%** | 64,511 |
| agg_by_string | 52.1 | 42.1 | 44.5 | +6% | 3 |
| join_agg_sort | 527.1 | 299.8 | 310.0 | +3% | 30 |
| sort_limit_double | 142.9 | 141.6 | 43.9 | −69% (parallel plan, §3.3) | 100 |
| chain_scope_example | 193.7 | 169.3 | 162.8 | −4% | 20 |

Times in ms. 10k and 100k results are in [`summary.md`](../benchmarks/results/2026-11-18/summary.md).

**Against the target** (median overhead ≤ 10% on queries of 100 ms or more, D5): **+19.9%** over the 6 qualifying scenario × size pairs. The target is not met, and §3.1 shows why.

## 3. Findings

### 3.1 The overhead is in turning rows into Python objects, not in compiling or in IRIS

- **Planning and compilation cost 0.02–0.05 ms** per action with the compiled-SQL cache, and 0.03–0.34 ms on a cold compile. This is negligible at every size.
- **IRIS execution time is the same** for Mborobak's SQL and the hand-written SQL when the plans match. The extra subquery that `count()` adds costs nothing measurable.
- **Queries returning few rows show no meaningful overhead** (chain, join, string aggregation, counts: −4% to +6%).
- **The overhead grows with the number of rows returned.** At 1M rows (`projection_fetch`), the driver's fetch takes about 830 ms in both cases. Mborobak then spends about 565 ms converting every value to its schema type (decimal quantizing, date parsing, which the driver returns as text) and about 735 ms building one `Row` per result row.

Both costs are in Mborobak's Python code and can be reduced (§4).

### 3.2 What `%EXACT` costs (compiler spec O4)

| Query shape | Effect of `%EXACT` | Why (from the IRIS plans) |
|-------------|--------------------|---------------------------|
| Equality filter on an indexed string column, counting rows | **3.3× slower** (25.7 → 83.9 ms at 1M; 0.8 → 1.4 ms on the smaller lookup) | Without it, IRIS counts from the index alone, in parallel. With it, IRIS still uses the index (by the uppercased value) but reads every candidate row to test the exact value, and the plan is serial |
| Grouping by a string, joining, then grouping | No cost (−19% to +6%, within noise) | The work is dominated by reading rows anyway |
| Filters on numbers, sorts, projections | Not affected | No string comparison |

So `%EXACT` does not lose the index entirely, as feared: it turns an index-only scan into index plus row reads. That matters for selective filters and counts on indexed string columns of large tables. A user who knows the data is case-consistent can switch to `stringSemantics="iris"`. Declaring a column `COLLATE %EXACT` avoids the cost entirely, since Mborobak then adds no `%EXACT` (compiler spec §6.4). **D10 stands.**

### 3.3 Sorting on a DOUBLE column ran 3× faster through Mborobak

For `orderBy(score.desc()).limit(100)`, IRIS chose a parallel plan for Mborobak's bucket sort key (compiler spec §7.9) and a serial plan for the plain `ORDER BY score DESC` (43.9 vs. 142.9 ms). This is a planner choice, not a property Mborobak can rely on, but it shows the bucket key costs nothing.

### 3.4 The driver returns some numbers as text

For `SUM(amount) … ORDER BY revenue DESC`, the IRIS driver returned the sums as `str` (`'968569.66'`), not `Decimal`. Mborobak converts by schema, so its results are correct. Code using the driver directly gets strings. The benchmark compares native results by value for this reason.

## 4. Next steps

1. **Faster result conversion:**
   - Skip the conversion when the driver already returns the schema type (most `int` and `float` columns).
   - Quantize decimals only when the scale differs.
   - Convert column by column instead of value by value.
2. **Cheaper `Row` objects:** one `Row` subclass per result schema, with the field names on the class instead of on every row.
3. **Re-run this benchmark** after those changes and against IRIS build 154 (D1).
4. **More runs:** add more measured runs, and pin parallel execution on and off, to reduce the noise seen in §1.
