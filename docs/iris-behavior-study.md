# IRIS Behavior Study

Scientificloud · Mborobak · Phase 1 · 2026-11-18 · Answers the questions in [mborobak-scope.md](mborobak-scope.md) §15 ("IRIS behavior study")

## How this was done

- **Instance:** InterSystems IRIS 2026.2 (Build 221U), Community Edition, ARM64 container (`irispark-iris:2026.2`, running locally from the IRISpark project), namespace `DATASPARK`.
- **Client:** `intersystems_irispython` 5.4.0 on Python 3.9, same machine (loopback network).
- **Probe:** [probes/iris_behavior_probe.py](../probes/iris_behavior_probe.py) creates tables in a scratch schema `MborobakProbe`, runs about 120 small SQL experiments and drops everything at the end. Raw output: [probes/results-2026-11-18-iris-2026.2.json](../probes/results-2026-11-18-iris-2026.2.json). That run used the schema name `FluxoraProbe`, from the project's former name; the script now uses `MborobakProbe`. Run with `IRIS_HOST`, `IRIS_PORT`, `IRIS_NAMESPACE`, `IRIS_USERNAME` and `IRIS_PASSWORD` set.
- **Prior evidence:** IRISpark's `docs/known_differences.md` and `docs/LESSONS_LEARNED.md`, which record bugs hit while generating SQL for IRIS 2026.1–2026.2.
- **Docs:** InterSystems documentation and Developer Community posts, linked where used.

"Spark" below means Spark 4.x with its defaults (ANSI mode on), from general knowledge, not tested here. D9 has since pinned the reference to PySpark 4.3.x with ANSI mode on; the Spark column still needs confirming against it.

## 1. Summary

Twelve findings matter for the compiler. Four of them are silent wrong results, which no error message would catch:

| # | Finding | Severity |
|---|---------|----------|
| F1 | **String columns compare case-insensitively and ignore trailing spaces** (default `SQLUPPER` collation). This affects `=`, `IN`, `LIKE`, joins, `GROUP BY`, `DISTINCT`, `UNION`, `ORDER BY`, `MIN`/`MAX`. | **Silent wrong result** |
| F2 | **`GROUP BY`, `DISTINCT`, `MIN`/`MAX` return the uppercased value** (`'apple'` comes back as `'APPLE'`). | **Silent wrong result** |
| F3 | **`GROUP BY 1` is a constant, not a column position.** It collapses the result into one group. | **Silent wrong result** |
| F4 | **`CAST('abc' AS INTEGER)` returns 0**, and casts that overflow a `NUMERIC(p,s)` or `INTEGER` don't fail. | **Silent wrong result** |
| F5 | `ORDER BY` is rejected inside subqueries and CTEs unless `TOP` is present; `TOP ALL` works. `LIMIT` is rejected inside a subquery. | Compile rule |
| F6 | `LIMIT`, `LIMIT … OFFSET`, `OFFSET … FETCH` and `TOP ?` all work at the outer level. | Compile rule |
| F7 | **About 999 literals plus parameters per statement, at most.** Beyond that: SQLCODE -1 or a misleading -202. Statement length itself was not the limit (a 26 KB statement passed). | Compile rule |
| F8 | No `NULLS FIRST`/`NULLS LAST` syntax. Default NULL order already matches Spark: first when ascending, last when descending. | Compile rule |
| F9 | Division by zero is a **fatal error** (SQLCODE -400) that kills the query, also for `DOUBLE`. | Semantics |
| F10 | `''` and `NULL` are distinct, like in Spark. | Good news |
| F11 | Floating-point `SUM` differs between serial and `%PARALLEL` execution in the last digits; `NUMERIC` sums are exact. | Confirms tolerance policy |
| F12 | Fetch speed is about **1 million rows/s** for 500k rows × 5 columns on loopback; building a pandas DataFrame adds about 55% on top. The driver returns `DATE` values as strings. | Benchmark baseline |

The scope assumed that a parameter used in both `SELECT` and `GROUP BY` would fail (§6.4). **On 2026.2 it works** (`SELECT i + ? … GROUP BY i + ?` returned correct groups). §6.4 should be corrected.

## 2. Findings by scope question

### 2.1 Ordering inside subqueries (scope §6.2)

| Statement | Result |
|-----------|--------|
| `SELECT … FROM (SELECT … ORDER BY id DESC)` | Error, SQLCODE -1 |
| `SELECT … FROM (SELECT TOP 3 … ORDER BY id DESC)` | OK, order kept (5, 4, 3) |
| `SELECT … FROM (SELECT TOP ALL … ORDER BY id DESC)` | OK, order kept (5, 4, 3, 2, 1) |
| Same, then `WHERE id > 1` on the outside | OK, order kept |
| `WITH c AS (SELECT … ORDER BY …) SELECT … FROM c` | Error, SQLCODE -1 |
| `ORDER BY` inside one arm of a `UNION ALL` | Error, SQLCODE -25 |
| `ORDER BY` after the whole `UNION ALL` | OK |

IRISpark hit the same wall twice (`count()` wrapping an ordered query; ordered pipelines with aggregations), and a [community thread](https://community.intersystems.com/node/435691) gives the same `TOP ALL` workaround.

**Rule for the compiler:** keep `ORDER BY` only at the outermost level. Where an order must survive inside a subquery (for example `orderBy().limit()` followed by more transformations), emit `TOP n` or `TOP ALL` with it. The probe showed the outer query keeping the inner order on a 5-row table; that is not a guarantee, so the compiler should still re-apply the final ordering at the outermost level.

### 2.2 Limits and paging (scope §6.3)

All of these work at the outer level: `TOP n`, `TOP ?`, `LIMIT n`, `LIMIT ?`, `LIMIT n OFFSET m`, `OFFSET m ROWS FETCH NEXT n ROWS ONLY`.

`LIMIT` inside a subquery fails (SQLCODE -1), even with `ORDER BY`. **Rule:** use `TOP` inside subqueries and either form at the outer level. Generating `TOP` everywhere keeps one form; `OFFSET` is only available through `LIMIT … OFFSET` or `OFFSET … FETCH`, so `offset` must stay at the outer level.

When `LIMIT`/`OFFSET` first appeared in IRIS was not checked. This matters for D1 (minimum version).

### 2.3 Parameters (scope §6.4)

- `?` works in `WHERE`, `IN`, `LIKE`, `HAVING`, `TOP`, `LIMIT`, the `SELECT` list, and in expressions repeated in `SELECT` and `GROUP BY` (including inside `CASE`).
- `GROUP BY` an alias works (`GROUP BY k`).
- **`GROUP BY 1` does not mean "first column".** It groups by the constant 1, so `SELECT w, COUNT(*) … GROUP BY 1` returned one row with a count of 7 (F3). The compiler must never emit ordinals in `GROUP BY`. Spark's SQL treats `GROUP BY 1` as a position, so this also matters for `session.sql()` passthrough.
- `IS NULL` on an expression in the `SELECT` list is rejected (SQLCODE -25); `CASE WHEN expr IS NULL THEN … END` works. IRISpark recorded the same.
- **Literal substitution:** IRIS replaces literals with `?` when it caches a query ([docs](https://docs.intersystems.com/csp/docbook/DocBook.UI.Page.cls?KEY=AGSOS)). So sending values as parameters is for safety (no SQL injection), not for plan reuse. Plan reuse happens either way.
- **About 999 literals and parameters per statement** (F7). Bisection found the limit at 997 values in an `IN` list (plus 2 other literals in the statement) and 999 values in a `SELECT` list, with literals and parameters counting the same. Past that, IRIS returns SQLCODE -1, or -202 ("closing parentheses missing") for a large `CASE`. IRISpark's earlier "UNION ALL statements fail around 9.5 KB" finding is probably the same limit, since every arm adds literals; in this probe a 450-arm, 26 KB `UNION ALL` passed and a 500-arm one failed. Nesting 60 subqueries deep also passed.

**Rules for the compiler:**
1. Count literals and parameters while compiling, and fail with a clear Mborobak error before reaching 999.
2. For `isin()` with a long list, plan a strategy for later (for example a temporary table); the MVP raises an explicit error above the limit.
3. Parameters stay the default for external values, for safety.

### 2.4 Identifiers and schemas (scope §6.5)

- Table and column names are case-insensitive: `mborobakprobe.t` finds `MborobakProbe.T`, and `ID` finds `id`. A double-quoted `"Id"` also matched `id`.
- `CREATE TABLE` without a schema puts the table in **`SQLUser`**.
- A reserved word works as an alias both quoted (`"Order"`) and unquoted (`AS Order`). The compiler should quote anyway; IRISpark hit reserved-word problems in `ORDER BY`.
- The cursor reports aliases with their original case (`RevenueTotal`, `lower_alias`), so Mborobak can preserve the user's column names.
- `INFORMATION_SCHEMA.COLUMNS` gives names, types, precision, scale and nullability, which is enough for schema lookups (§5.2). **It reports `COLLATION_NAME` as NULL**, so collation (F1) has to be read from another catalog source; still to be found.

### 2.5 Strings: collation (new, the most important finding)

IRIS string columns use `SQLUPPER` collation by default: values are compared, grouped and sorted as if uppercased, with trailing spaces removed ([community explanation](https://cn.community.intersystems.com/printpdf/505226)). String literals are not affected; columns are.

Test data: `'apple'`, `'Apple'`, `'APPLE'`, `'banana'`, `'apple '`, `' apple'`, `'Banana'`.

| Operation on a default column | IRIS result | Spark result |
|-------------------------------|-------------|--------------|
| `WHERE w = 'APPLE'` | 4 rows (`apple`, `Apple`, `APPLE`, `apple `) | 1 row |
| `WHERE w LIKE 'APP%'` / `IN ('APPLE')` | 4 / 4 | 1 / 1 |
| `GROUP BY w` | 3 groups: `' APPLE'`, `'APPLE'` ×4, `'BANANA'` ×2 | 7 groups, original values |
| `SELECT DISTINCT w` | `APPLE`, `BANANA`, `' APPLE'` | 7 values |
| `MAX(w)`, `MIN(w)` | `'BANANA'`, `' APPLE'` | `'banana'`, `' apple'` |
| Self-join `ON a.w = b.w` | **21 rows** | 7 rows |
| `UNION` (distinct) | 3 rows | 7 rows |
| `ORDER BY w` | Case-insensitive order | Binary order (uppercase first) |

`'a' = 'A'` between two literals is false, and `COUNT(DISTINCT 'b','B')` on literals is 2: only columns use the collation.

**Two ways to get Spark's behavior, both verified:**
- Wrap the column in `%EXACT(...)`: `GROUP BY %EXACT(w)` gave the 7 original values.
- A column declared `COLLATE %EXACT` behaves exactly (1 row for `= 'APPLE'`, 7 groups, 7 rows in the self-join).

The community notes that `%EXACT` can't use an index on the column, so it may be slower. Since IRIS 2024.2, `UNION` and `SELECT DISTINCT` also apply column collation ([community post](https://community.intersystems.com/node/587171)).

This is a product decision, not just a compiler rule (proposed **D10**):
- **Spark semantics by default:** wrap string columns in `%EXACT` for comparisons, joins, grouping, distinct, ordering and `MIN`/`MAX`. Results match PySpark; some queries may lose index use.
- **IRIS semantics by default:** document the difference prominently and offer an opt-in. Faster, but results differ from PySpark on mixed-case data.

The rewrite must skip columns already declared `%EXACT`, which needs the collation from the catalog (§2.4).

### 2.6 NULL, empty string, numbers (scope §6.8)

| Case | IRIS 2026.2 | Spark (ANSI on) |
|------|-------------|-----------------|
| `''` vs `NULL` | Distinct: `''` round-trips as `''`, `IS NULL` doesn't match it, `LENGTH('') = 0`, `'' || 'q' = 'q'` | Same |
| `NULL` in `||` | `NULL` | `NULL` |
| `7/2` (integers) | `3.5` (returned as `Decimal`, 18 decimal places) | `3.5` as `DOUBLE` |
| `7\2`, `7#2` | Integer division `3`, modulo `1` | `div`, `%` |
| `1/0`, `CAST(1 AS DOUBLE)/0` | **Fatal error, SQLCODE -400** | Error (`DIVIDE_BY_ZERO`); NULL with ANSI off |
| `CAST('abc' AS INTEGER)` | **`0`** | Error; NULL with ANSI off |
| `CAST(2147483647 AS INTEGER) + 1` | `2147483648` (no overflow) | Error; wraps with ANSI off |
| `CAST(123456.789 AS NUMERIC(5,2))` | `123456.79` (precision ignored) | Error; NULL with ANSI off |
| `AVG` of integers | Decimal (`4.5`) | `DOUBLE` |
| `AVG`/`SUM` of `NUMERIC(10,2)` | `AVG` with 17–18 decimals; `SUM` keeps scale 2 | Decimal with widened precision/scale |
| `ORDER BY s ASC` / `DESC` | NULL first / NULL last | Same default |
| `NULLS FIRST` / `NULLS LAST` | **Syntax error** (SQLCODE -25) | Supported |
| Aggregates on empty input | `COUNT(*) = 0`, others `NULL`; `GROUP BY` returns no rows | Same |
| `GROUP BY` a nullable key | One `NULL` group | Same |

**Rules for the compiler:**
1. Guard division: `CASE WHEN d = 0 THEN <ANSI choice> ELSE n / d END`. With ANSI on, Mborobak raises its own error; with ANSI off, NULL. A -400 that kills the query must never reach the user unexplained.
2. Casts from strings to numbers need a validity check (or a Mborobak-side error), because IRIS returns 0 silently.
3. Precision overflow is not detected by IRIS. Matching Spark needs explicit range checks; otherwise this goes into the compatibility matrix as a known difference.
4. Explicit `nulls_first()`/`nulls_last()` compile to an extra `CASE WHEN x IS NULL THEN 0 ELSE 1 END` sort key.
5. Result types differ (Decimal vs. double). `toPandas()` must apply the schema's types, not the driver's.

### 2.7 −0.0, NaN, infinity and time (scope §6.8)

- **−0.0:** stored in a `DOUBLE` column and returned as `-0.0`; `WHERE d = 0` matches it. The literal `-0.0` evaluates to `0`. Spark normalizes −0.0 in grouping and joins; whether IRIS groups `-0.0` with `0.0` was not probed.
- **NaN and ±Infinity:** can be inserted **as parameters** into a `DOUBLE` column and read back as `NaN`/`Infinity`. As SQL literals they fail (IRISpark: temp tables crash on `NAN`), and IRIS functions return errors (-400) where Spark returns NaN or Inf. How IRIS compares and sorts NaN was not probed.
- **Time zones:** `TIMESTAMP` has no time zone. A literal with an offset (`'2026-11-18T10:00:00+02:00'`) is a fatal error (-400). `CURRENT_TIMESTAMP` and `GETUTCDATE()` matched, because the container runs in UTC. Microseconds are kept. Spark's `TimestampType` is an instant shown in the session time zone, so Mborobak must define how timestamps map (most likely: IRIS timestamps are treated as `TimestampNTZ`, timestamps without a time zone).
- **Python types from the driver:** `TIMESTAMP` → `datetime`, **`DATE` → `str`**, `NUMERIC` → `Decimal`, `DOUBLE` → `float`, `INTEGER` → `int`, `BIT` → `bool`. Mborobak must convert `DATE` values itself.

### 2.8 Parallel execution and floating-point sums (scope §8.2 tolerance policy)

500,000 rows of `DOUBLE` values spread from 1e-6 to 1e9, plus a `NUMERIC(14,2)` column:

| Aggregate | Serial vs. `%PARALLEL` |
|-----------|------------------------|
| `SUM(numeric)` | Identical, exact |
| `SUM(double)`, global | Identical here; in a 20k-row run, repeated **serial** runs differed in the last digits (`…719.0291324` vs. `…719.0291328`) |
| `SUM(double)` per group (1,000 groups) | **130 groups differ**, max relative difference 1.6e-14 |
| IRIS `SUM(double)` vs. exact sum (Python `math.fsum`) | Relative difference ≈ 2e-15 |

This confirms the scope's tolerance policy: exact for integers and decimals, relative tolerance (1e-12 is enough here) for floating point. IRISpark used 1e-12/1e-9 for the same reason.

Notes: the hint goes after `FROM` (`FROM %PARALLEL t`); `FROM t %PARALLEL` is a parse error (IRISpark). IRIS also decides on its own to run queries in parallel above a cost threshold (default 3200, per [community docs](https://dev.to/intersystemsdev/parallel-query-processing-system-wide-and-query-based-58j6)), so serial and parallel plans can vary without any hint. Timings at 500k rows were similar with and without the hint (≈40–65 ms), so the tables were probably too small for parallelism to help.

### 2.9 Driver speed (scope §8.3 baseline)

`SELECT TOP n id, k, d, amount, s` (5 columns: int, int, double, numeric, varchar), `fetchall()`:

| Rows | Execute | Fetch | Total rows/s | Building a pandas DataFrame |
|------|---------|-------|--------------|------------------------------|
| 10,000 | 31 ms (first run) | 11 ms | ≈240k | 4 ms |
| 100,000 | 2 ms | 110 ms | ≈890k | 47 ms |
| 500,000 | 3 ms | 483 ms | ≈1.03M | 272 ms |

- Inserts with `executemany` in batches of 10,000: ≈72,000 rows/s.
- For large results the time is almost all fetching, and building the pandas DataFrame from Python rows adds ≈55%. This is where `toPandas()` will spend its time, and the strongest case for a faster transfer path later (P2).
- These numbers are optimistic: loopback network, narrow rows, warm cache, one run each. They are a baseline for the benchmark, not a result.

## 3. Proposed changes

### To the scope (v1.3)

**Status:** S1–S6 applied to [mborobak-scope.md](mborobak-scope.md) v1.3 on 2026-11-18. S2's "never emit ordinals" rule is in §6.12 with the other forbidden constructs. D1 and D10 are still to be decided.

| # | Section | Change |
|---|---------|--------|
| S1 | §6.4 | Remove the claim that parameters in both `SELECT` and `GROUP BY` fail; parameters are for safety, since IRIS caches plans through literal substitution anyway. Add the ~999 literal/parameter limit with an explicit Mborobak error. |
| S2 | §6.2, §6.3 | Write the concrete rules: `ORDER BY` only at the outermost level or with `TOP`/`TOP ALL`; `TOP` inside subqueries; `LIMIT`/`OFFSET` only at the outer level. Never emit `GROUP BY` ordinals. |
| S3 | §6.8 | Replace "document differences" with the measured table (§2.6) and the guards: division, string-to-number casts, NULL-order emulation, `DATE` conversion. |
| S4 | §6 (new rule) | String collation handling (§2.5), following decision D10. |
| S5 | §15 | New **D10**: Spark or IRIS string semantics by default. Suggest **D1 = IRIS 2026.2** unless an older target is required, since that is the only version tested. |
| S6 | §12 | New risk: silent wrong results from collation and casts; mitigation: `%EXACT` rewrite and guards, plus tests with mixed-case and trailing-space data. |

### To the compatibility matrix

Add every row of the tables in §2.5 and §2.6 as a known difference or emulated behavior, depending on D9 and D10.

## 4. Still open

- ~~**Re-run on IRIS 2026.3.0AI build 154**~~ Withdrawn: D1 keeps IRIS 2026.2 (build 2026.2.0.221.0), the build this study used (scope 1.9).
- How IRIS compares, groups and sorts **NaN**, and whether **−0.0 and 0.0 group together**.
- Where to read a column's **collation** from the catalog (`INFORMATION_SCHEMA.COLUMNS.COLLATION_NAME` was NULL).
- In which IRIS version **`LIMIT`/`OFFSET`** appeared (affects D1).
- Whether the cost of **`%EXACT`** (no index use) matters at realistic sizes; measure in the benchmark.
- Driver speed over a **real network** and for **wide rows**.
- Behavior on **non-Community** editions and x86; only one build was tested.
