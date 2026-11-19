# POC Readiness Plan

Scientificloud · Mborobak · 2026-11-19 · plan for release 0.2

## 1. Goal

Make Mborobak convincing in proofs of concept with clients who use Databricks. Their PySpark code is full of column functions (`coalesce`, `upper`, `to_date`, `date_trunc`, `round` …) and window functions (`row_number`, `lag`, running totals), and much of it writes expressions as SQL strings (`F.expr`, `selectExpr`). Release 0.1 has about 15 functions and no windows, so most notebook code fails on its first unfamiliar call.

This plan reorders release 0.2 around what those users reach for first. Everything below follows the same discipline as 0.1: an IRIS probe, then the compiler specification, then unit tests, IRIS integration tests and a PySpark comparison row per behavior in the [compatibility matrix](compatibility-matrix.md).

## 2. What IRIS offers (probe 2026-11-19)

Script [`probes/function_probe.py`](../probes/function_probe.py), raw output [`probes/results-2026-11-19-functions-iris-2026.2.json`](../probes/results-2026-11-19-functions-iris-2026.2.json), IRIS 2026.2.

| Area | IRIS 2026.2 | Consequence for Mborobak |
|------|-------------|--------------------------|
| Window functions | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `LAG`, `LEAD`, `FIRST_VALUE`, `SUM … OVER (PARTITION BY … ORDER BY … ROWS BETWEEN …)`, `AVG … OVER` all work; filtering on a window result works through a subquery | Direct mapping; the compiler wraps a filter on a window column in a subquery, as Spark plans it |
| Case and trimming | `UPPER`, `LOWER`, `TRIM`, `LTRIM`, `RTRIM` work | Direct mapping |
| Length | **`LENGTH` ignores trailing spaces** (`'Hello World '` → 11); `CHAR_LENGTH` counts them (12, as Spark) | Render `length()` as `CHAR_LENGTH` |
| Substring, position, padding, replace | `SUBSTRING`, `INSTR`, `POSITION`, `LPAD`, `REPLACE` work (1-based, as Spark) | Direct mapping; check `substring` with negative positions |
| Concatenation | `\|\|` and `CONCAT` return NULL if any part is NULL, as Spark's `concat` | Direct; `concat_ws` (which skips NULLs in Spark) needs `CASE` handling |
| NULL handling | `COALESCE`, `NULLIF` work | Direct |
| Rounding | `ROUND(2.5)` → 3, `ROUND(-2.5)` → −3 (half away from zero, as Spark's `round`) | Direct; Spark's `bround` (half even) needs emulation |
| Math | `ABS`, `CEILING`, `FLOOR`, `POWER`, `SQRT` work | Direct; check result types against Spark |
| Date parts | `YEAR`, `MONTH`, `DAYOFMONTH`, `DAYOFWEEK` (Sunday = 1, as Spark), `QUARTER`, `HOUR`, `MINUTE`, `SECOND` work | Direct |
| Date arithmetic | `DATEADD` works but **returns a timestamp**; `DATEDIFF('dd', a, b)` takes its dates **in the opposite order** to Spark's `datediff(end, start)` | Cast back to `DATE`; swap arguments |
| Month end, parsing | `LAST_DAY`, `TO_DATE`, `TO_CHAR` of a date work | Direct for simple patterns; Spark's format letters (`yyyy-MM-dd`) must be translated to IRIS's (`YYYY-MM-DD`) |
| Truncation | **No `DATE_TRUNC` or `TRUNC`** for dates | Emulate (`'month'` → first day from `YEAR`/`MONTH`, and so on) |
| Timestamp formatting | **`TO_CHAR(ts, 'YYYY-MM-DD HH24:MI')` fails** (SQLCODE -400) | Needs more probing before `date_format` on timestamps can be offered |
| Regular expressions | **No regex in IRIS SQL:** `$MATCH` is rejected in SQL; `%MATCHES` is IRIS's own pattern syntax, not regex | `rlike`, `regexp_replace`, `regexp_extract` can't be compiled to plain SQL; see tier 3 |

## 3. Plan by tier

### Tier 0: know what a client's code needs (do first)

**Notebook compatibility scanner.** A small command-line tool: `mborobak-scan notebook.ipynb` (or `.py`) lists every PySpark call and function the code uses and marks each as supported, planned (with release), or not planned. Before a POC it tells us, from the client's real notebooks, what will work and what to build first. Size: small.

### Tier 1: release 0.2, the POC core

| Item | Contents | IRIS notes | Size |
|------|----------|-----------|------|
| **Window functions** | `Window.partitionBy().orderBy()`, `rowsBetween`, `rangeBetween` (where IRIS supports it); `row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile`, `lag`, `lead`, `first`, `last`; `sum`/`avg`/`min`/`max`/`count` over a window | All core functions probed OK; window results filtered through a subquery; `%EXACT` rules apply to string partition and order keys | Large |
| **Null and conditional functions** | `coalesce`, `nullif`, `isnan`, `greatest`, `least`, `ifnull`/`nvl` | `isnan` via `x <> x` inside `CASE` (compiler spec §7.9) | Small |
| **String functions** | `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`, `substring`, `concat`, `concat_ws`, `lpad`, `rpad`, `instr`, `locate`, `replace`, `initcap`, `reverse` | `length` → `CHAR_LENGTH`; `concat_ws` skips NULLs; collation: functions on `%EXACT` columns | Medium |
| **Math functions** | `round`, `bround`, `abs`, `ceil`, `floor`, `pow`, `sqrt`, `exp`, `log`, `mod`/`pmod` | `round` direct; result types follow Spark; `log(0)`/`sqrt(-1)` raise in IRIS where Spark returns NaN (matrix FP-11) | Small |
| **Date and time functions** | `current_date`, `current_timestamp`, `year` … `second`, `dayofweek`, `quarter`, `date_add`, `date_sub`, `datediff`, `add_months`, `months_between`, `last_day`, `to_date`, `to_timestamp`, `date_trunc`, `date_format` (common patterns) | `DATEADD` → cast to date; `DATEDIFF` argument order; `date_trunc` emulated; format-pattern translation; timestamp formatting to be probed | Medium |
| **SQL-string expressions** | `F.expr(...)`, `df.selectExpr(...)`, `df.filter("x > 1")` | A parser for the subset of Spark SQL expressions that the functions above cover; compiles to the same expression nodes | Medium |
| **`dropDuplicates(subset)`** | Keeps one row per key | `ROW_NUMBER() OVER (PARTITION BY …)` = 1, which the window work provides | Small |

**Exit criterion for 0.2:** the scanner reports at least 80% of calls as supported on two or three representative client notebooks (or a public set of typical Databricks analytics notebooks), and every new function has a passing PySpark comparison row or a documented known difference.

### Tier 2: release 0.2.x, round out pipelines

- **`createDataFrame`** from Python data or pandas, as a temporary table in IRIS. Needs write permission; size medium.
- **Writing tables:** `df.write.saveAsTable(name, mode=...)` via `CREATE TABLE` plus `INSERT … SELECT`, so the data never leaves IRIS. Size medium.
- **`unionByName`**, **semi and anti joins**, **named `sql()` parameters**. Small each.

### Tier 3: needs research (release 0.3)

- **Regular expressions** (`rlike`, `regexp_replace`, `regexp_extract`). IRIS SQL has no regex functions. Options: an opt-in server-side SQL function (Embedded Python or ObjectScript `$MATCH`/`$LOCATE`), enabled by administrators like the columnar transfer (scope §13); or translating simple patterns to `LIKE`. Until then they raise a clear `UnsupportedOperationError`.
- **Spark SQL in `session.sql()`:** translating Spark's SQL dialect to IRIS SQL, beyond the expression subset of tier 1.
- **Columnar result transfer** (already on the 0.3 roadmap).

### Out of reach for Mborobak, to state up front in POCs

Reading files and Delta tables (`spark.read`), Databricks-specific APIs (`display()`, `dbutils`), MLlib, streaming, and `cache()`. In a POC, the data is already in IRIS.

## 4. Suggested order

1. Tier 0 scanner, and run it on whatever client or sample notebooks are available, so the next steps follow real usage.
2. Null, string and math functions (small and fast, high coverage gain).
3. Window functions (largest single item).
4. Date and time functions (after a second, focused IRIS probe on formatting and truncation).
5. SQL-string expressions and `dropDuplicates`.
6. Release 0.2, then tier 2 as POCs ask for it.
