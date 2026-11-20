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

### Tier 0: know what a client's code needs (done)

**Notebook compatibility scanner** ([`src/mborobak/scan.py`](../src/mborobak/scan.py)). `mborobak-scan notebook.ipynb` (also Databricks `.py` exports, plain scripts and whole directories) lists every PySpark call the code makes and marks each as supported, planned (with its release), not scheduled, or out of scope, with where it occurs (`--details`) and as text, Markdown or JSON. It reads the code without running it. "Supported" is read from the installed Mborobak, so the scanner never claims more than the code provides; "planned" follows this plan. It classifies by name, not by type (it can't tell a DataFrame's `count()` from a list's), skipping calls on non-Spark modules, string literals and pandas variables, so its figures are an estimate to start a POC from. First results are in §5.

### Tier 1: release 0.2, the POC core

| Item | Contents | IRIS notes | Size |
|------|----------|-----------|------|
| **Window functions** | `Window.partitionBy().orderBy()`, `rowsBetween`, `rangeBetween` (where IRIS supports it); `row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile`, `lag`, `lead`, `first`, `last`; `sum`/`avg`/`min`/`max`/`count` over a window | All core functions probed OK; window results filtered through a subquery; `%EXACT` rules apply to string partition and order keys | Large |
| **Null and conditional functions** | `coalesce`, `nullif`, `isnan`, `isnull`, `greatest`, `least`, `ifnull`/`nvl`; `fillna`/`na.fill` and `dropna`/`na.drop` | `isnan` via `x <> x` inside `CASE` (compiler spec §7.9); `fillna` is `COALESCE` per column, `dropna` a filter on `IS NOT NULL` | Small |
| **String functions** | `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `length`, `substring`, `concat`, `concat_ws`, `lpad`, `rpad`, `instr`, `locate`, `replace`, `initcap`, `reverse` | `length` → `CHAR_LENGTH`; `concat_ws` skips NULLs; collation: functions on `%EXACT` columns | Medium |
| **Math functions** | `round`, `bround`, `abs`, `ceil`, `floor`, `pow`, `sqrt`, `exp`, `log`, `mod`/`pmod` | `round` direct; result types follow Spark; `log(0)`/`sqrt(-1)` raise in IRIS where Spark returns NaN (matrix FP-11) | Small |
| **Date and time functions** | `current_date`, `current_timestamp`, `year` … `second`, `dayofweek`, `quarter`, `date_add`, `date_sub`, `datediff`, `add_months`, `months_between`, `last_day`, `to_date`, `to_timestamp`, `date_trunc`, `date_format` (common patterns) | `DATEADD` → cast to date; `DATEDIFF` argument order; `date_trunc` emulated; format-pattern translation; timestamp formatting to be probed | Medium |
| **SQL-string expressions** | `F.expr(...)`, `df.selectExpr(...)`, `df.filter("x > 1")` | A parser for the subset of Spark SQL expressions that the functions above cover; compiles to the same expression nodes | Medium |
| **`dropDuplicates(subset)`** | Keeps one row per key | `ROW_NUMBER() OVER (PARTITION BY …)` = 1, which the window work provides | Small |

**Exit criterion for 0.2:** the scanner reports at least 80% of calls as supported, not counting out-of-scope calls (a POC rewrites `display()` and file reads anyway), on two or three representative client notebooks or, until there are some, on the public notebook set of §5; and every new function has a passing PySpark comparison row or a documented known difference.

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
2. ~~Null, string and math functions~~ Done, with `fillna`/`dropna` (compiler spec §7.10, matrix §2.12). Building them uncovered a fix for 0.1: strings that look like numbers sorted as numbers (matrix STR-16).
3. ~~Window functions~~ Done, with `dropDuplicates` (compiler spec §7.11, matrix §2.13). `rangeBetween` with value offsets stays unsupported: IRIS has no `RANGE` frames.
4. ~~Date and time functions~~ Done, after a focused IRIS probe (compiler spec §7.12, matrix §2.14). Building them uncovered a fix for 0.1: invalid text cast to a date returned `1840-12-31` (matrix CAST-18).
5. ~~SQL-string expressions~~ Done (compiler spec §7.13, matrix §2.15); `dropDuplicates` came with windows. This completes tier 1. Building the parser uncovered a fix for 0.1: a fractional constant beside an integer column was rounded (`7 + 1.5` gave 9, matrix NUM-14).
6. ~~Release 0.2~~ Released as 0.2.0 on 2026-11-20. Then tier 2 as POCs ask for it, in 0.2.x releases.

## 5. First scan: public Databricks notebooks (2026-11-19)

No client notebooks were available yet, so the scanner ran on eight public notebooks from Databricks' own demo repository, [`databricks-demos/dbdemos-notebooks`](https://github.com/databricks-demos/dbdemos-notebooks): the silver and gold transformations of the retail customer-360 demo, the silver layer of the fraud-detection demo, feature engineering for credit decisioning, patient readmission (analysis and feature engineering), the MLOps quick-start feature engineering, and the gold layer of the streaming sessionization demo. They lean toward pipelines and machine learning more than a typical analytics POC would, so they are a demanding test. A synthetic analytics notebook in [`examples/notebooks/`](../examples/notebooks/) serves as the second sample.

| Calls supported | dbdemos, all calls | dbdemos, without out-of-scope | Sample notebook, without out-of-scope |
|-----------------|-------------------:|------------------------------:|--------------------------------------:|
| Today (0.1) | 67% | 78% | 58% |
| After 0.2 | 78% | 91% | 93% |
| After 0.2.x | 80% | 93% | 96% |
| After 0.3 | 85% | 98% | 99% |

The dbdemos set has 197 PySpark calls. What it shows:

- **The core already carries most of the code.** `col`, `alias`, `withColumn`, `spark.table`, `cast`, `join`, `when`/`otherwise` and `groupBy`/`agg` are 132 of the 197 calls.
- **Planned 0.2 items, ranked by use:** `dropDuplicates` (5), `to_timestamp` (4), `fillna`/`na.fill` (4), `current_timestamp` (3), `datediff` (3), `initcap` (2), then windows, `first` and SQL-string filters. This confirms the order of §4, and it brought `fillna`/`dropna` into tier 1.
- **`%sql` cells are common (7).** Spark SQL in notebooks matters more than tier 3 suggests. Many such cells are plain `SELECT … GROUP BY` that IRIS runs as is, so a POC can move them to `session.sql()` today. Translating the dialect stays in 0.3.
- **Out of scope:** `display` (10), file and stream reads (10) and Lakeflow pipeline decorators (3) are 27 calls. They are platform code that a POC replaces, not analytics.

With 0.2 as planned, both samples pass the 80% exit criterion (91% and 93%).

After the null, string and math functions (step 2 of §4), in-scope coverage today is 79% on the dbdemos set (from 78%) and 67% on the sample notebook (from 58%). The dbdemos set gains little because what it still needs is mostly dates, `dropDuplicates` and windows, steps 3 to 5.

After window functions and `dropDuplicates` (step 3), in-scope coverage today is **84%** on the dbdemos set and **88%** on the sample: both now pass the 80% exit criterion. What remains planned in the dbdemos set is date functions (`to_timestamp`, `current_timestamp`, `datediff`: 10 calls) and one SQL-string filter.

After date and time functions (step 4), in-scope coverage today is **90%** on both the dbdemos set and the sample notebook. Of the 0.2 items, only SQL-string expressions remain (step 5); the rest of the dbdemos gap is `saveAsTable` (0.2.x), `%sql` cells and regular expressions (0.3).

With SQL-string expressions (step 5), tier 1 is complete: in-scope coverage today is **91%** on the dbdemos set and **93%** on the sample. Nothing planned for 0.2 is left in either; what remains is tier 2 (`saveAsTable`, 4 calls) and tier 3 (`%sql` cells and regular expressions, 9 calls).
