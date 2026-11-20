# Changelog

## Unreleased

- Spark SQL expression strings: `F.expr`, `selectExpr`, `filter("…")`/`where("…")`, with Spark SQL's literal types, operators, `CASE`, `CAST`, functions, aggregates and windows, and Spark's default column names (compiler spec §7.13, compatibility matrix §2.15).
- **Fix:** a fractional constant beside an integer column (`F.col("i") + 1.5`, `coalesce(i, 1.5)`) is computed exactly; IRIS turned the parameter into an integer (`7 + 1.5` gave 9). Decimal and double constants are now sent as typed parameters (matrix NUM-14).
- **Fix:** default names of `!=` (`(NOT (a = b))`) and of date and timestamp literals (`DATE '2026-01-01'`) are Spark's.
- Date and time functions: `current_date`, `current_timestamp`, `now`, `year`, `month`, `dayofmonth`, `day`, `dayofweek`, `dayofyear`, `weekofyear` (ISO), `quarter`, `hour`, `minute`, `second`, `date_add`, `date_sub`, `add_months`, `datediff`, `months_between`, `last_day`, `trunc`, `date_trunc`, `date_format` (Java patterns) and `to_date`/`to_timestamp` with or without a pattern (compiler spec §7.12, compatibility matrix §2.14).
- **Fix:** casting invalid text to `date` raises `MborobakCastError`, as Spark's ANSI cast does; IRIS returned `1840-12-31` without an error (matrix CAST-18). Text casts to `date` and `timestamp` accept exactly Spark's shapes.
- Window functions: `Window`/`WindowSpec` with `partitionBy`, `orderBy`, `rowsBetween`, `rangeBetween` (unbounded and current-row bounds); `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first`, `last` and aggregates with `.over()`. Spark's names, types, sort order and analysis errors; filtering on window results works (compiler spec §7.11, compatibility matrix §2.13). `rangeBetween` with value offsets raises `UnsupportedOperationError`: IRIS has no `RANGE` frames.
- `dropDuplicates(subset)` and `drop_duplicates`.
- Null, string and math functions: `coalesce`, `ifnull`, `nvl`, `nvl2`, `nullif`, `isnan`, `isnull`, `isnotnull`, `greatest`, `least`; `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `initcap`, `reverse`, `repeat`, `length`, `char_length`, `substring`, `substr` (also `Column.substr`), `concat`, `concat_ws`, `lpad`, `rpad`, `instr`, `locate`, `replace`; `abs`, `round`, `bround`, `ceil`, `ceiling`, `floor`, `pow`, `power`, `sqrt`, `exp`, `ln`, `log`, `log10`, `log2`, `log1p`, `signum`, `sign`, `pmod`. Each has PySpark's result type and default name, and Spark's results where IRIS's functions differ or fail (compiler spec §7.10, compatibility matrix §2.12).
- `fillna`/`na.fill` and `dropna`/`na.drop`, including NaN in double columns.
- **Fix:** strings that look like numbers (`'10'`, `'9'`) now compare, sort and aggregate (`min`, `max`) as text, as in Spark. With `%EXACT`, IRIS ordered them as numbers (matrix STR-16).
- New known differences, each with a strict test: adding `+∞` and `-∞` raises an error (FP-12), `pow` overflow raises an error (FUN-24), integer overflow in `abs`/`round` is not detected (FUN-25), `initcap` starts words after tabs (FUN-22).
- `mborobak-scan`: a notebook compatibility scanner. It lists the PySpark calls in `.ipynb` notebooks, Databricks `.py` exports and scripts, and marks each as supported, planned (with release), not scheduled, or out of scope. Output is text, Markdown or JSON.

## 0.1.0 (2026-11-18)

First release, for the InterSystems Open Exchange contest.

- Lazy, immutable DataFrames over IRIS tables (`session.table`) and IRIS SQL (`session.sql`), compiled to one SQL statement per action.
- Transformations: `select`, `filter`/`where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `orderBy`/`sort`, `limit`, `distinct`, `alias`, `join` (inner, left, right, full, cross), `union`/`unionAll`, `groupBy().agg()` and its shortcuts.
- Actions: `collect`, `show`, `count`, `take`, `first`, `head`, `toPandas`; inspection with `explain()`, `toSQL()`, `schema`, `dtypes` and per-action metrics.
- PySpark 4.3 (ANSI) semantics on IRIS: case-sensitive strings via `%EXACT` (switchable), ANSI errors for division by zero and invalid casts, Spark's `DOUBLE` sort order, Spark's result types and column names.
- Control documents (compiler specification, API contract, compatibility matrix), a differential test suite against PySpark 4.3 and a benchmark suite against hand-written IRIS SQL.
