# Changelog

## Unreleased

- Faster results: stored `NUMERIC` columns skip re-rounding when the driver already returns their scale, numeric pandas columns are built with numpy, and rows are fetched in batches. `toPandas()` on 1M rows takes about 2.5 s instead of 3.0 s, 13% over the IRIS driver plus pandas (was 29%), with the same values and types. The planned columnar (Arrow) transfer is not possible on IRIS 2026.2: the driver has no columnar API and IRIS iterates a query no faster than the driver delivers it (compiler spec §9.9, §12.9).
- **Changed:** `session.sql()` takes Spark SQL `SELECT` statements by default (`WITH`, joins including semi and anti, `GROUP BY` aliases, ordinals and `ALL`, `HAVING`, `ORDER BY` aliases, `UNION`, `LIMIT`, `?`/`:name` parameters and `{df}` placeholders) and builds them from DataFrame operations, with Spark's names, types and semantics, as one IRIS query. Notebook `%sql` cells run as `spark.sql("…")`. Code that passes IRIS-specific SQL (IRIS functions, `TOP`, `%EXACT`) sets `conf.set("mborobak.sql.dialect", "iris")`, the behavior of 0.2 (compiler spec §7.17, API contract §3.5, matrix §2.19).
- Regular expressions: `Column.rlike`, `rlike`, `regexp`, `regexp_like`, `regexp_replace`, `regexp_extract`, `regexp_count`, `regexp_substr`, `regexp_instr`, and `RLIKE`/`REGEXP` in SQL expression strings, with Spark's names, types and results. IRIS SQL has no regular expressions; Mborobak creates small ICU-regex SQL functions in the schema `Mborobak` the first time a session needs one (needs `CREATE FUNCTION` permission; `session.installFunctions()` lets an administrator do it once). ICU's `\w`, `\d` and `\p{…}` are Unicode-aware where Java's are ASCII (compiler spec §7.16, matrix §2.18).

## 0.2.2 (2026-11-20)

The rest of tier 2 of the [POC readiness plan](docs/poc-readiness-plan.md): `createDataFrame`, semi and anti joins, `unionByName`, `spark.read.table` and named `sql()` parameters. On the public Databricks notebooks, 94% of the PySpark calls that matter in a POC are now supported (93% in 0.2.1); what remains is planned for 0.3.

- `createDataFrame(data, schema=None)`: local rows (tuples, lists, `Row`s, dicts) or a pandas DataFrame, with a `StructType`, a DDL string, column names or inferred types, checked as PySpark does. The rows go into a temporary IRIS table that only this connection sees; `stop()` drops it. The result joins with IRIS tables and can be saved with `df.write` (compiler spec §9.8, API contract §3.6, matrix §2.17).
- Semi and anti joins: `join(other, on, "left_semi")` and `"left_anti"` (with the aliases `semi`, `leftsemi`, `anti`, `leftanti`), by condition or by column names. They compile to `[NOT] EXISTS`, so a left row never repeats (compiler spec §7.15, matrix JOIN-05).
- `unionByName(other, allowMissingColumns=False)`: union with columns matched by name, case-insensitively; missing columns become NULL with `allowMissingColumns` (matrix UNI-05).
- `spark.read.table(name)`, the same as `spark.table(name)`. File readers (`read.csv`, `read.parquet`, `read.load` …) raise `UnsupportedOperationError`.
- Named `sql()` parameters: `session.sql("… WHERE id > :low", {"low": 5})`, bound as parameters (API contract §3.5).
- Known difference FP-10 now also covers semi and anti joins: on a double key, a NULL matches the value `-1E19`, which IRIS uses as its internal NULL marker.

## 0.2.1 (2026-11-20)

Writing tables, the first tier-2 item of the [POC readiness plan](docs/poc-readiness-plan.md), and a fix for comparisons with NaN. On the public Databricks notebooks, 93% of the PySpark calls that matter in a POC are now supported (91% in 0.2.0).

- Writing tables: `df.write.saveAsTable(name)` with save modes `errorifexists` (default), `append` (columns matched by name, cast by Spark's ANSI store-assignment rules), `overwrite` (new columns; may read the table it replaces) and `ignore`, and `df.write.insertInto(name, overwrite)`. The DataFrame's query runs as `CREATE TABLE` plus `INSERT … SELECT` in one transaction, so the rows never leave IRIS and a failed write changes nothing. String columns are created with `EXACT` collation, so they compare as in Spark; the new setting `mborobak.write.stringLength` sets their length. `format()`, `partitionBy()` and options are accepted and have no effect; file writers (`save`, `parquet`, `csv`) raise `UnsupportedOperationError` (compiler spec §9.7, API contract §4.7, compatibility matrix §2.16).
- New known difference: decimals written to IRIS keep at most 19 digits before the point, so `decimal(30,2)` reads back as `decimal(21,2)` (matrix WRITE-07).
- `mborobak-scan` reports `saveAsTable` and `insertInto` as supported.
- **Fix:** comparisons with NaN follow Spark, which orders NaN above every value and treats NaN = NaN as true. IRIS put NaN below every value and its answers varied with the plan, so `filter(d > 0)` dropped NaN rows and `d < e` was wrong when a side was NaN. Comparisons, `isin`, `between` and `eqNullSafe` on doubles now test for NaN first (compiler spec §7.14, matrix FP-13).

## 0.2.0 (2026-11-20)

Analytical functions, for proofs of concept with Databricks users (tier 1 of the [POC readiness plan](docs/poc-readiness-plan.md)). On a set of public Databricks notebooks, 91% of the PySpark calls that matter in a POC are now supported. The rest are platform calls such as `display()` and file reads.

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
- New known differences, each with a strict test: adding `+∞` and `-∞` raises an error (FP-12), `pow` overflow raises an error (FUN-24), integer overflow in `abs`/`round` is not detected (FUN-25), `initcap` starts words after tabs (FUN-22), `to_timestamp`/`date_trunc`/`current_timestamp` return `timestamp_ntz` where Spark returns `timestamp` (TIME-05).
- `mborobak-scan`: a notebook compatibility scanner. It lists the PySpark calls in `.ipynb` notebooks, Databricks `.py` exports and scripts, and marks each as supported, planned (with release), not scheduled, or out of scope. Output is text, Markdown or JSON.

## 0.1.0 (2026-11-18)

First release, for the InterSystems Open Exchange contest.

- Lazy, immutable DataFrames over IRIS tables (`session.table`) and IRIS SQL (`session.sql`), compiled to one SQL statement per action.
- Transformations: `select`, `filter`/`where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `orderBy`/`sort`, `limit`, `distinct`, `alias`, `join` (inner, left, right, full, cross), `union`/`unionAll`, `groupBy().agg()` and its shortcuts.
- Actions: `collect`, `show`, `count`, `take`, `first`, `head`, `toPandas`; inspection with `explain()`, `toSQL()`, `schema`, `dtypes` and per-action metrics.
- PySpark 4.3 (ANSI) semantics on IRIS: case-sensitive strings via `%EXACT` (switchable), ANSI errors for division by zero and invalid casts, Spark's `DOUBLE` sort order, Spark's result types and column names.
- Control documents (compiler specification, API contract, compatibility matrix), a differential test suite against PySpark 4.3 and a benchmark suite against hand-written IRIS SQL.
