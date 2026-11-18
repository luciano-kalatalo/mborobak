# Changelog

## 0.1.0 (2026-11-18)

First release, for the InterSystems Open Exchange contest.

- Lazy, immutable DataFrames over IRIS tables (`session.table`) and IRIS SQL (`session.sql`), compiled to one SQL statement per action.
- Transformations: `select`, `filter`/`where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `orderBy`/`sort`, `limit`, `distinct`, `alias`, `join` (inner, left, right, full, cross), `union`/`unionAll`, `groupBy().agg()` and its shortcuts.
- Actions: `collect`, `show`, `count`, `take`, `first`, `head`, `toPandas`; inspection with `explain()`, `toSQL()`, `schema`, `dtypes` and per-action metrics.
- PySpark 4.3 (ANSI) semantics on IRIS: case-sensitive strings via `%EXACT` (switchable), ANSI errors for division by zero and invalid casts, Spark's `DOUBLE` sort order, Spark's result types and column names.
- Control documents (compiler specification, API contract, compatibility matrix), a differential test suite against PySpark 4.3 and a benchmark suite against hand-written IRIS SQL.
