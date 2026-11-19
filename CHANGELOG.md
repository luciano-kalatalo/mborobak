# Changelog

## Unreleased

- `mborobak-scan`: a notebook compatibility scanner. It lists the PySpark calls in `.ipynb` notebooks, Databricks `.py` exports and scripts, and marks each as supported, planned (with release), not scheduled, or out of scope. Output is text, Markdown or JSON.

## 0.1.0 (2026-11-18)

First release, for the InterSystems Open Exchange contest.

- Lazy, immutable DataFrames over IRIS tables (`session.table`) and IRIS SQL (`session.sql`), compiled to one SQL statement per action.
- Transformations: `select`, `filter`/`where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `orderBy`/`sort`, `limit`, `distinct`, `alias`, `join` (inner, left, right, full, cross), `union`/`unionAll`, `groupBy().agg()` and its shortcuts.
- Actions: `collect`, `show`, `count`, `take`, `first`, `head`, `toPandas`; inspection with `explain()`, `toSQL()`, `schema`, `dtypes` and per-action metrics.
- PySpark 4.3 (ANSI) semantics on IRIS: case-sensitive strings via `%EXACT` (switchable), ANSI errors for division by zero and invalid casts, Spark's `DOUBLE` sort order, Spark's result types and column names.
- Control documents (compiler specification, API contract, compatibility matrix), a differential test suite against PySpark 4.3 and a benchmark suite against hand-written IRIS SQL.
