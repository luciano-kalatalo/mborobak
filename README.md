# Mborobak

**PySpark-style DataFrames for InterSystems IRIS, compiled to one SQL statement and run inside IRIS.**

You write the DataFrame code you already know from PySpark. Mborobak builds a lazy, immutable plan and compiles it to a single IRIS SQL statement with parameters. The query runs inside IRIS when you call an action (`show()`, `collect()`, `count()`, `toPandas()`), and only the result travels to Python. Nothing is computed in Python that IRIS can compute, and there is no Spark or JVM.

```python
from mborobak import MborobakSession
from mborobak.sql import functions as F

session = MborobakSession.builder.config(map={...}).getOrCreate()   # connection details below

revenue = (
    session.table("Sales")
    .filter(F.col("status") == "PAID")
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.col("revenue").desc())
)
revenue.explain()   # the plan, the IRIS SQL and its parameters; nothing has run yet
revenue.show()      # one IRIS query
```

```sql
SELECT "customer_id", SUM("amount") AS "revenue"
FROM "SQLUser"."Sales"
WHERE %EXACT("status") = ?
GROUP BY "customer_id"
ORDER BY SUM("amount") DESC
```

## Why

IRIS is fast at SQL, and many data teams think in DataFrames. Mborobak gives them the PySpark API without moving data out of IRIS, and makes the results match what PySpark would return, including where IRIS's defaults differ:

- **Strings compare case-sensitively,** as in Spark. IRIS's default collation treats `'PAID'` and `'paid'` as equal and returns grouped values uppercased. Mborobak adds `%EXACT` where it changes the result, reads each column's collation from the catalog to skip it where it isn't needed, and offers `mborobak.sql.stringSemantics = "iris"` for IRIS's rules.
- **Spark's ANSI errors.** Division by zero, invalid casts (`'abc'` to `int`, which IRIS silently turns into `0`) and casting NaN to an integer raise clear Mborobak errors instead of returning wrong values.
- **Spark's sort order for `DOUBLE`.** IRIS sorts −Infinity after the largest numbers and uses −1E19 internally as its NULL marker when sorting, so a stored −1E19 comes back as NULL. Mborobak restores Spark's order and values.
- **Spark's string order.** With `%EXACT`, IRIS sorts strings that look like numbers as numbers (`'10'` after `'9'`); Mborobak compares and sorts them as text, as Spark does.
- **Spark's results for edge cases in functions:** `round(0.285, 2)` is 0.29, `sqrt(-1)` is NaN, `log(0)` is NULL, `substring` with negative positions, `greatest` skipping NULLs, where IRIS's functions differ or fail.
- **Spark's types and names:** `int / int` is a `double`, `sum(int)` is a `bigint`, decimal precision follows Spark's rules, and unaliased columns are named `sum(amount)`, `count(1)`, `(a + b)`.
- **What IRIS SQL requires:** `TOP` instead of `LIMIT` in subqueries, `ORDER BY` only where IRIS allows it, `WITH` clauses moved to the front of the statement, no `GROUP BY` positions (IRIS reads `GROUP BY 1` as the constant 1), and NaN and infinity sent as parameters.

## Features

| Area | Supported |
|------|-----------|
| Sources | `session.table(name)`, `session.read.table(name)`, `session.sql(query, args)` (IRIS SQL with `?` or `:name` parameters), `session.createDataFrame(rows or pandas, schema)` (loaded into a temporary IRIS table) |
| Transformations | `select`, `filter`/`where`, `withColumn(s)`, `withColumnRenamed`, `drop`, `orderBy`/`sort`, `limit`, `distinct`, `alias` |
| Joins | inner, left, right, full, cross, semi, anti; on a column name, a list of names, or conditions; self-joins via `alias()` |
| Aggregation | `groupBy().agg()`, `count`, `sum`, `avg`/`mean`, `min`, `max`, `countDistinct`; `agg({"x": "sum"})`; filters after `agg()` |
| Union | `union`/`unionAll` by position, with Spark's numeric widening; `unionByName`, with `allowMissingColumns` |
| Expressions | arithmetic, comparisons, `&`, `\|`, `~`, `when().otherwise()`, `cast`, `isNull`, `isin`, `like`, `between`, `eqNullSafe`, sort orders with null placement |
| Functions | null handling (`coalesce`, `nullif`, `nvl`, `nvl2`, `greatest`, `least`, `isnan`, `isnull`); strings (`upper`, `lower`, `trim`, `length`, `substring`, `concat`, `concat_ws`, `lpad`, `rpad`, `instr`, `locate`, `replace`, `initcap`, `reverse`, `repeat`); math (`round`, `bround`, `abs`, `ceil`, `floor`, `pow`, `sqrt`, `exp`, `log`, `log10`, `log2`, `log1p`, `signum`, `pmod`) |
| Missing values | `fillna`/`na.fill`, `dropna`/`na.drop` |
| Dates and times | `year` … `second`, `weekofyear`, `date_add`, `date_sub`, `add_months`, `datediff`, `months_between`, `last_day`, `trunc`, `date_trunc`, `date_format`, `to_date`, `to_timestamp`, `current_date`, `current_timestamp` |
| SQL strings | `F.expr("…")`, `selectExpr("…")`, `filter("…")`/`where("…")` with Spark SQL expressions, functions and windows |
| Windows | `Window.partitionBy().orderBy().rowsBetween()/rangeBetween()`; `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first`, `last`, and aggregates `.over(window)`; filtering on window results (top-N per group); `dropDuplicates` |
| Actions | `collect`, `show`, `count`, `take`, `first`, `head`, `toPandas` |
| Writing tables | `df.write.saveAsTable(name)` with modes `errorifexists`, `append`, `overwrite`, `ignore`; `insertInto`. Runs as `CREATE TABLE` plus `INSERT … SELECT` in one transaction, so the rows never leave IRIS |
| Inspection | `schema`, `columns`, `dtypes`, `printSchema`, `explain()`, `toSQL()`, per-action metrics in `session.lastExecution` |

Transformations never run queries. Each action runs exactly one data query; table schemas are looked up once and cached.

## Installation

Requires Python 3.11 or newer and the official IRIS driver, which is installed with the package.

```bash
pip install "mborobak[pandas] @ git+https://github.com/luciano-kalatalo/mborobak"
```

From a clone, for development:

```bash
git clone https://github.com/luciano-kalatalo/mborobak && cd mborobak
python3.11 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Running IRIS

Mborobak is developed and tested on **IRIS 2026.2** (build 221U); it needs IRIS 2026.2 or later. To run IRIS locally with the included `docker-compose.yml`, point `IRIS_IMAGE` at an IRIS Community image you have pulled or loaded, then start it:

```bash
export IRIS_IMAGE=<an IRIS 2026.2+ community image>
docker compose up -d
```

A fresh community container asks for a password change on first login. For a local, throwaway container you can clear that once:

```bash
docker compose exec iris iris session IRIS -U %SYS '##class(Security.Users).UnExpireUserPasswords("*")'
```

Then connect with the image's default credentials (`_SYSTEM` / `SYS`, namespace `USER`), or with your own user on a real instance.

## Connecting

```python
import os
from mborobak import MborobakSession

session = (
    MborobakSession.builder
    .config("host", os.environ["IRIS_HOST"])
    .config("port", int(os.environ.get("IRIS_PORT", 1972)))
    .config("namespace", os.environ.get("IRIS_NAMESPACE", "USER"))
    .config("user", os.environ["IRIS_USERNAME"])
    .config("password", os.environ["IRIS_PASSWORD"])
    .getOrCreate()
)
```

Or pass an existing connection: `MborobakSession.builder.connection(iris.connect(...)).getOrCreate()`. Keep credentials in the environment or a secrets store, not in code.

## Demo

[`examples/demo.py`](examples/demo.py) creates two small tables and walks through the features: a lazy pipeline and its `explain()`, one query per action, PySpark vs. IRIS string rules, joins and self-joins, union, NaN and infinity in sorting, ANSI casts and errors, `session.sql()`, and `toPandas()`.

```bash
export IRIS_HOST=localhost IRIS_PORT=1972 IRIS_NAMESPACE=USER IRIS_USERNAME=_SYSTEM IRIS_PASSWORD=SYS
python examples/demo.py
```

An excerpt of its output:

```
3. PySpark semantics on IRIS: 'PAID' and 'paid' are different values
+------+-----+
|status|count|
+------+-----+
|  OPEN|    1|
|  PAID|    5|
|  paid|    1|
+------+-----+

5. Edge cases handled like Spark: NaN, infinity, NULL and -1e19 in a DOUBLE sort
+---+---------+
| id|    score|
+---+---------+
|  2|     NULL|
|  5|-Infinity|
|  3|  -1.0E19|
|  7|      0.0|
|  1|      1.5|
|  6|      2.5|
|  4|      NaN|
+---+---------+
```

## Will my notebook run? (compatibility scanner)

`mborobak-scan` reads PySpark notebooks (`.ipynb`, Databricks `.py` exports) or scripts without running them. It lists every PySpark call and whether Mborobak supports it today, plans it (with the release), or leaves it out of scope (`display()`, `spark.read`, MLlib, streaming).

```bash
mborobak-scan my_notebook.ipynb            # summary by status
mborobak-scan notebooks/ --details         # every call, with cell and line
mborobak-scan my_notebook.ipynb --format markdown > scan.md   # or --format json
```

```
Mborobak compatibility scan: 1 file(s)
76 PySpark calls, 50 distinct: 42 supported, 29 planned, 1 not scheduled, 4 out of scope
Calls supported: today 55%, after 0.2 88%, after 0.2.x 91%, after 0.3 93%
...
```

It matches calls by name, not by type, so treat its figures as an estimate.

## How it works

```
DataFrame calls ──► immutable logical plan ──► analyzer (names, types; errors at the call that caused them)
   ──► required IRIS translations (collation, ordering, limits, casts) ──► SQL generation
   ──► one IRIS statement + parameters ──► values converted to the schema's types ──► Rows / pandas
```

The design is specified in three control documents:

- [SQL compiler specification](docs/compiler-spec.md): plan nodes, typing, the IRIS translations and SQL generation.
- [API contract](docs/api-contract.md): every public call, its arguments, errors and the queries it may run.
- [Compatibility matrix](docs/compatibility-matrix.md): behavior by behavior, how Mborobak on IRIS compares with PySpark 4.3 (ANSI on), and what it does about each difference.

They rest on measurements: an [IRIS behavior study](docs/iris-behavior-study.md) and follow-up probes in [`probes/`](probes/).

## Compatibility and performance

- **Compared with PySpark 4.3.** A differential suite runs the same DataFrame code on Mborobak and on PySpark with ANSI mode on, and compares names, types and rows. 157 matrix rows are checked this way: 138 match, 16 are documented known differences that the suite confirms, and 3 confirm that Mborobak refuses what IRIS can't do (`rangeBetween` with value offsets, regular expressions, column names IRIS can't hold). The main known differences are integer and decimal overflow (not detected), `max` ignoring NaN, IRIS's precision for very wide decimal division, and `timestamp_ntz` where Spark returns `timestamp`.
- **Benchmarked against hand-written IRIS SQL** through the same driver, at 10k to 1M rows ([report](docs/benchmark-report-2026-11-18.md)). Every result matches. Planning and compilation take about 0.05 ms per action. Queries returning few rows run at hand-written speed; returning many rows costs extra for converting values to Python types (about +20% for 100k rows returned).

## Tests

```bash
.venv/bin/pytest tests/unit          # compiler and API, no IRIS needed
.venv/bin/pytest tests/integration   # against IRIS: needs IRIS_HOST, IRIS_PORT, IRIS_NAMESPACE, IRIS_USERNAME, IRIS_PASSWORD
.venv/bin/pytest tests/compat        # against IRIS and PySpark 4.3 (pip install -e ".[compat]", Java 17+)
```

## Status and roadmap

Version 0.2.1. Release 0.1 was built for the InterSystems Open Exchange contest and covered the P0 scope in [docs/mborobak-scope.md](docs/mborobak-scope.md), where the full roadmap lives (§13). Release 0.2 adds what PySpark users on Databricks reach for first.

- **0.2, Analytical functions (released):** for proofs of concept with Databricks users ([plan](docs/poc-readiness-plan.md)): the notebook compatibility scanner; null, string, math, date and time functions; `fillna`/`dropna`; window functions and `dropDuplicates`; `F.expr`/`selectExpr`/`filter("…")`.
- **0.2.x, Pipelines:** writing tables (`saveAsTable`, `insertInto`, released in 0.2.1); semi and anti joins, `unionByName`, `spark.read.table`, named `sql()` parameters and `createDataFrame` (done).
- **0.3, Advanced execution:** regular expressions (IRIS SQL has none), **columnar (Arrow-style) result transfer**, so large results reach pandas as whole columns instead of row by row (today about 1.7 s per million rows against 0.85 s for the raw driver); research on server-side UDFs, vectorized execution and parallelism.
- **1.0, Production readiness:** stable API, documented compatibility, robustness testing, observability, security, and CI on every supported Python version.

## License

MIT, see [LICENSE](LICENSE).
