# API Contract

Scientificloud · Mborobak · Control document 2 of 3 · Draft 0.10 · 2026-11-20

Defines the public Python API of the P0 release: names, signatures, accepted arguments, return values, errors, and the number of queries each call may run. Implements [mborobak-scope.md](mborobak-scope.md) §5 and §7. How a plan becomes SQL is in the [compiler specification](compiler-spec.md) (control document 1); how results compare to PySpark is in the [compatibility matrix](compatibility-matrix.md) (control document 3).

## 1. Principles

1. **PySpark names and shapes.** Every P0 call has the name, argument order, keyword names and return type of its PySpark 4.3 counterpart (D9). Code written for PySpark's DataFrame API should run after changing the imports and the session builder. Where Mborobak differs, this document says so in a **Differs from PySpark** note, and the compatibility matrix lists it.
2. **Lazy until an action.** Transformations return a new `DataFrame` and never touch data. Actions run exactly one data query (scope §5.2). §8 lists, per call, what it may run.
3. **Immutable.** No call changes an existing `DataFrame` or `Column`. A `DataFrame` can be reused and branched freely.
4. **Fail early and clearly.** Errors that the analyzer can detect are raised by the transformation that causes them, not later at the action. Every error names the user's operation (§9.2).
5. **Nothing runs in Python that should run in IRIS.** No silent client-side fallback (scope §6.9). An operation that can't be translated raises `UnsupportedOperationError`.
6. **Small companions allowed.** A few members not listed in scope §5.1 are included because they lower to existing plan nodes and cost nothing to support: `withColumns`, `withColumnsRenamed`, `crossJoin`, the `GroupedData` shortcuts, `toSQL()` and two catalog helpers.
7. **No surprises for IRIS administrators.** The library never changes IRIS configuration, never installs server-side code and needs only `SELECT` on the tables it reads (scope §7).

## 2. Package layout

| Import | Contents |
|--------|----------|
| `mborobak` | `MborobakSession`, `DataFrame`, `Column`, `Row`, `GroupedData`, `__version__` |
| `mborobak.sql.functions` | Column functions (§6). Import as `F` |
| `mborobak.sql.types` | Data types, `StructType`, `StructField`, `Row` |
| `mborobak.errors` | The error classes (§9) |

`mborobak.sql` re-exports `MborobakSession`, `DataFrame`, `Column`, `Row` and `GroupedData`, so `from mborobak.sql import DataFrame` works like `from pyspark.sql import DataFrame`.

The public API is exactly the names in each module's `__all__`. Anything else, including modules whose names start with `_`, may change in any release.

**Dependencies:** `intersystems-irispython>=5.4` (D3). `pandas` is optional (`pip install mborobak[pandas]`); only `toPandas()` needs it, and it raises `ImportError` with the install command when pandas is missing.

## 3. Session

### 3.1 Creating a session

```python
session = (
    MborobakSession.builder
    .config("host", "localhost")
    .config("port", 1972)
    .config("namespace", "USER")
    .config("user", os.environ["IRIS_USER"])
    .config("password", os.environ["IRIS_PASSWORD"])
    .getOrCreate()
)
```

| Builder method | Behavior |
|----------------|----------|
| `config(key, value)` | Sets one setting (§3.3). Returns the builder |
| `config(map={...})` | Sets several. Returns the builder |
| `connection(conn)` | Use an existing DB-API connection from `iris.connect()` instead of opening one. The session never closes a connection it was given |
| `appName(name)` | Stored and shown in diagnostics only |
| `getOrCreate()` | Returns the active session if one exists and its connection settings are the same; otherwise opens a connection and makes the new session active. Mborobak settings (`mborobak.*`) given here are applied to the returned session |
| `create()` | Always returns a new session, which does not become the active one |

`MborobakSession.getActiveSession()` returns the active session or `None`.

Opening the connection is the only network call `getOrCreate()` and `create()` make. A connection failure raises `MborobakConnectionError` with host, port and namespace, never the password.

**Differs from PySpark:** `master()`, `remote()` and `enableHiveSupport()` don't exist. Passing any `spark.*` key to `config()` raises `UnsupportedOperationError` instead of being ignored, so a copied Spark configuration doesn't silently do nothing.

### 3.2 Session methods

| Member | Returns | Notes |
|--------|---------|-------|
| `table(name)` | `DataFrame` | See §3.4. Runs one schema lookup unless cached |
| `sql(query, args=None)` | `DataFrame` | See §3.5 |
| `createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)` | `DataFrame` | See §3.6 (0.2.x). Runs 2 statements at the call |
| `installFunctions()` | `list[str]` | Creates the SQL functions Mborobak uses for regular expressions (schema `Mborobak`), for an administrator to run once where users may not create functions; returns their names. Not needed otherwise: they are created on first use (0.3) |
| `read.table(name)` | `DataFrame` | The same as `table(name)` (0.2.x). `read.format()`, `option()`, `options()` and `schema()` return the reader and have no effect; `read.load()`, `csv()`, `parquet()`, `json()` and the other file readers raise `UnsupportedOperationError` |
| `catalog.refresh(name=None)` | `None` | Drops cached schemas: one table, or all when `name` is `None` |
| `catalog.tableExists(name)` | `bool` | One schema lookup |
| `catalog.listColumns(name)` | `list[CatalogColumn]` | Name, type, nullability, collation |
| `catalog.listTables(dbName=None, pattern=None)` | `list[Table]` | Tables and views of an IRIS schema (`SQLUser` by default), then the session's temporary views; `Table` has PySpark's fields (`name`, `namespace`, `tableType`, `isTemporary` …) |
| `catalog.dropTempView(name)` | `bool` | Removes a temporary view; `True` if it existed |
| `conf.get(key, default=None)`, `conf.set(key, value)`, `conf.unset(key)` | | Mborobak settings only (§3.3); connection settings are read-only after creation |
| `lastExecution` | `ExecutionMetrics` or `None` | Metrics of the most recent action (§7.2) |
| `stop()` | `None` | Closes the connection it opened; the session can't be used afterwards (`SessionClosedError`) |
| `with MborobakSession.builder...getOrCreate() as session:` | | Calls `stop()` on exit |
| `version` | `str` | Mborobak version |
| `irisVersion` | `str` | `$ZVERSION`; read once at connect time |

### 3.3 Settings

Connection settings (`host`, `port`, `namespace`, `user`, `password`, `timeout` in seconds, default 30) are fixed when the session is created. The password is kept only as long as the driver needs it and never appears in `repr()`, errors or diagnostics.

Mborobak settings can be changed at any time with `session.conf.set()`. They apply to actions run afterwards; existing `DataFrame`s pick them up, since compilation happens at the action.

| Key | Values | Default | Effect |
|-----|--------|---------|--------|
| `mborobak.sql.stringSemantics` | `"pyspark"`, `"iris"` | `"pyspark"` | D10: `%EXACT` handling (compiler spec §7.1) |
| `mborobak.sql.dialect` | `"spark"`, `"iris"` | `"spark"` | The language of `sql()`: Spark SQL built from DataFrame operations, or IRIS SQL passed through (§3.5). The default was `"iris"` before 0.3 |
| `mborobak.optimizer.enabled` | `bool` | `True` | Turns all optional rewrites on or off (compiler spec §8) |
| `mborobak.optimizer.<rewrite>` | `bool` | `True` | Turns one rewrite on or off, for example `mborobak.optimizer.pruneColumns` |
| `mborobak.sql.maxParameters` | `int`, 1–900 | `900` | Parameter count that raises `PlanTooLargeError` (compiler spec §9.5) |
| `mborobak.sql.cacheSize` | `int` ≥ 0 | `256` | Compiled-SQL cache entries; `0` turns the cache off |
| `mborobak.show.truncate` | `int` ≥ 0 | `20` | Default cell width of `show()` |
| `mborobak.write.stringLength` | `int`, 1–3641144 | `3641144` | `VARCHAR` length of string columns in tables that `df.write` creates (§4.7, compiler spec §9.7) |

Unknown `mborobak.*` keys raise `UnsupportedOperationError` listing the valid keys.

### 3.4 `table(name)`

- `"Sales"` refers to `SQLUser.Sales`, the IRIS default schema (study §2.4). `"Finance.Sales"` names the schema.
- Names are case-insensitive, like IRIS identifiers. Names containing a dot or other special characters use backticks, as in PySpark: ``"`My.Schema`.`Odd Table`"``.
- A missing table raises `TableNotFoundError` at the `table()` call.
- Columns of unsupported IRIS types (streams, `%List`, binary) stay in the catalog; selecting one, including through `select("*")` or an action on the unmodified table, raises `UnsupportedTypeError` naming the column and suggesting `drop()` (compiler spec §3).

### 3.5 `sql(query, args=None, **kwargs)`

`sql()` speaks one of two dialects, set by `mborobak.sql.dialect` (§3.3):

- **`"spark"` (default since 0.3):** `query` is a Spark SQL `SELECT`, built from DataFrame operations, so names, types and semantics are Spark's and it compiles to one IRIS statement like any DataFrame (compiler spec §7.17). Supported: `WITH`, `SELECT [DISTINCT]` with `*`, `t.*`, expressions and aliases (everything §6.7 accepts), `FROM` tables (`table` or `schema.table`, with an alias), subqueries and CTEs, joins (`[INNER]`, `LEFT/RIGHT/FULL [OUTER]`, `CROSS`, `LEFT SEMI`, `LEFT ANTI`, a comma) with `ON` or `USING`, `WHERE`, `GROUP BY` expressions, select aliases, ordinals or `ALL`, `HAVING`, `ORDER BY` select aliases, ordinals, expressions and columns the select drops, `LIMIT`, `UNION [ALL | DISTINCT]`. `?` and `:name` markers take `args` as literal values. Not supported, with `UnsupportedOperationError`: other statements (`CREATE`, `INSERT` …; write with `df.write`), `SELECT` without `FROM`, `EXCEPT`/`INTERSECT`, `OFFSET`, `NATURAL JOIN`, three-part names, `ROLLUP`/`CUBE`, `WINDOW`/`QUALIFY`/`LATERAL VIEW`, window functions in an aggregating select, and subqueries in expressions. For IRIS-only SQL (IRIS functions, `%EXACT`, `TOP`), set the dialect to `"iris"`.
- **`"iris"`:** `query` is IRIS SQL, passed through as described below. This was the only dialect before 0.3.

In both dialects, `kwargs` fill `{name}` placeholders as in PySpark: a `DataFrame` becomes a relation (Spark dialect only), other values are formatted into the text.

The IRIS dialect:

- `query` is one IRIS `SELECT` statement (or `WITH … SELECT`). Anything else raises `UnsupportedOperationError` before running.
- `args` is a list of values for `?` markers, in order, or a `dict` for Spark's named markers: `sql("… WHERE id > :low", {"low": 5})` (0.2.x). A name may appear more than once; markers inside strings, quoted identifiers and comments are left alone; `::` is not a marker. A marker without a value raises `ValueError`, and so does a `:name` marker without a dict; a dict with `?` markers raises `UnsupportedOperationError`. Extra dict entries are ignored. The values are sent as parameters, never pasted into the text.
- The result is a `DataFrame` like any other: transformations wrap the query as a subquery (scope §6.7). Its schema comes from preparing the statement, without fetching rows (scope §5.2).
- A `sql()` DataFrame used on its own (for example `session.sql(q).collect()`) runs the query exactly as written, so its `ORDER BY` holds.
- When transformations follow, the query becomes a subquery, and Mborobak adjusts it the way IRIS requires (prototype, verified on IRIS 2026.2):
  - an `ORDER BY` without `TOP` gets `TOP ALL` (compiler spec §7.2);
  - a `UNION … ORDER BY` is wrapped as `SELECT TOP ALL * FROM (union) ORDER BY …`, because IRIS rejects it as a subquery even with `TOP ALL`;
  - a `WITH` clause is moved to the front of the final statement, because IRIS rejects `WITH` inside a subquery. Combining `sql()` queries that use `WITH` works only when they are the same query with the same args (for example a self-join); otherwise it raises `UnsupportedOperationError`, because two `WITH` clauses would clash at the front of the statement.
- A later transformation may still drop the order, as in Spark.
- Result columns must have distinct names before transformations can follow; IRIS names unaliased expressions `Expression_1`, `Expression_2`, …, so alias them in the SQL.
- Column types come from the driver's result metadata. A column of a type Mborobak doesn't support (for example `TIME`) raises `UnsupportedTypeError` at the `sql()` call; cast it in the SQL.

**Differs from PySpark:** with `mborobak.sql.dialect = "iris"`, the SQL is IRIS SQL, not Spark SQL.

### 3.6 `createDataFrame(data, schema=None)`

- `data`: a list of tuples, lists, `Row`s or dicts, a list of scalars (with an atomic `schema` such as `"int"`), or a pandas DataFrame.
- `schema`: a `StructType`, a DDL string (`"id int, name string"` or `"id: int, name: string"`, `NOT NULL` allowed), an atomic type name, a list of column names (types inferred), or `None` (names and types inferred; names `_1`, `_2`, … for tuples, sorted keys for dicts).
- Types are inferred and values checked as in PySpark 4.3 (compiler spec §9.8). Invalid input raises `TypeError` or `ValueError` at the call, before anything reaches IRIS. `samplingRatio` is ignored: every row is checked. With `verifySchema=False` and no schema, values are not checked.
- The rows are loaded at the call into a temporary table of this connection: other connections can't see them, and they go when the connection closes or the session stops. Loading needs `CREATE TABLE` permission in the namespace (schema `MborobakTemp`).
- **Differs from PySpark:** `datetime` values infer `timestamp_ntz`; missing values in pandas string columns are NULL; `Decimal` values with more than 19 digits before the point raise `MborobakExecutionError` (matrix §2.17).

## 4. DataFrame

### 4.1 Transformations

All return a new `DataFrame`. Column arguments accept a column name (`str`) or a `Column`, unless the table says otherwise. "Lookup" means a cached schema lookup may run (§8).

| Method | Arguments | Notes |
|--------|-----------|-------|
| `select(*cols)` | names, `Column`s, `"*"`, or one list of these | `"*"` expands to all columns |
| `filter(condition)`, `where(condition)` | `Column` of boolean type, or a Spark SQL condition string (0.2) | A non-boolean column raises `AnalysisError`. A string is parsed as a Spark SQL expression (§6.7) |
| `selectExpr(*exprs)` | Spark SQL expression strings, each with an optional alias (`"a + 1 AS b"`), or `"*"` | Same as `select` of `F.expr(...)` (0.2) |
| `withColumn(colName, col)` | `str`, `Column` | Replaces a column with the same name (case-insensitive), otherwise appends |
| `withColumns(colsMap)` | `dict[str, Column]` | Several `withColumn`s at once |
| `withColumnRenamed(existing, new)` | `str`, `str` | No-op when `existing` is not a column, as in PySpark |
| `withColumnsRenamed(colsMap)` | `dict[str, str]` | |
| `drop(*cols)` | names or `Column`s | Unknown names are ignored, as in PySpark |
| `alias(alias)` | `str` | Qualifies columns for `col("a.x")` and self-joins |
| `orderBy(*cols, ascending=True)`, `sort(...)` | columns; `ascending` is a `bool` or a list of `bool` | Default null order: first when ascending, last when descending |
| `limit(num)` | `int` ≥ 0 | |
| `distinct()` | | |
| `union(other)`, `unionAll(other)` | `DataFrame` | By position, duplicates kept (D4). Same column count required; numeric types widened |
| `unionByName(other, allowMissingColumns=False)` | `DataFrame`, `bool` | By name, case-insensitively; output columns in the left side's order and names. With `allowMissingColumns`, a column one side lacks is NULL there, and the right side's extra columns come last; without it, a column on one side only raises `AnalysisError`, and so does a name twice on one side (0.2.x) |
| `join(other, on=None, how=None)` | `on`: name, list of names, or a boolean `Column`; `how`: §4.2 | `on=None` gives a cross join, as in PySpark 3+ |
| `crossJoin(other)` | `DataFrame` | |
| `groupBy(*cols)`, `groupby(...)` | columns | Returns `GroupedData` (§4.3) |
| `agg(*exprs)` | aggregate `Column`s or a `dict` | Same as `groupBy().agg(...)` |
| `fillna(value, subset=None)`, `na.fill(...)` | `value`: `int`, `float`, `str`, `bool`, or a `dict` of column name to value; `subset`: a name or a list | Replaces NULL, and NaN in double columns. Without a dict, a number fills numeric columns, a string string columns, a bool boolean columns; other columns are left as they are. The value is cast to each column's type, as in Spark (`1.7` fills an `int` column with 1). A dict value of the wrong kind raises `AnalysisError`. (0.2) |
| `dropDuplicates(subset=None)`, `drop_duplicates(...)` | `subset`: a name or a list | One row per distinct `subset` value; which row is kept is not defined, as in Spark. Without `subset`, `distinct()` (0.2) |
| `replace(to_replace, value, subset=None)`, `na.replace(...)` | `to_replace`: a value, a list, or a dict of old to new; `value`: a value or a list | Spark's rules: numbers replace in numeric columns, strings in string columns, booleans in boolean columns; the new value is cast to the column's type; `None` makes NULL; mixed kinds raise `ValueError` |
| `transform(func, *args, **kwargs)` | a function returning a `DataFrame` | `func(df, *args, **kwargs)`, for chaining custom steps |
| `toDF(*cols)` | new names, one per column | Renames every column |
| `createOrReplaceTempView(name)`, `createTempView(name)` | `str` | A session name for the DataFrame, found by `table(name)`, `read.table(name)` and Spark SQL in `sql()`. Nothing is created in IRIS. `createTempView` raises `AnalysisError` if the name exists |
| `sample(withReplacement=None, fraction=None, seed=None)` | `sample(0.1)`, `sample(0.1, 42)`, `sample(False, 0.1, 42)`, `sample(fraction=0.1, seed=42)`; `fraction` in [0, 1] | Each row with probability `fraction`; the same rows for the same seed on every run, other rows than Spark's (compiler spec §7.20). `withReplacement=True` raises `UnsupportedOperationError` (0.3.x) |
| `randomSplit(weights, seed=None)` | a list of non-negative numbers, normalized | A list of DataFrames; every row is in exactly one of them, the same one on every run for a given seed (0.3.x) |
| `sampleBy(col, fractions, seed=None)`, `stat.sampleBy(...)` | `col`: name or `Column`; `fractions`: dict of value to fraction in [0, 1] | Rows whose `col` is not a key are never kept (0.3.x) |
| `dropna(how="any", thresh=None, subset=None)`, `na.drop(...)` | `how`: `"any"` or `"all"`; `thresh`: `int` | Drops rows with NULL, or NaN in double columns, in the `subset` columns. `thresh` keeps rows with at least that many valid values and overrides `how`. (0.2) |

### 4.2 Join types

| `how` | Meaning |
|-------|---------|
| `"inner"` (default) | Inner join |
| `"left"`, `"leftouter"`, `"left_outer"` | Left outer |
| `"right"`, `"rightouter"`, `"right_outer"` | Right outer |
| `"full"`, `"fullouter"`, `"full_outer"`, `"outer"` | Full outer |
| `"cross"` | Cross join |
| `"semi"`, `"leftsemi"`, `"left_semi"` | Left rows that have a match, each once; left columns only (0.2.x) |
| `"anti"`, `"leftanti"`, `"left_anti"` | Left rows without a match, a NULL key included; left columns only (0.2.x) |

Output columns follow compiler spec §6.3. A semi or anti join on names (`join(other, "k", "left_semi")`) puts the key columns first, as Spark does. Join types are case-insensitive.

### 4.3 `GroupedData`

| Method | Notes |
|--------|-------|
| `agg(*exprs)` | Aggregate `Column`s, optionally aliased. Also `agg({"amount": "sum", "id": "count"})`, as in PySpark |
| `count()` | Adds a `count` column |
| `sum(*cols)`, `avg(*cols)`, `mean(*cols)`, `min(*cols)`, `max(*cols)` | Column names; with no names, all numeric columns (`min`/`max`: all columns), as in PySpark |
| `pivot(pivot_col, values=None)` | After `groupBy()`: one column per value, named by it, or `value_aggregate` with several aggregates; NULL where a group has no row with the value. Without `values`, a query reads the distinct values first (at most 10,000) (0.3.x) |

`df.rollup(*cols)` and `df.cube(*cols)` return the same `GroupedData` methods (no `pivot`); rows for the subtotals have NULL keys. `df.unpivot(ids, values, variableColumnName, valueColumnName)` and `df.melt(...)` turn value columns into rows; `values=None` takes every column that is not an id (0.3.x, compiler spec §7.21).

A non-aggregated column in `agg()` that isn't a grouping column raises `AnalysisError` at the `agg()` call.

### 4.4 Actions

Each runs exactly one data query.

| Method | Returns | Notes |
|--------|---------|-------|
| `collect()` | `list[Row]` | All rows |
| `take(num)` | `list[Row]` | At most `num` rows (`TOP num`) |
| `head(n=None)` | `Row` or `None` when `n` is `None`; `list[Row]` otherwise | |
| `first()` | `Row` or `None` | `None` for an empty result |
| `count()` | `int` | Runs `SELECT COUNT(*)` over the plan; ordering is dropped |
| `show(n=20, truncate=True, vertical=False)` | `None` | Prints. `truncate=True` cuts cells to 20 characters (or `mborobak.show.truncate`); an `int` sets the width; `False` disables it. NULL prints as `NULL`. Fetches `n + 1` rows so it can print "only showing top n rows" |
| `toPandas()` | `pandas.DataFrame` | Types per §5.3 |
| `isEmpty()` | `bool` | Runs `take(1)` |
| `tail(num)` | `list[Row]` | The last `num` rows; fetches every row, as IRIS has no offset from the end |
| `describe(*cols)` | `DataFrame` of strings | `count`, `mean`, `stddev`, `min`, `max` of the numeric and string columns (or of `cols`), formatted as Spark prints them; string columns' mean and stddev use `try_cast` to double. Runs one aggregate query per 20 columns, then loads the small result as `createDataFrame` does (§3.6) |
| `summary(*statistics)` | `DataFrame` of strings | As `describe()`, with Spark's default `count`, `mean`, `stddev`, `min`, `25%`, `50%`, `75%`, `max`, or the given ones (also `count_distinct`, `approx_count_distinct`, any `p%`). Percentages follow Spark's `percentile_approx`: the smallest value with at least p of the rows, here exact |

### 4.5 Schema and diagnostics

These may run a schema lookup but never a data query.

| Member | Returns | Notes |
|--------|---------|-------|
| `schema` | `StructType` | |
| `columns` | `list[str]` | Output names in order |
| `dtypes` | `list[tuple[str, str]]` | Type names as in PySpark: `int`, `bigint`, `double`, `decimal(10,2)`, `string`, `boolean`, `date`, `timestamp_ntz` |
| `printSchema()` | `None` | PySpark's tree format: `root`, then ` |-- name: type (nullable = true)` |
| `explain(mode=None)` | `None` | Prints the plan, translations, rewrites, SQL and parameters (compiler spec §10). `mode="sql"` prints only the SQL and parameters. `explain(True)` equals the default |
| `toSQL()` | `tuple[str, list]` | The SQL text and parameter list that an action would run. Not in PySpark; for debugging and tests |
| `df["name"]`, `df.name` | `Column` | Bound to this `DataFrame` (compiler spec §6.1). Attribute access works only for names that are valid Python identifiers and not `DataFrame` attributes |

### 4.6 Known PySpark members that are not in P0

Calling these raises `UnsupportedOperationError`, which names the release that adds them, instead of `AttributeError`:

- P1: `toLocalIterator`. (Null, string, math, date and window functions, `fillna`, `dropna` and `dropDuplicates` arrived in 0.2, §6.4, §6.6; `write`, `unionByName`, semi and anti joins and `createDataFrame` in 0.2.x, §4.7, §4.1, §4.2, §3.6.)
- Not planned: `cache`, `persist`, `unpersist`, `checkpoint`, `repartition`, `coalesce`, `rdd`, `foreach`, `mapInPandas`, `toArrow`, streaming, UDFs (P2).

`cache()` and `persist()` raise rather than doing nothing, because a silent no-op would hide the fact that every action re-runs the query.

### 4.7 Writing tables

`df.write` returns a `DataFrameWriter` (also importable from `mborobak.sql`). Writing runs inside IRIS: the DataFrame's query feeds `INSERT … SELECT`, and the rows never travel to Python. Each write is one transaction, so a failed write changes nothing (compiler spec §9.7).

| Member | Returns | Notes |
|--------|---------|-------|
| `mode(saveMode)` | the writer | `"errorifexists"` (default), `"error"`, `"append"`, `"overwrite"`, `"ignore"`, case-insensitive; anything else raises `ValueError` |
| `saveAsTable(name, format=None, mode=None, partitionBy=None, **options)` | `None` | `name` is `"schema.table"` or `"table"` (schema `SQLUser`), as for `table()`. A missing table is created with the DataFrame's columns and types. When it exists: `errorifexists` raises `AnalysisError`, `ignore` does nothing, `append` inserts with columns matched by name (case-insensitive) and cast by Spark's ANSI store-assignment rules, `overwrite` replaces the table, its columns included, and may read the table it replaces |
| `insertInto(tableName, overwrite=None)` | `None` | The table must exist (`TableNotFoundError`). Columns are matched by position. `overwrite=True`, or `mode("overwrite")`, deletes the rows first |
| `format(source)`, `option(key, value)`, `options(**kw)`, `partitionBy(*cols)`, `bucketBy(…)`, `sortBy(…)` | the writer | Describe files in Spark and have no effect, so `format("delta")` notebooks run unchanged. Exceptions: `option("mergeSchema", "true")` and a `path` option raise `UnsupportedOperationError` |
| `save()`, `parquet()`, `csv()`, `json()`, `orc()`, `text()`, `jdbc()` | — | `UnsupportedOperationError`: they write files or other databases |

Errors before anything runs: `AnalysisError` for an existing table under `errorifexists`, a view as target, a column name with `.`, `,` or `^` or names differing only in case (IRIS can't hold either), a column count or name that doesn't match the table, or a type the table column can't take (`string` into `int`). Errors while writing are the action errors of §9 (`MborobakArithmeticError`, `MborobakCastError`), plus `MborobakExecutionError` when a value doesn't fit the column (a string longer than `mborobak.write.stringLength`, a NULL into a NOT NULL column of an existing table).

`session.lastExecution` records `action="saveAsTable"` or `"insertInto"`, the statements, the `INSERT`'s parameters and `rows`, the number of rows written.

## 5. Results

### 5.1 `Row`

`Row` behaves like PySpark's: a tuple with named fields.

- `row[0]`, `row["name"]`, `row.name`; `len(row)`; iteration yields values; `row.asDict()` returns a `dict` (later duplicates overwrite earlier ones, as in PySpark).
- Field names keep the output spelling. Duplicate names (after a join on an expression) are allowed; `row["id"]` then returns the first.
- `Row(a=1, b=2)` and `Row("a", "b")(1, 2)` construct rows, for tests and comparisons.
- Equality is tuple equality, as in PySpark.

### 5.2 Python values

Values are converted to the column's schema type, never left as the driver returned them (compiler spec §3):

| Type | Python value |
|------|--------------|
| `IntegerType`, `LongType` | `int` |
| `DoubleType` | `float` (including `nan`, `inf`) |
| `DecimalType(p, s)` | `decimal.Decimal`, quantized to scale `s` |
| `StringType` | `str` |
| `BooleanType` | `bool` |
| `DateType` | `datetime.date` |
| `TimestampNTZType` | `datetime.datetime`, naive |
| NULL | `None` |

### 5.3 `toPandas()` types

Following PySpark 4.3's default conversion, which uses Arrow (`spark.sql.execution.arrow.pyspark.enabled` is `true` by default in PySpark 4.3, checked 2026-11-19). The integer, double, decimal and date rows below are verified against it by the differential suite (matrix RES-02); the boolean and timestamp rows are not yet:

| Type | pandas dtype without NULLs | with NULLs |
|------|----------------------------|------------|
| `IntegerType` | `int32` | `float64` |
| `LongType` | `int64` | `float64` |
| `DoubleType` | `float64` | `float64` |
| `BooleanType` | `bool` | `object` |
| `DecimalType`, `StringType`, `DateType` | `object` | `object` |
| `TimestampNTZType` | `datetime64[ns]` | `datetime64[ns]` (`NaT`) |

The pandas `DataFrame` is built from columns directly, not from `Row` objects, which avoids most of the 55% overhead measured in the study (§2.8).

## 6. `Column` and functions

### 6.1 Building columns

| Form | Meaning |
|------|---------|
| `F.col("x")`, `F.column("x")` | Column by name, resolved against the `DataFrame` it is used on |
| `F.col("a.x")` | Column `x` of the `DataFrame` aliased `a` |
| `df["x"]`, `df.x` | Column bound to `df` |
| `F.lit(value)` | Literal. Accepts `None`, `bool`, `int`, `float` (including NaN, ±inf), `Decimal`, `str`, `date`, `datetime`, or a `Column` (returned as is) |
| `F.when(cond, value).when(...).otherwise(value)` | `CASE`. Without `otherwise`, `ELSE NULL` |

Plain Python values in an expression (`F.col("x") + 1`, `F.col("s") == "PAID"`) become literals.

### 6.2 Operators

| Operator | Meaning |
|----------|---------|
| `+ - * / %`, unary `-` | Arithmetic. `/` follows Spark's types (compiler spec §3). Division by zero raises `MborobakArithmeticError` at the action |
| `== != < <= > >=` | Comparison; NULL in, NULL out |
| `&`, `\|`, `~` | Logical and, or, not |
| `and`, `or`, `not`, `if col:`, `bool(col)` | Raise `ValueError` ("Cannot convert column into bool"), as in PySpark |
| `col[...]`, `col.getItem()`, `col.getField()` | Not supported (no complex types in P0) |

Python operator precedence applies: `F.col("a") > 1 & F.col("b") < 2` is a mistake in PySpark too; parenthesize each comparison.

### 6.3 `Column` methods

| Method | Notes |
|--------|-------|
| `alias(name)`, `name(name)` | Output name |
| `try_cast(dataType)` | As `cast`, but text that isn't a valid number becomes NULL instead of an error (Spark 4) |
| `cast(dataType)`, `astype(dataType)` | A `DataType` or a type name (`"int"`, `"bigint"`, `"double"`, `"decimal(10,2)"`, `"string"`, `"boolean"`, `"date"`, `"timestamp_ntz"`). Invalid text raises `MborobakCastError` at the action (compiler spec §7.7) |
| `isNull()`, `isNotNull()` | |
| `eqNullSafe(other)` | Null-safe equality (`<=>` in Spark) |
| `isin(*values)` | Values or one list. Counts toward the parameter limit |
| `like(pattern)` | SQL `LIKE` with `%` and `_`. Escape characters are P1 |
| `between(lower, upper)` | Inclusive |
| `asc()`, `desc()`, `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, `desc_nulls_last()` | Sort orders |
| `when(cond, value)`, `otherwise(value)` | On a `when()` result only |
| `over(window)` | On an aggregate or a window function (§6.6). A window function in `filter`, `orderBy`, `groupBy`, `agg` or a join raises `AnalysisError`: add it with `withColumn` first (0.2) |
| `substr(startPos, length)` | Integers (or `F.lit` integers); same as `F.substr` (0.2) |

### 6.4 Functions

`mborobak.sql.functions` in P0:

| Function | Notes |
|----------|-------|
| `col`, `column`, `lit`, `when` | §6.1 |
| `count(col)` | `count("*")` and `count(F.lit(1))` count rows; `count(col)` counts non-NULL values |
| `countDistinct(col, *cols)`, `count_distinct(...)` | |
| `sum(col)`, `avg(col)`, `mean(col)`, `min(col)`, `max(col)` | Types per compiler spec §3 |
| `asc(name)`, `desc(name)`, and the `_nulls_` variants | Same as the `Column` methods |
| `expr(str)` | A Spark SQL expression, with an optional alias (§6.7) (0.2) |

Release 0.2 adds null, string and math functions. Their IRIS SQL is in compiler spec §7.10 and their behavior next to PySpark's in the compatibility matrix §2.12.

| Function | Notes |
|----------|-------|
| `coalesce(*cols)`, `ifnull(a, b)`, `nvl(a, b)`, `nvl2(a, b, c)`, `nullif(a, b)` | Result type: the wider argument type |
| `isnan(col)`, `isnull(col)`, `isnotnull(col)` | `isnan` takes a number; NULL is not NaN |
| `greatest(*cols)`, `least(*cols)` | At least two columns; NULLs skipped |
| `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `initcap`, `reverse` | A string column. `trim(col, trim)` with a trim string is not supported yet |
| `length(col)`, `char_length(col)`, `character_length(col)` | Counts trailing spaces |
| `substring(str, pos, len)`, `substr(str, pos, len=None)` | `pos` and `len` are integers (or `F.lit` integers); Spark's rules for 0 and negative positions |
| `concat(*cols)`, `concat_ws(sep, *cols)` | Strings, integers, booleans, dates, timestamps. Double and decimal columns raise `UnsupportedOperationError` (their text differs, matrix CAST-12, CAST-13): format them first |
| `lpad(col, len, pad)`, `rpad(...)` | `len` an integer, `pad` a string |
| `instr(str, substr)`, `locate(substr, str, pos=1)` | `substr` a string; 1-based results, 0 when not found |
| `replace(src, search, replace=None)` | Columns or names, as in PySpark: use `F.lit("x")` for a constant |
| `repeat(col, n)` | `n` an integer |
| `abs(col)` | Same type |
| `round(col, scale=0)`, `bround(col, scale=0)` | Half up, half even. Integers stay integers, decimals get Spark's precision, doubles stay doubles |
| `ceil(col)`, `ceiling(col)`, `floor(col)` | `bigint` for integers and doubles, `decimal(p-s+1,0)` for decimals. The `scale` argument is not supported yet |
| `pow(a, b)`, `power(a, b)`, `sqrt`, `exp`, `ln`, `log(x)`, `log(base, x)`, `log10`, `log2`, `log1p`, `signum`, `sign` | `double`. Out-of-domain inputs give NaN, NULL or infinity as in Spark |
| `pmod(dividend, divisor)` | Non-negative remainder; a zero divisor raises `MborobakArithmeticError` |

Statistics (compiler spec §7.18, compatibility matrix §2.20). All return `double`; NaN in a group gives NaN, as in Spark:

| Function | Notes |
|----------|-------|
| `stddev(col)`, `std(col)`, `stddev_samp(col)`, `stddev_pop(col)`, `variance(col)`, `var_samp(col)`, `var_pop(col)` | IRIS's `STDDEV_SAMP`, `VAR_POP` …; sample statistics of one row are NULL |
| `corr(col1, col2)`, `covar_pop(col1, col2)`, `covar_samp(col1, col2)` | Over rows where both are not NULL; NULL where Spark divides by zero (a constant column) |
| `skewness(col)`, `kurtosis(col)` | Spark's population moments (excess kurtosis); NULL when the spread is zero |
| `median(col)`, `percentile(col, percentage)` | Exact, interpolated between neighbors as Spark's `percentile`; in `agg()` and `select()`, not over a window. A list of percentages or a frequency raises `UnsupportedOperationError` |

Random values (compiler spec §7.20, compatibility matrix §2.21): `rand(seed=None)` in [0, 1) and `randn(seed=None)`, standard normal, `double` and not nullable, named `rand(42)` as in Spark. A row's value comes from the seed and the row's columns, so it is the same on every run and differs from Spark's. Without a seed, a random one is drawn when the column is made. Not allowed in a join condition.

The statistical aggregates over a window (`stddev(x).over(w)`) raise `UnsupportedOperationError`.

More functions (compiler spec §7.19, compatibility matrix FUN-27 to FUN-34): `md5`, `sha1`, `sha2(col, numBits)`, `crc32` (strings), `levenshtein(left, right, threshold=None)`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `cot`, `degrees`, `radians`, `pi`, `dayname`, `monthname`, `conv(col, fromBase, toBase)`, `parse_url(url, part, key=None)`, `to_utc_timestamp(ts, tz)`, `from_utc_timestamp(ts, tz)` (as `timestamp_ntz`), and the window functions `first_value`, `last_value`. The hashes, `levenshtein`, `conv`, `parse_url` and the time-zone conversions run in SQL functions Mborobak creates in the `Mborobak` schema on first use, as regular expressions do.

Regular expressions (0.3, compiler spec §7.16, compatibility matrix §2.18). The pattern syntax is ICU's, which is Java's for nearly every pattern; IRIS runs them in SQL functions Mborobak creates in the schema `Mborobak` the first time a session needs one (`CREATE FUNCTION` permission, or an administrator's `session.installFunctions()`):

| Function | Notes |
|----------|-------|
| `Column.rlike(pattern)`, `rlike(str, regexp)`, `regexp(str, regexp)`, `regexp_like(str, regexp)` | `boolean`: a match anywhere. `pattern` is a string; `regexp` a column or name (`F.lit("…")` for a constant), as in PySpark |
| `regexp_replace(string, pattern, replacement)` | Every match replaced; `$1` refers to a group, `\$` is a dollar sign. A `str` pattern or replacement is the text itself |
| `regexp_extract(str, pattern, idx)` | Group `idx` (0: the whole match) of the first match; `''` when there is no match or the group took no part. A group beyond the pattern's raises `MborobakExecutionError` |
| `regexp_count(str, regexp)`, `regexp_substr(str, regexp)`, `regexp_instr(str, regexp, idx=0)` | `int`; the first match or NULL; the 1-based position of the first match or 0. `regexp_instr` supports `idx=0` only |

An invalid pattern raises `MborobakExecutionError` ("invalid regular expression") when the query runs. `split` and `regexp_extract_all` return arrays and raise `UnsupportedOperationError`.

Date and time functions (compiler spec §7.12, compatibility matrix §2.14). Date arguments take dates, timestamps or date text (cast as Spark casts it):

| Function | Notes |
|----------|-------|
| `current_date()`, `current_timestamp()`, `now()` | `date`; `timestamp_ntz` (Spark: `timestamp`, matrix TIME-05) |
| `year`, `month`, `dayofmonth`, `day`, `dayofweek`, `dayofyear`, `weekofyear`, `quarter`, `hour`, `minute`, `second` | `int`; `weekofyear` is ISO 8601 |
| `date_add(start, days)`, `date_sub(...)`, `add_months(start, months)` | `days`/`months` an integer or an integer column; `date` |
| `datediff(end, start)` | `int` |
| `months_between(date1, date2, roundOff=True)` | `double` |
| `last_day(date)`, `trunc(date, format)` | `date`; `trunc` takes year, quarter, month, week (Spark's spellings); other units give NULL |
| `date_trunc(format, timestamp)` | `timestamp_ntz`; year … millisecond; other units give NULL |
| `date_format(date, format)` | `string`; Java pattern letters `y M d D H h m s S a E Q` and quoted text; others raise `UnsupportedOperationError` |
| `to_date(col, format=None)`, `to_timestamp(col, format=None)` | Without a format, Spark's cast; with one, strict parsing of the pattern letters `yyyy M MM MMM MMMM d dd H HH h hh m mm s ss S a`. Invalid text raises `MborobakCastError` at the action; `yy` raises `UnsupportedOperationError` |

Window functions, each used with `.over(window)` (§6.6):

| Function | Notes |
|----------|-------|
| `row_number()`, `rank()`, `dense_rank()`, `ntile(n)` | `int`; need an ordered window |
| `percent_rank()`, `cume_dist()` | `double`; need an ordered window |
| `lag(col, offset=1, default=None)`, `lead(...)` | The column's type; need an ordered window and no frame. `default` is a constant |
| `first(col)`, `last(col)` | Over the window's frame. `ignorenulls=True` raises `NotImplementedError` |
| `count`, `sum`, `avg`, `min`, `max` | Over the window's frame, with their usual types |

Accepting a column name where PySpark does (`F.sum("amount")`) works the same way. Where PySpark takes a constant (`substring`'s position, `lpad`'s pad, `concat_ws`'s separator), a column there raises `TypeError`.

### 6.6 Windows

`from mborobak.sql import Window` (also `mborobak.sql.window`). `Window.partitionBy(*cols)`, `Window.orderBy(*cols)`, `Window.rowsBetween(start, end)`, `Window.rangeBetween(start, end)` return a `WindowSpec`, which has the same four methods and is immutable. `Window.unboundedPreceding`, `Window.unboundedFollowing` and `Window.currentRow` have PySpark's values.

| Frame | Supported |
|-------|-----------|
| None | Yes: Spark's default (running values with ties shared when ordered, the whole partition when not) |
| `rowsBetween(a, b)` | Yes, with an ordered window (or the whole partition without one) |
| `rangeBetween(unboundedPreceding, currentRow)`, `(unboundedPreceding, unboundedFollowing)`, `(currentRow, unboundedFollowing)` | Yes |
| `rangeBetween` with value offsets, or `(currentRow, currentRow)` | No: `UnsupportedOperationError` (IRIS has no `RANGE` frames, compatibility matrix WIN-13) |

Output names are Spark's, for example `RANK() OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`; give windowed columns an alias. The compiled SQL is in compiler spec §7.11.

### 6.7 SQL expression strings

`F.expr(str)`, `df.selectExpr(*str)` and `df.filter(str)`/`where(str)` take Spark SQL expressions: literals with Spark SQL's types (`1.5` is a decimal, `1e3` and `2.0D` doubles, `1L` a bigint), column names (backticks for odd names, `a.x` for qualified ones), arithmetic, comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `[NOT] IN`, `[NOT] LIKE`, `[NOT] BETWEEN`, `CASE`, `CAST(x AS type)`, `if()`, `||`, every function of §6.4 by name (case-insensitive), `count(*)`, `count(DISTINCT x)`, and windows with `OVER (PARTITION BY … ORDER BY … ROWS/RANGE BETWEEN …)`. An alias may follow (`AS name` or just `name`) in `F.expr` and `selectExpr`. Default column names are Spark's for the SQL text.

`RLIKE` and `REGEXP` (and `NOT RLIKE`) are regular-expression matches, as `rlike` (0.3). Not supported: `ILIKE`, `DIV`, bitwise operators, subqueries, `INTERVAL`, lambda functions, array and map access: each raises `UnsupportedOperationError`. Malformed text raises `mborobak.sqlexpr.ParseError` (an `AnalysisError`) when the expression is built, naming the position. The details are in compiler spec §7.13.

### 6.5 Output names

An expression without `alias()` gets the name PySpark would give it, so `df.columns` and `Row` fields match:

| Expression | Name |
|------------|------|
| `F.col("amount")` | `amount` |
| `F.sum("amount")` | `sum(amount)` |
| `F.count("*")` | `count(1)` |
| `F.countDistinct("name")` | `count(DISTINCT name)` |
| `F.col("a") + F.col("b")` | `(a + b)` |
| `F.col("x").cast("int")` | `x` for a plain cast of a column |
| `F.lit(1)` | `1` |
| `F.when(F.col("x") > 1, "hi")` | `CASE WHEN (x > 1) THEN hi END` |

The compatibility matrix tests each P0 form against PySpark 4.3. Names are quoted in the generated SQL, so any of these is a valid IRIS column name.

## 7. Diagnostics

### 7.1 `explain()`

Format defined in compiler spec §10. Output is deterministic for a given plan and settings, so it can be used in tests.

### 7.2 `ExecutionMetrics`

`session.lastExecution` after each action:

| Field | Type | Meaning |
|-------|------|---------|
| `action` | `str` | `collect`, `count`, `show`, … |
| `sql`, `params` | `str`, `list` | What ran |
| `data_queries` | `int` | 1 for every P0 action |
| `schema_lookups` | `int` | Lookups during this action (0 when cached) |
| `rows` | `int` | Rows fetched |
| `python_ms` | `float` | Plan analysis and compilation, including schema lookups |
| `server_ms` | `float` | From execute until the first row is available |
| `fetch_ms` | `float` | Fetching and converting rows (and building pandas for `toPandas`) |
| `cache_hit` | `bool` | Compiled SQL came from the cache |

`server_ms` is measured from the client, so it includes network time; the benchmark reports this (scope §8.3).

## 8. Queries per call

| Call | Schema lookups | Data queries |
|------|----------------|--------------|
| `table(name)` | 1 if not cached | 0 |
| `sql(query)` | 1 prepare (not cached) | 0 |
| Any transformation | 0, or 1 per uncached table it needs (for example joining a new `table()`) | 0 |
| `schema`, `columns`, `dtypes`, `printSchema()`, `explain()`, `toSQL()` | 0 if all tables are cached | 0 |
| Any action (§4.4) | 0 if cached | **exactly 1** |
| `catalog.refresh()` | 0 | 0 |
| The first action of a session that uses a regular expression | 1 lookup of the `Mborobak` functions | 1 `CREATE FUNCTION` per function missing in the namespace, before the query |
| `createDataFrame()` | 0 | 2 statements at the call: `CREATE GLOBAL TEMPORARY TABLE`, and an `INSERT` of all rows (none for empty data) |
| `write.saveAsTable()`, `write.insertInto()` | 1 existence check, plus 1 column lookup for `append` and `insertInto` | 0 to 4 statements, in one transaction: `CREATE TABLE`, `INSERT`, `DROP TABLE`, `ALTER TABLE` or `DELETE` (compiler spec §9.7) |

Integration tests assert these counts using `ExecutionMetrics` and a counting connection wrapper.

## 9. Errors

### 9.1 Hierarchy

```
MborobakError
├── MborobakConnectionError          connecting or a lost connection
├── SessionClosedError               session used after stop()
├── AnalysisError                    raised by transformations
│   ├── TableNotFoundError
│   ├── ColumnNotFoundError          with close-match suggestions
│   ├── AmbiguousColumnError         lists the candidates and how to qualify them
│   └── UnsupportedTypeError         column type not supported in P0
├── UnsupportedOperationError        also a NotImplementedError; names the planned release when there is one
├── PlanTooLargeError                parameter count over the limit (compiler spec §9.5)
└── MborobakExecutionError           raised by actions; keeps the IRIS SQLCODE and message
    ├── MborobakArithmeticError      division by zero, numeric overflow
    └── MborobakCastError            invalid text for a cast
```

`AnalysisError` matches PySpark's class name, so `except AnalysisError` reads the same; it is Mborobak's own class, not PySpark's.

`ValueError` and `TypeError` are used, as in PySpark, for wrong Python argument types or values (for example `limit(-1)` or `limit("5")`).

### 9.2 Messages

Every Mborobak error carries:

- `operation`: the user call that created the problem (`filter`, `join`, …) and its source location (`file.py:42`), captured when the transformation was called. Execution errors name the transformation(s) whose expressions could have failed (compiler spec §7.5).
- For execution errors: `sqlcode` and `iris_message` (the driver's text, including the `%msg` part), and `sql`.
- Never the password or other connection secrets.

Example:

```
MborobakArithmeticError: division by zero
  in: select at report.py:42  (expression: (id / customer_id))
  IRIS: SQLCODE -400 <DIVIDE>
```

## 10. Types module

`mborobak.sql.types` provides `IntegerType`, `LongType`, `DoubleType`, `DecimalType(precision=10, scale=0)`, `StringType`, `BooleanType`, `DateType`, `TimestampNTZType`, `StructField(name, dataType, nullable=True)` and `StructType(fields)`.

They behave like PySpark's: equality by value, `simpleString()` (the names in §4.5), `StructType.fieldNames()`, `StructType["name"]`, iteration over fields. `FloatType`, `ShortType` and `ByteType` exist so that `cast()` accepts them; they are read from IRIS as `DoubleType` and `IntegerType`.

## 11. Stability and versioning

- SemVer (scope §7). Before 1.0, a minor release may change the public API, and each change is listed in `CHANGELOG.md` with the old and new forms.
- The `explain()` text and generated SQL are not part of the contract: they may change in any release. `toSQL()` output is for debugging and tests, not for persisting.
- Error classes and their attributes in §9 are part of the contract; message wording is not.

## 12. Open questions

| # | Question | Needed for |
|---|----------|------------|
| A1 | ~~Default output name of a cast and of `when()`~~ **Resolved** by the differential run: as in §6.5 (matrix NAME-04) | — |
| A2 | ~~Whether to offer an option matching `toPandas()` with Arrow on~~ **Resolved:** Arrow is on by default in PySpark 4.3 and §5.3 already matches its types for the tested columns. The speed of Arrow's columnar transfer is a roadmap item (scope §13, release 0.3) | — |
| A3 | Whether `getOrCreate()` should warn when settings differ from the active session's instead of opening a new one silently | §3.1 |
