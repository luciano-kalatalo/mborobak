# API Contract

Scientificloud · Mborobak · Control document 2 of 3 · Draft 0.1 · 2026-11-18

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
| `catalog.refresh(name=None)` | `None` | Drops cached schemas: one table, or all when `name` is `None` |
| `catalog.tableExists(name)` | `bool` | One schema lookup |
| `catalog.listColumns(name)` | `list[CatalogColumn]` | Name, type, nullability, collation |
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
| `mborobak.optimizer.enabled` | `bool` | `True` | Turns all optional rewrites on or off (compiler spec §8) |
| `mborobak.optimizer.<rewrite>` | `bool` | `True` | Turns one rewrite on or off, for example `mborobak.optimizer.pruneColumns` |
| `mborobak.sql.maxParameters` | `int`, 1–900 | `900` | Parameter count that raises `PlanTooLargeError` (compiler spec §9.5) |
| `mborobak.sql.cacheSize` | `int` ≥ 0 | `256` | Compiled-SQL cache entries; `0` turns the cache off |
| `mborobak.show.truncate` | `int` ≥ 0 | `20` | Default cell width of `show()` |

Unknown `mborobak.*` keys raise `UnsupportedOperationError` listing the valid keys.

### 3.4 `table(name)`

- `"Sales"` refers to `SQLUser.Sales`, the IRIS default schema (study §2.4). `"Finance.Sales"` names the schema.
- Names are case-insensitive, like IRIS identifiers. Names containing a dot or other special characters use backticks, as in PySpark: ``"`My.Schema`.`Odd Table`"``.
- A missing table raises `TableNotFoundError` at the `table()` call.
- Columns of unsupported IRIS types (streams, `%List`, binary) stay in the catalog; selecting one, including through `select("*")` or an action on the unmodified table, raises `UnsupportedTypeError` naming the column and suggesting `drop()` (compiler spec §3).

### 3.5 `sql(query, args=None)`

- `query` is one IRIS `SELECT` statement (or `WITH … SELECT`). Anything else raises `UnsupportedOperationError` before running.
- `args` is a list of values for `?` markers, in order. The values are sent as parameters, never pasted into the text. Named parameters (`:name`, PySpark's dict form) are P1.
- The result is a `DataFrame` like any other: transformations wrap the query as a subquery (scope §6.7). Its schema comes from preparing the statement, without fetching rows (scope §5.2).
- A `sql()` DataFrame used on its own (for example `session.sql(q).collect()`) runs the query exactly as written, so its `ORDER BY` holds.
- When transformations follow, the query becomes a subquery, and Mborobak adjusts it the way IRIS requires (prototype, verified on IRIS 2026.2):
  - an `ORDER BY` without `TOP` gets `TOP ALL` (compiler spec §7.2);
  - a `UNION … ORDER BY` is wrapped as `SELECT TOP ALL * FROM (union) ORDER BY …`, because IRIS rejects it as a subquery even with `TOP ALL`;
  - a `WITH` clause is moved to the front of the final statement, because IRIS rejects `WITH` inside a subquery. Combining `sql()` queries that use `WITH` works only when they are the same query with the same args (for example a self-join); otherwise it raises `UnsupportedOperationError`, because two `WITH` clauses would clash at the front of the statement.
- A later transformation may still drop the order, as in Spark.
- Result columns must have distinct names before transformations can follow; IRIS names unaliased expressions `Expression_1`, `Expression_2`, …, so alias them in the SQL.
- Column types come from the driver's result metadata. A column of a type Mborobak doesn't support (for example `TIME`) raises `UnsupportedTypeError` at the `sql()` call; cast it in the SQL.

**Differs from PySpark:** the SQL dialect is IRIS SQL, not Spark SQL.

## 4. DataFrame

### 4.1 Transformations

All return a new `DataFrame`. Column arguments accept a column name (`str`) or a `Column`, unless the table says otherwise. "Lookup" means a cached schema lookup may run (§8).

| Method | Arguments | Notes |
|--------|-----------|-------|
| `select(*cols)` | names, `Column`s, `"*"`, or one list of these | `"*"` expands to all columns |
| `filter(condition)`, `where(condition)` | `Column` of boolean type | A non-boolean column raises `AnalysisError`. **Differs:** SQL-string conditions (`filter("x > 1")`) are P1 and raise `UnsupportedOperationError` |
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
| `join(other, on=None, how=None)` | `on`: name, list of names, or a boolean `Column`; `how`: §4.2 | `on=None` gives a cross join, as in PySpark 3+ |
| `crossJoin(other)` | `DataFrame` | |
| `groupBy(*cols)`, `groupby(...)` | columns | Returns `GroupedData` (§4.3) |
| `agg(*exprs)` | aggregate `Column`s or a `dict` | Same as `groupBy().agg(...)` |

### 4.2 Join types

| `how` | Meaning |
|-------|---------|
| `"inner"` (default) | Inner join |
| `"left"`, `"leftouter"`, `"left_outer"` | Left outer |
| `"right"`, `"rightouter"`, `"right_outer"` | Right outer |
| `"full"`, `"fullouter"`, `"full_outer"`, `"outer"` | Full outer |
| `"cross"` | Cross join |
| `"semi"`, `"leftsemi"`, `"left_semi"`, `"anti"`, `"leftanti"`, `"left_anti"` | P1: raise `UnsupportedOperationError` |

Output columns follow compiler spec §6.3.

### 4.3 `GroupedData`

| Method | Notes |
|--------|-------|
| `agg(*exprs)` | Aggregate `Column`s, optionally aliased. Also `agg({"amount": "sum", "id": "count"})`, as in PySpark |
| `count()` | Adds a `count` column |
| `sum(*cols)`, `avg(*cols)`, `mean(*cols)`, `min(*cols)`, `max(*cols)` | Column names; with no names, all numeric columns (`min`/`max`: all columns), as in PySpark |

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

- P1: `unionByName`, `dropDuplicates`, `toLocalIterator`, `selectExpr`, `filter`/`where` with a string, semi and anti joins, `createDataFrame`, window functions, date and string functions, `write`.
- Not planned: `cache`, `persist`, `unpersist`, `checkpoint`, `repartition`, `coalesce`, `rdd`, `foreach`, `mapInPandas`, `toArrow`, streaming, UDFs (P2).

`cache()` and `persist()` raise rather than doing nothing, because a silent no-op would hide the fact that every action re-runs the query.

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
| `cast(dataType)`, `astype(dataType)` | A `DataType` or a type name (`"int"`, `"bigint"`, `"double"`, `"decimal(10,2)"`, `"string"`, `"boolean"`, `"date"`, `"timestamp_ntz"`). Invalid text raises `MborobakCastError` at the action (compiler spec §7.7) |
| `isNull()`, `isNotNull()` | |
| `eqNullSafe(other)` | Null-safe equality (`<=>` in Spark) |
| `isin(*values)` | Values or one list. Counts toward the parameter limit |
| `like(pattern)` | SQL `LIKE` with `%` and `_`. Escape characters are P1 |
| `between(lower, upper)` | Inclusive |
| `asc()`, `desc()`, `asc_nulls_first()`, `asc_nulls_last()`, `desc_nulls_first()`, `desc_nulls_last()` | Sort orders |
| `when(cond, value)`, `otherwise(value)` | On a `when()` result only |

### 6.4 Functions

`mborobak.sql.functions` in P0:

| Function | Notes |
|----------|-------|
| `col`, `column`, `lit`, `when` | §6.1 |
| `count(col)` | `count("*")` and `count(F.lit(1))` count rows; `count(col)` counts non-NULL values |
| `countDistinct(col, *cols)`, `count_distinct(...)` | |
| `sum(col)`, `avg(col)`, `mean(col)`, `min(col)`, `max(col)` | Types per compiler spec §3 |
| `asc(name)`, `desc(name)`, and the `_nulls_` variants | Same as the `Column` methods |

Accepting a column name where PySpark does (`F.sum("amount")`) works the same way.

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
