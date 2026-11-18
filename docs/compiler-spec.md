# SQL Compiler Specification

Scientificloud · Mborobak · Control document 1 of 3 · Draft 0.1 · 2026-11-18

Implements [mborobak-scope.md](mborobak-scope.md) §4–§6. IRIS facts cited as "study §x" come from [iris-behavior-study.md](iris-behavior-study.md) (IRIS 2026.2); facts marked **(probe 2026-11-18b)** come from a follow-up check on the same instance, recorded in §12.

## 1. Purpose and boundaries

This document defines how a chain of DataFrame operations becomes one IRIS SQL statement plus its parameters. It covers the P0 operations in scope §5.1. It does not cover the public API's signatures and error messages (control document 2, the API contract) or the PySpark comparison (control document 3, the compatibility matrix).

Design principles:

1. **Plan first, SQL last.** Operations build an immutable plan. SQL text exists only at the end, generated from a resolved plan. No step edits SQL strings.
2. **One action, one data query** (scope §5.2). Schema lookups are separate and cached.
3. **Correct before fast.** Required translations (§7) always run. Optional rewrites (§8) may only change performance, each can be switched off, and tests run with them on and off (scope §6.10).
4. **Deterministic.** The same plan always produces the same SQL text and parameter list, so compiled SQL can be cached (scope §7) and `explain()` output is stable.
5. **Explicit failure.** Anything that can't be translated correctly raises a Mborobak error at compile time (scope §6.9). Nothing silently falls back to running in Python.

## 2. Pipeline

```
DataFrame API calls
      │  build
      ▼
Logical plan (unresolved)        immutable tree; column references by name
      │  analyze (uses Catalog, cached schema lookups)
      ▼
Resolved plan                    every column has an id, a type and nullability
      │  required translations (§7)
      ▼
Translated plan                  IRIS-safe: collation, guards, ordering, limits
      │  optional rewrites (§8), each switchable
      ▼
Optimized plan
      │  SQL generation (§9)
      ▼
SqlStatement(text, params, output schema, fingerprint)
      │  Execution adapter
      ▼
Rows → converted to schema types → collect() / show() / toPandas()
```

Each stage is a pure function of its input (plus the cached catalog), which keeps every stage unit-testable without IRIS.

## 3. Types

| Mborobak type | IRIS types read from `INFORMATION_SCHEMA.COLUMNS.DATA_TYPE` | Python value |
|---------------|---------------------------------------------------------------|--------------|
| `IntegerType` | `integer`, `smallint`, `tinyint` | `int` |
| `LongType` | `bigint` | `int` |
| `DoubleType` | `double`, `float`, `real` | `float` |
| `DecimalType(p, s)` | `numeric`, `decimal`, `money` (precision and scale from the catalog) | `decimal.Decimal` |
| `StringType` | `varchar`, `char`, `longvarchar` | `str` |
| `BooleanType` | `bit` | `bool` |
| `DateType` | `date` | `datetime.date` (the driver returns `str`; converted, study §2.7) |
| `TimestampNTZType` | `timestamp`, `datetime`, `posixtime` | `datetime.datetime` (no time zone, scope §6.8) |

Other IRIS types (streams, `%List`, binary) are unsupported in P0: selecting such a column raises `UnsupportedTypeError`, but the table can still be used without it.

**Expression typing follows PySpark (D9: Spark 4.3, ANSI on)** for the P0 operators:

| Expression | Result type |
|------------|-------------|
| `int + int`, `-`, `*` | `IntegerType` (ANSI overflow: §7.6) |
| `int / int`, any `/` with a non-decimal side | `DoubleType` |
| `decimal op decimal` | `DecimalType` with Spark's precision/scale rules, capped at 38 |
| `int op double` | `DoubleType` |
| comparisons, `isNull`, `isin`, `like`, `between`, `&`, `\|`, `~` | `BooleanType` |
| `count`, `countDistinct` | `LongType`, never null |
| `sum(int)` | `LongType`; `sum(decimal(p,s))` → `DecimalType(min(38, p+10), s)`; `sum(double)` → `DoubleType` |
| `avg(int / double)` | `DoubleType`; `avg(decimal(p,s))` → `DecimalType(min(38, p+4), min(38, s+4))` |
| `min`, `max` | type of the argument |

IRIS returns other types in several of these cases (for example `Decimal` for `7/2`, study §2.6). The SQL generator adds a `CAST` where the difference is visible in results, and the execution adapter converts every value to the schema type, so `collect()` and `toPandas()` always follow the schema, not the driver.

## 4. Expressions

All expression nodes are immutable dataclasses. Every expression knows its children; after analysis it also knows its type and nullability.

| Node | Fields | Notes |
|------|--------|-------|
| `UnresolvedColumn` | `name`, `plan_id` (optional) | `col("x")` has no `plan_id`; `df["x"]` and `df.x` carry the id of `df`'s plan |
| `ColumnRef` | `column_id`, `name`, `type`, `nullable`, `collation` | Resolved form; `column_id` is unique within a session |
| `Literal` | `value`, `type` | Always becomes a `?` parameter (§9.5), never inline text |
| `Alias` | `child`, `name` | Gives an output column its name |
| `BinaryArithmetic` | `op` (`+ - * / %`), `left`, `right` | |
| `Comparison` | `op` (`= <> < <= > >=`, null-safe `<=>`), `left`, `right` | |
| `And`, `Or`, `Not` | children | |
| `IsNull`, `IsNotNull` | `child` | |
| `In` | `child`, `values` | List of expressions, usually literals |
| `Like` | `child`, `pattern` | |
| `Between` | `child`, `low`, `high` | Lowered to `>= AND <=` |
| `Cast` | `child`, `to_type` | Validity guard per §7.7 |
| `CaseWhen` | `branches[(cond, value)]`, `otherwise` | `when()` without `otherwise()` means `ELSE NULL` |
| `AggregateCall` | `function`, `args`, `distinct` | `count`, `sum`, `avg`, `min`, `max`, `countDistinct` |
| `SortOrder` | `child`, `direction`, `nulls` | Default nulls: first for ascending, last for descending (Spark default) |

**Internal nodes added by required translations (§7),** never created by users: `Exact(child)` (renders `%EXACT(...)`), `DivisionGuard`, `CastGuard`, `NullOrderKey`.

## 5. Plan nodes

| Node | Fields | Output columns |
|------|--------|----------------|
| `TableScan` | `schema`, `table`, catalog columns | One `ColumnRef` per catalog column, new ids |
| `SqlSource` | `sql`, `params` | From statement metadata (scope §5.2) |
| `Project` | `child`, `exprs` (each an `Alias` or a `ColumnRef`) | The listed expressions |
| `Filter` | `child`, `condition` | Same as child |
| `Join` | `left`, `right`, `join_type`, `condition` or `using_columns` | See §6.3 |
| `Aggregate` | `child`, `grouping_exprs`, `agg_exprs` | Grouping columns, then aggregates |
| `Sort` | `child`, `orders` | Same as child |
| `Limit` | `child`, `n` | Same as child |
| `Distinct` | `child` | Same as child |
| `Union` | `children`, `by_name=False` | First child's names; types widened |
| `SubqueryAlias` | `child`, `alias` | Same columns, qualified by `alias` (from `df.alias("a")`) |

`withColumn`, `withColumnRenamed`, `drop` and `select` are all lowered to `Project` by the API layer. `where` is `filter`. `unionAll` is `union` (D4: duplicates are kept).

## 6. Analyzer

### 6.1 Name resolution

- Names are matched **case-insensitively**, as in PySpark's default (`spark.sql.caseSensitive=false`), which also matches IRIS identifiers (study §2.4). Output names keep the user's spelling.
- `col("x")` resolves against the child's output columns. Exactly one match is required: zero raises `ColumnNotFoundError` (with close-match suggestions); more than one raises `AmbiguousColumnError`.
- `df["x"]` carries `df`'s plan id and resolves to that plan's column `x`, even after joins. This is what makes `a.join(b, a["id"] == b["id"])` unambiguous.
- `df.alias("a")` qualifies columns, so `col("a.id")` resolves through the `SubqueryAlias`. Self-joins require an alias or `df[...]` references.

### 6.2 Checks

- Every non-aggregated column in an `Aggregate`'s output must be a grouping expression (`AnalysisError` otherwise).
- `Filter` conditions must be `BooleanType`.
- `Union` children must have the same number of columns. Numeric types are widened (int → long → decimal → double); any other mismatch raises `AnalysisError`. This may be stricter than Spark for some type pairs; the compatibility matrix records where.
- Aggregates are not allowed inside `Filter` or `Project` without a `groupBy`; a filter after `agg()` is allowed and becomes `HAVING` or an outer `WHERE` (§9.3).
- `Literal` values must be representable: NaN and ±infinity are allowed only as parameters, which they always are (§9.5).

### 6.3 Join output

| Join form | Output columns |
|-----------|----------------|
| `a.join(b, condition)` | All of `a`'s columns, then all of `b`'s. Duplicate names are allowed but must be referenced through `a[...]`/`b[...]` or aliases |
| `a.join(b, "id")` or `["id", ...]` | The key columns once, then `a`'s other columns, then `b`'s. For `inner` and `left` the key comes from `a`; for `right` from `b`; for `full` it is `COALESCE(a.id, b.id)` |
| `cross` | All of `a`'s, then all of `b`'s |

Supported `join_type`: `inner`, `left`/`left_outer`, `right`/`right_outer`, `full`/`full_outer`/`outer`, `cross`. All verified on IRIS 2026.2 **(probe 2026-11-18b)**. `left_semi` and `left_anti` are P1.

### 6.4 Schema lookups

The `Catalog` reads `INFORMATION_SCHEMA.COLUMNS` once per table per session and caches it (scope §5.2). It returns names, types, precision, scale, nullability, and **collation**. `COLLATION_NAME` is NULL in `INFORMATION_SCHEMA` (study §2.4), so the source for collation is still open (§13, O1). Until it is found, the catalog treats every string column as `SQLUPPER`, which is safe: wrapping an already-exact column in `%EXACT` gives the same result, only possibly slower.

## 7. Required translations

These run on every plan. They define correctness and can't be switched off.

### 7.1 String collation (D10)

When the session setting `string_semantics` is `"pyspark"` (the default), any string expression whose collation is not exact is wrapped in `Exact(...)` where collation affects the result:

| Position | Rendered as |
|----------|-------------|
| Comparison, `IN`, `LIKE`, `BETWEEN` operands | `%EXACT(col) = ?` |
| Join conditions on strings | `%EXACT(a.k) = %EXACT(b.k)` |
| Grouping keys | `GROUP BY %EXACT(col)`, and the matching output column `%EXACT(col) AS "col"`, so original values come back |
| `DISTINCT` and `UNION` (distinct) | Each string output column projected as `%EXACT(col)` |
| `Sort` keys | `ORDER BY %EXACT(col)` |
| `min`, `max` on strings | `MIN(%EXACT(col))` |
| `countDistinct` on strings | `COUNT(DISTINCT %EXACT(col))` |

All forms verified **(probe 2026-11-18b)**. With `string_semantics="iris"`, no wrapping happens and the compatibility matrix lists the differences. String literals and parameters are never wrapped (they compare exactly already, study §2.5).

### 7.2 Ordering placement

- `ORDER BY` is emitted only on the outermost `SELECT`, or inside a subquery together with `TOP` (study §2.1).
- A `Sort` below other operators (for example `orderBy().limit().filter()`) is kept inside its subquery only when a `Limit` depends on it; it is then rendered with `TOP n`. The final output order is re-applied at the outermost level whenever any `Sort` exists in the plan and no later operator destroys order (`Aggregate`, `Distinct`, `Union`, `Join` destroy it, as in Spark).
- A `Sort` that no `Limit` depends on and that is followed by an order-destroying operator is dropped (it has no observable effect).

### 7.3 Limits

- A `Limit` that ends up inside a subquery renders as `TOP n` (`LIMIT` is rejected there, study §2.2).
- The outermost `Limit` renders as `TOP n` too, so one form is used everywhere. `LIMIT … OFFSET` is reserved for a future `offset()` operation.
- `TOP` takes a `?` parameter.

### 7.4 Explicit null ordering

When a `SortOrder` asks for the non-default null position (`asc_nulls_last`, `desc_nulls_first`), the generator adds a sort key in front of it: `CASE WHEN x IS NULL THEN 1 ELSE 0 END` (ascending nulls last) or `… THEN 0 ELSE 1 END` (descending nulls first). IRIS has no `NULLS FIRST/LAST` syntax (study §2.6).

### 7.5 Division

Every `/` and `%` whose divisor isn't a non-zero literal is wrapped in a `DivisionGuard`:

```sql
CASE WHEN d = 0 THEN <error trigger> ELSE n / d END
```

IRIS can't raise a custom error from SQL without installing server-side code, which the scope rules out (scope §7). The **error trigger** is an expression known to fail with SQLCODE -400. The execution adapter maps a -400 from a plan that contains guards to `MborobakArithmeticError`, naming the possible causes present in the plan (division by zero, invalid cast). Exact attribution to one expression is open (§13, O2).

With ANSI mode on (D9) this matches Spark's behavior (an error); the user never sees an unexplained "fatal error".

### 7.6 Integer overflow

IRIS doesn't overflow `INTEGER` arithmetic (study §2.6). In P0 this is a **documented known difference**, not a guard: checking every operation would cost too much. The compatibility matrix lists it.

### 7.7 Casts

| Cast | Rendering |
|------|-----------|
| string → integer, long | `CASE WHEN ISNUMERIC(TRIM(s)) = 1 AND <integer pattern> THEN CAST(TRIM(s) AS INTEGER) WHEN s IS NULL THEN NULL ELSE <error trigger> END` |
| string → double, decimal | Same, without the integer pattern (IRIS returns 0 / 0.0 for invalid text, study §2.6, **probe 2026-11-18b**) |
| numeric → string, numeric → numeric, date/timestamp ↔ string | Plain `CAST` |

`ISNUMERIC(' 1')` is 0 and `ISNUMERIC('1e3')` is 1 **(probe 2026-11-18b)**, so the value is trimmed first (Spark trims), and the integer pattern rejects exponents and decimal points for integer targets. Numeric precision overflow is a documented known difference in P0 (§7.6).

### 7.8 Never-emitted constructs

The generator must not produce (scope §6.12): `GROUP BY` positions; `IS NULL` or any other boolean expression directly in the `SELECT` list (rendered as `CASE WHEN <condition> THEN 1 ELSE 0 END` there, typed `BooleanType` and converted to `bool` on fetch); NaN or infinity literals; `%PARALLEL` after a table name; `ORDER BY` or `LIMIT` inside subqueries (§7.2–7.3).

## 8. Optional rewrites

Each rewrite has a name, a precondition and a session switch (`optimizer.<name>`, default on). The test suite runs every test with all rewrites on and with all off and compares results (scope §6.10).

| Rewrite | Does | Precondition |
|---------|------|--------------|
| `collapse_project` | Merges adjacent `Project`s | The outer one only references columns or deterministic expressions of the inner one |
| `merge_filter` | Combines adjacent `Filter`s with `AND` | None |
| `push_filter_through_project` | Moves a `Filter` below a `Project` | The condition only uses columns that the `Project` passes through unchanged |
| `prune_columns` | Drops unused columns from lower `Project`s and scans | Never drops a column that a `Distinct`, `Union` or `Aggregate` needs |
| `eliminate_sort` | Removes a `Sort` that has no observable effect (§7.2) | Not inside a subquery with `TOP` |

None of these changes duplicate counts, so none needs the extra duplicate-count test (scope §6.10). Pre-aggregation and other rewrites that would are deferred to release 0.2.

## 9. SQL generation

### 9.1 SELECT blocks

The generator walks the optimized plan bottom-up and builds `SelectBlock`s:

```
SelectBlock(
  from_      # TableRef | DerivedTable(SelectBlock, alias) | JoinTree
  where      # list of conditions, AND-ed
  group_by   # list of expressions, or None
  having     # list of conditions
  select     # list of (expression, output name)
  distinct   # bool
  order_by   # list of SortOrder (outermost block, or with top)
  top        # parameter index or None
)
```

Each plan node either **merges** into the current block or **wraps** it: the current block becomes a derived table `t<n>` and a new block starts on top.

### 9.2 Merge rules

| Node arriving | Merges into the current block when… | Otherwise |
|---------------|-------------------------------------|-----------|
| `Filter` | The block has no `select` yet beyond pass-through, no `group_by`, no `distinct`, no `top` → goes to `where`. The block has `group_by` and no `select` beyond the aggregate output → goes to `having` | Wrap |
| `Project` | The block has no `distinct`, `top` or `group_by`, and the new expressions only use columns visible in `from_` | Wrap |
| `Aggregate` | The block has no `group_by`, `distinct` or `top`, and its `select` is pass-through | Wrap |
| `Sort` | Always recorded as the block's pending `order_by`; emitted only per §7.2 | — |
| `Limit` | The block has no `top` → set `top` | Wrap |
| `Distinct` | The block has no `top` → set `distinct` | Wrap |
| `Join` | Never merges: both sides become `from_` entries (tables stay tables; anything else becomes a derived table) | — |
| `Union` | Never merges: each child becomes its own block; the union becomes a derived table if anything follows it | — |

### 9.3 Names and aliases

- Every identifier is double-quoted (`"SQLUser"."Sales"`, `"customer_id"`). Quoted identifiers are case-insensitive in IRIS and safe for reserved words (study §2.4, **probe 2026-11-18b**).
- Unqualified table names resolve to schema `SQLUser` (study §2.4).
- Derived tables are named `t0`, `t1`, … in the order they are created during the bottom-up walk, which keeps the text deterministic.
- When two output columns would share a name inside a derived table (after a join), the generator gives the inner one an internal alias `c<column_id>`; the outermost `SELECT` restores the user's names.
- `ORDER BY` may use output aliases (verified **probe 2026-11-18b**).

### 9.4 Grouping

`GROUP BY` repeats each grouping expression in full (never an ordinal, §7.8). The block's `select` lists grouping expressions first, then aggregates, each with its output alias.

### 9.5 Parameters

- Every `Literal` becomes `?`. The parameter list is built in the order the `?` marks appear in the final text.
- The compiler counts parameters. Above **900**, it raises `PlanTooLargeError` with the count and the operation that contributed most (usually an `isin()` list). IRIS fails at about 999 (study §2.3); 900 leaves room for literals that IRIS itself extracts.
- `lit(None)` renders as `NULL` text (wrapped in a `CAST` when the type matters), not as a parameter. A comparison with `None` (`col("x") == None`) follows SQL rules and yields NULL, as in Spark; users test for nulls with `isNull()`.

### 9.6 Fingerprint and cache

`SqlStatement.fingerprint` is a hash of the optimized plan with literal values removed. The session caches `fingerprint → SQL text`, so running the same pipeline shape again with different values skips analysis and generation (scope §7). Parameter values are bound per execution.

## 10. `explain()`

`df.explain()` prints, without running anything:

```
== Logical plan ==
Sort [revenue DESC]
+- Aggregate [customer_id] [sum(amount) AS revenue]
   +- Filter (status = 'PAID')
      +- TableScan SQLUser.Sales [id, customer_id, status, amount]

== Translations ==
collation: %EXACT on status (string, SQLUPPER)

== Rewrites ==
prune_columns: removed id

== SQL ==
SELECT "customer_id", SUM("amount") AS "revenue"
FROM "SQLUser"."Sales"
WHERE %EXACT("status") = ?
GROUP BY "customer_id"
ORDER BY "revenue" DESC

== Parameters ==
1: 'PAID'
```

`df.explain(mode="sql")` prints only the SQL and parameters.

## 11. Worked examples

Tables: `Sales(id INTEGER, customer_id INTEGER, status VARCHAR, amount NUMERIC(10,2))`, `Customers(id INTEGER, name VARCHAR, country VARCHAR)`.

**E1. Scope example** (filter, group, aggregate, sort): see §10.

**E2. Sort and limit inside, then filter**

```python
sales.orderBy(col("amount").desc()).limit(10).filter(col("status") == "PAID")
```

```sql
SELECT "t0"."id", "t0"."customer_id", "t0"."status", "t0"."amount"
FROM (SELECT TOP ? "id", "customer_id", "status", "amount"
      FROM "SQLUser"."Sales"
      ORDER BY "amount" DESC) "t0"
WHERE %EXACT("t0"."status") = ?
ORDER BY "t0"."amount" DESC
-- params: 10, 'PAID'
```

**E3. Join by column name, full outer**

```python
sales.join(customers.withColumnRenamed("id", "customer_id"), "customer_id", "full")
```

```sql
SELECT COALESCE("t0"."customer_id", "t1"."customer_id") AS "customer_id",
       "t0"."id", "t0"."status", "t0"."amount", "t1"."name", "t1"."country"
FROM "SQLUser"."Sales" "t0"
FULL OUTER JOIN (SELECT "id" AS "customer_id", "name", "country"
                 FROM "SQLUser"."Customers") "t1"
  ON "t0"."customer_id" = "t1"."customer_id"
```

**E4. Group by a string, distinct count**

```python
customers.groupBy("country").agg(countDistinct("name").alias("names"))
```

```sql
SELECT %EXACT("country") AS "country", COUNT(DISTINCT %EXACT("name")) AS "names"
FROM "SQLUser"."Customers"
GROUP BY %EXACT("country")
```

**E5. Division and cast**

```python
sales.select((col("id") / col("customer_id")).alias("ratio"), col("status").cast("int").alias("s"))
```

Integer divided by integer is `DoubleType` in Spark (§3), so the dividend is cast first:

```sql
SELECT CASE WHEN "customer_id" = 0 THEN <error trigger>
            ELSE CAST("id" AS DOUBLE) / "customer_id" END AS "ratio",
       CASE WHEN "status" IS NULL THEN NULL
            WHEN ISNUMERIC(TRIM("status")) = 1 AND <integer pattern> THEN CAST(TRIM("status") AS INTEGER)
            ELSE <error trigger> END AS "s"
FROM "SQLUser"."Sales"
```

## 12. Follow-up probe (2026-11-18b)

Run on the same IRIS 2026.2 instance as the study, in a scratch schema that was dropped afterwards:

| Check | Result |
|-------|--------|
| `FULL OUTER JOIN`, `FULL JOIN`, `RIGHT JOIN`, `CROSS JOIN` | All work; `COALESCE` of full-join keys works |
| String join without / with `%EXACT` | `'x'` joined `'X'` and `'Y'` joined `'y'` without; no matches with `%EXACT` (correct) |
| `%EXACT` in `WHERE … = ?`, `LIKE ?`, `IN (?, ?)` | Exact matching |
| `MIN(%EXACT(k))`, `MAX(%EXACT(k))`, `ORDER BY %EXACT(k)`, `SELECT DISTINCT %EXACT(k)`, `GROUP BY %EXACT(k)` | Original values, exact semantics |
| `ORDER BY` an output alias, including an aggregate alias | Works |
| Fully quoted identifiers, alias with a space | Work |
| `ISNUMERIC('abc', '1.5', ' 1', '1e3', '', '-2')` | 0, 1, **0**, **1**, 0, 1 |
| `CAST('abc' AS DOUBLE)` | `0.0` (silent, like `INTEGER`) |
| `BETWEEN ? AND ?`, `CASE WHEN v > ? THEN ? ELSE ? END` | Work |
| `TOP` in an aliased derived table; `AS` before a derived-table alias; `UNION ALL` in a derived table | Work |

IRISpark's notes list "outer join" as having no SQL mapping; on 2026.2 full outer joins work, so that note doesn't apply.

## 13. Open questions

| # | Question | Needed for |
|---|----------|------------|
| O1 | Where to read a column's collation (`INFORMATION_SCHEMA.COLUMNS.COLLATION_NAME` is NULL). Candidates: `%Dictionary.CompiledProperty`, the `SQLUPPER`/`EXACT` property parameters | §6.4, avoiding unneeded `%EXACT` |
| O2 | A stable error trigger, and how to tell which guard fired | §7.5, §7.7 |
| O3 | The exact `<integer pattern>` expression in IRIS SQL (`%PATTERN`, `LIKE`, or `$MATCH` via a SQL function) | §7.7 |
| O4 | Cost of `%EXACT` at 1M rows (index loss) | Benchmark, D10 |
| O5 | Whether `-0.0` and `0.0` group together, and how NaN compares and sorts | §3, compatibility matrix |
| O6 | Re-check everything on IRIS 2026.3.0AI build 154 (D1) | Before submission |
