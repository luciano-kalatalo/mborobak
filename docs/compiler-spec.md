# SQL Compiler Specification

Scientificloud · Mborobak · Control document 1 of 3 · Draft 0.4 · 2026-11-18

Implements [mborobak-scope.md](mborobak-scope.md) §4–§6. IRIS facts cited as "study §x" come from [iris-behavior-study.md](iris-behavior-study.md) (IRIS 2026.2); facts marked **(probe 2026-11-18b)** or **(probe 2026-11-18c)** come from follow-up checks on the same instance, recorded in §12. Draft 0.2 resolves O1, O2, O3 and O5 (§13) and adds §7.9 after probe 2026-11-18c. Draft 0.3 follows the first differential run against PySpark 4.3 ([compatibility matrix](compatibility-matrix.md) 0.2): the cast trigger moves from `LOG(0)` to `$LISTGET` because IRIS also raises `<ILLEGAL VALUE>` for `DOUBLE` division by zero, and casts from `double`/`decimal` to integers get a range check (§7.7).

## 1. Purpose and boundaries

This document defines how a chain of DataFrame operations becomes one IRIS SQL statement plus its parameters. It covers the P0 operations in scope §5.1. It does not cover the public API's signatures and error messages (control document 2, the [API contract](api-contract.md)) or the PySpark comparison (control document 3, the [compatibility matrix](compatibility-matrix.md)).

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
| `int / int`, any `/` with a `double` side | `DoubleType` |
| `decimal / int`, `int / decimal` | `DecimalType` by Spark's division rule, the integer side counting as `decimal(10,0)` (`long`: `decimal(20,0)`) and an integer literal as `decimal(<digits>,0)`; above 38 digits the scale is cut to keep the integer digits (minimum 6). IRIS computes about 19 significant digits (matrix NUM-13) |
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
| `UnresolvedColumn` | `name` | `col("x")`; `df["x"]` and `df.x` resolve at once to an `AttrRef` of `df`'s column |
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

**Internal nodes added by required translations (§7),** never created by users: `Exact(child)` (renders `%EXACT(...)`), `CastGuard`, `NullOrderKey`, `DoubleSortKey`. (Draft 0.1 also had `DivisionGuard`; §7.5 explains why it was removed.)

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
- **Columns dropped by an earlier `select`.** As in Spark, `filter` and `orderBy` may refer to a column that the `select` directly below them (possibly under other filters, sorts and limits) dropped: the analyzer adds the column back to that projection, applies the filter or sort, and projects the original columns again (matrix NAME-07). A name found nowhere still raises `ColumnNotFoundError`.

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

The `Catalog` reads `INFORMATION_SCHEMA.COLUMNS` once per table per session and caches it (scope §5.2). It returns names, types, precision, scale, nullability, and **collation**.

`COLLATION_NAME` is NULL in `INFORMATION_SCHEMA.COLUMNS` (study §2.4), and so is `%Dictionary.CompiledProperty.SqlCollation`. The collation is in the property's `Parameters` string **(probe 2026-11-18c)**:

```sql
SELECT p.SqlFieldName, p.Parameters
FROM INFORMATION_SCHEMA.TABLES t
JOIN %Dictionary.CompiledProperty p ON p.parent = t.CLASSNAME
WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
```

`Parameters` is a flat `KEY,value,KEY,value,…` string. The catalog reads the value after `COLLATION`:

| `COLLATION` value | DDL that produced it | Catalog collation | `%EXACT` needed |
|-------------------|----------------------|-------------------|-----------------|
| empty | none (default) | namespace default, taken as `SQLUPPER` | yes |
| `EXACT` | `%EXACT` or `COLLATE %EXACT` | `EXACT` | no |
| `SQLSTRING` | `COLLATE SQLSTRING` | `SQLSTRING` (case-sensitive, ignores trailing spaces) | yes |
| anything else (`TRUNCATE(n)`, `SQLUPPER(n)`, …) | various | as read | yes |

Rules:

- Only `EXACT` skips the wrapper; every other value, and any parsing failure, falls back to wrapping. Wrapping an exact column gives the same result, only possibly slower, so the fallback is safe.
- An empty value means the namespace default. It is assumed to be `SQLUPPER`, the IRIS default; a namespace configured otherwise still gets correct results because of the fallback.
- Tables without a class (`CLASSNAME` NULL, for example linked tables) also fall back to wrapping.
- Both lookups share the one cached catalog round trip per table.

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
- When a subquery has to be closed over a sorted block whose sort key a `select` dropped (`orderBy(d).select(id).limit(3).filter(…)`), the key travels up as a hidden column of the subquery, so the order can be applied again outside (matrix ORD-06). Not under `DISTINCT`, where an extra column would change the result.

### 7.3 Limits

- A `Limit` that ends up inside a subquery renders as `TOP n` (`LIMIT` is rejected there, study §2.2).
- The outermost `Limit` renders as `TOP n` too, so one form is used everywhere. `LIMIT … OFFSET` is reserved for a future `offset()` operation.
- `TOP` takes a `?` parameter.

### 7.4 Explicit null ordering

When a `SortOrder` asks for the non-default null position (`asc_nulls_last`, `desc_nulls_first`), the generator adds a sort key in front of it: `CASE WHEN x IS NULL THEN 1 ELSE 0 END` (ascending nulls last) or `… THEN 0 ELSE 1 END` (descending nulls first). IRIS has no `NULLS FIRST/LAST` syntax (study §2.6).

### 7.5 Division and runtime errors

IRIS already raises SQLCODE -400 on a zero divisor at run time, and `NULL / 0` is `NULL` **(probe 2026-11-18c)**, which is Spark's ANSI behavior (D9). The draft 0.1 `DivisionGuard` is therefore **dropped**: `/` and `%` render as plain `n / d` and `MOD(n, d)`.

The -400 driver message carries the ObjectScript error in its `%msg` part, for example `<DIVIDE>` **(probe 2026-11-18c)**. The connection stays usable afterwards. The execution adapter reads that code and raises a Mborobak error:

| `%msg` code | Mborobak error | Possible causes in the plan |
|-------------|----------------|-----------------------------|
| `<DIVIDE>` | `MborobakArithmeticError` ("division by zero") | any `/` or `%` |
| `<ILLEGAL VALUE>` | `MborobakArithmeticError` ("division by zero or invalid arithmetic") | `DOUBLE` division by zero (differential run, NUM-07) |
| `<RANGE>` | `MborobakCastError` ("invalid input for cast") | a `CastGuard` (§7.7) |
| `<MAXNUMBER>` | `MborobakArithmeticError` ("numeric overflow") | numeric arithmetic or cast |
| other | `MborobakExecutionError` with the IRIS text attached | — |

The message lists the candidate expressions of that kind from the plan (for example, the two divisions in the `select`). Narrowing it down to one expression would need extra queries and is not done in P0.

**Error triggers must depend on a row value.** IRIS evaluates constant subexpressions before reading rows: `CASE WHEN v = 99 THEN 1/0 ELSE v END` fails even when no row has `v = 99`, and so do `1/?`, `LOG(0)` and `SQRT(-1)` **(probe 2026-11-18c)**. A trigger such as `$LISTGET(s, LENGTH(s) - LENGTH(s) - 5)` only fails when its branch is taken. The same eager evaluation affects user expressions: a constant division by zero anywhere in a statement fails the statement, as Spark's constant folding does too.

### 7.6 Integer overflow

IRIS doesn't overflow `INTEGER` arithmetic (study §2.6). In P0 this is a **documented known difference**, not a guard: checking every operation would cost too much. The compatibility matrix lists it.

### 7.7 Casts

| Cast | Rendering |
|------|-----------|
| string → integer, long | `CASE WHEN s IS NULL THEN NULL WHEN ISNUMERIC(TRIM(s)) = 1 AND TRIM(s) %PATTERN '0.1(1"-",1"+")1.N' THEN CAST(TRIM(s) AS INTEGER) ELSE $LISTGET(s, LENGTH(s) - LENGTH(s) - 5) END` |
| string → double | As for decimal, plus `WHEN UPPER(TRIM(s)) IN ('NAN') THEN ?`, `… IN ('INF', '+INF', 'INFINITY', '+INFINITY') THEN ?` and `… IN ('-INF', '-INFINITY') THEN ?` before the `ISNUMERIC` test, with NaN, +∞ and −∞ as parameters (CAST-07) |
| string → decimal | `CASE WHEN s IS NULL THEN NULL WHEN ISNUMERIC(TRIM(s)) = 1 THEN CAST(TRIM(s) AS NUMERIC(p,s)) ELSE $LISTGET(s, LENGTH(s) - LENGTH(s) - 5) END` |
| boolean → string | `CASE WHEN p THEN 'true' WHEN NOT (p) THEN 'false' END` (or `b = 1` / `b = 0` for a `BIT` value); a plain `CAST` gives `'1'`/`'0'` (CAST-16) |
| any guarded cast of a constant (`lit("7").cast("int")`) | Folded in Python with Spark's rules and sent as one parameter; invalid input raises the same Mborobak error when the action runs. A constant has no row value for the trigger to depend on (§7.5) |
| double, decimal → integer, long | `CASE WHEN x IS NULL THEN NULL WHEN x <> x OR x >= 2147483648 OR x <= -2147483649 THEN EXP(100000 + LENGTH(CAST(x AS VARCHAR(60))) - LENGTH(CAST(x AS VARCHAR(60)))) ELSE CAST(x AS INTEGER) END` (bounds ±2⁶³ for `long`) |
| numeric → string, other numeric → numeric, date/timestamp ↔ string | Plain `CAST` |

Why each part is there:

- **NULL first.** NULL input gives NULL, and the `ELSE` branch is only reached with a non-NULL `s`, so `LENGTH(s) - LENGTH(s) - 5` is −5 there and `$LISTGET` at position −5 raises `<RANGE>` (§7.5). Draft 0.2 used `LOG(0)`, whose `<ILLEGAL VALUE>` turned out to be ambiguous: IRIS raises the same code for `DOUBLE` division by zero.
- **Range check for double/decimal → integer.** IRIS turns NaN into 0 without an error, returns ±∞ as text (`'inf'`) that the driver can't read, and ignores overflow (`1e20` → `100000000000000000000`); Spark fails in all three cases (CAST-10, CAST-15). `x <> x` catches NaN (inside a `CASE`, §7.9), the bounds catch ±∞ and overflow, and `EXP(100000)` raises `<MAXNUMBER>` ("numeric overflow"). The bounds keep Spark's truncation: `2147483647.9` still casts to `2147483647`.
- **`ISNUMERIC(TRIM(s))`.** Spark trims before casting; `ISNUMERIC(' 1')` is 0 without the trim. It rejects `''`, `'abc'`, `'1-'`, `'0x1F'` and full-width digits such as `'１'` **(probe 2026-11-18c)**.
- **The integer pattern** rejects what `ISNUMERIC` accepts but an integer cast must not: `'1.5'`, `'1e3'`, `'1.'`, `'.5'`. It accepts an optional sign and leading zeros (`'+3'` → 3, `'007'` → 7). `%PATTERN` alone would accept `'１'`, so both tests are needed. The pattern is emitted as SQL text, not a parameter: it is fixed compiler text, never user input (§9.5).

Verified on 18 inputs **(probe 2026-11-18c)**: the combined rule accepts exactly the integer-shaped strings.

**Operands are computed once.** The guarded forms repeat their operand up to 13 times. When the operand is itself an expression (not a column or a constant), it becomes a hidden column of a subquery and the cast refers to that column; nested casts are handled the same way, innermost first. Operands containing an aggregate stay in place, since they must be computed after the `GROUP BY`.

**Known difference:** IRIS returns `12345678901234567890` from `CAST(… AS INTEGER)` and `CAST(… AS BIGINT)` without an error, where Spark raises an overflow for `long`. This falls under §7.6. Values above 18 digits are rare in text columns; the compatibility matrix lists the case.

### 7.8 Never-emitted constructs

The generator must not produce (scope §6.12): `GROUP BY` positions; `IS NULL` or any other boolean expression directly in the `SELECT` list (rendered as `CASE WHEN <condition> THEN 1 ELSE 0 END` there, typed `BooleanType` and converted to `bool` on fetch); NaN or infinity literals; `%PARALLEL` after a table name; `ORDER BY` or `LIMIT` inside subqueries (§7.2–7.3).

### 7.9 Sorting `DOUBLE` columns

IRIS uses `-1E19` as its internal NULL marker when it sorts `DOUBLE` values **(probe 2026-11-18c)**:

- `ORDER BY d` returns a stored `-1E19` as NULL, and NULLs sort between `-1E20` and `-1E18` instead of first.
- `-Infinity` sorts after the largest finite value, next to `+Infinity`.
- The same marker applies to `GROUP BY d`, `DISTINCT d` and `COUNT(DISTINCT d)`: `-1E19` merges with NULL. `GROUP BY d + 0` does not help.
- `WHERE d = -1E19`, joins on `d`, `MIN` and `MAX` are correct.

Every sort key of type `DoubleType` (or `FloatType`) is rendered as two keys, a bucket and the value plus zero:

```sql
ORDER BY CASE WHEN d IS NULL THEN 0
              WHEN d < -1.7976931348623157E308 THEN 1
              WHEN d <> d THEN 4
              WHEN d > 1.7976931348623157E308 THEN 3
              ELSE 2 END,
         d + 0
```

- **Buckets:** NULL, −∞, finite, +∞, NaN, which is Spark's ascending order. `DESC` applies to both keys, giving Spark's descending order; explicit null positions (§7.4) replace the NULL bucket.
- **`d + 0`:** sorting on an expression instead of the column stops the corrupted value being returned (`-1E19` comes back intact).
- **`d <> d`:** true only for NaN inside a `CASE`. As a bare `WHERE d <> d` the optimizer folds it to false, so it must stay inside the `CASE`.

Verified ascending and descending on 14 values including NULL, ±0, ±1E19, ±1E300, ±∞ and NaN **(probe 2026-11-18c)**. `NUMERIC` and integer sorts are not affected.

**Grouping and distinct are a known difference in P0:** a `-1E19` in a grouping key or `DISTINCT` column merges with NULL. A fix needs a key that keeps the two apart without changing the output value; it is open (§13, O7).

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
- When two output columns would share a name inside a derived table (after a join), the generator gives the later one a suffix (`id_2`); the outermost `SELECT` restores the user's names.
- IRIS rejects `.`, `,` and `^` inside a delimited identifier (probed on 2026.2). Unaliased names can contain them (`CAST(1.5 AS DECIMAL(10,2))`), so SQL aliases replace them with `_`, `;` and `~`. Result columns are named from the DataFrame's schema, never from the cursor, so users see the original names.
- `ORDER BY` may use output aliases (verified **probe 2026-11-18b**).

### 9.4 Grouping

`GROUP BY` repeats each grouping expression in full (never an ordinal, §7.8). The block's `select` lists grouping expressions first, then aggregates, each with its output alias.

### 9.5 Parameters

- Every `Literal` becomes `?`. The parameter list is built in the order the `?` marks appear in the final text.
- The compiler counts parameters. Above **900**, it raises `PlanTooLargeError` with the count and the operation that contributed most (usually an `isin()` list). IRIS fails at about 999 (study §2.3); 900 leaves room for literals that IRIS itself extracts.
- **Fixed compiler text is inline, not a parameter:** the `%PATTERN` string (§7.7), the `DOUBLE` bucket constants (§7.9), the `CASE` results of `NullOrderKey` (§7.4) and boolean projections (§7.8). None of it comes from users, and it stays out of the 900 count. (A `%PATTERN ?` parameter also works, probe 2026-11-18c, but the text form keeps `explain()` readable.)
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
SELECT CAST("id" AS DOUBLE) / "customer_id" AS "ratio",
       CASE WHEN "status" IS NULL THEN NULL
            WHEN ISNUMERIC(TRIM("status")) = 1
             AND TRIM("status") %PATTERN '0.1(1"-",1"+")1.N' THEN CAST(TRIM("status") AS INTEGER)
            ELSE $LISTGET("status", LENGTH("status") - LENGTH("status") - 5) END AS "s"
FROM "SQLUser"."Sales"
```

## 12. Follow-up probes

### 12.1 Probe 2026-11-18b

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

### 12.2 Probe 2026-11-18c

Script [`probes/compiler_followup_probe.py`](../probes/compiler_followup_probe.py), raw output [`probes/results-2026-11-18c-iris-2026.2.json`](../probes/results-2026-11-18c-iris-2026.2.json). Same IRIS 2026.2 instance (build 221U), scratch tables in `MborobakProbe`, dropped afterwards.

| Question | Check | Result |
|----------|-------|--------|
| O1 | `INFORMATION_SCHEMA.COLUMNS.COLLATION_NAME`, `CompiledProperty.SqlCollation` | NULL for all five DDL variants |
| O1 | `CompiledProperty.Parameters` | `COLLATION,` empty for default, `EXACT` for `%EXACT` and `COLLATE %EXACT`, `SQLSTRING`, `TRUNCATE(5)` (§6.4) |
| O1 | `INFORMATION_SCHEMA.TABLES.CLASSNAME` | Gives the class (`MborobakProbe.Coll`) to join on |
| O2 | `1/0`, `1/?` (=0), `LOG(0)`, `SQRT(-1)` in an untaken `CASE` branch | All fail with -400: constants are evaluated eagerly |
| O2 | `1/(v-v)`, `LOG(LENGTH(s)-LENGTH(s))` in an untaken branch | No error; fail only when taken |
| O2 | `TO_DATE('tag', …)`, `CAST('tag' AS DATE)` | -378 at prepare time (unusable) |
| O2 | `TO_NUMBER('tag')`, `CHAR(-1)`, `REPEAT('x', -1)` | No error (NULL or a value) |
| O2 | `%msg` of -400 | `<DIVIDE>` (division, `MOD`), `<ILLEGAL VALUE>` (`LOG`, `SQRT`, `POWER`), `<MAXNUMBER>` (`EXP`, huge cast), `<RANGE>` (`$LISTGET`) |
| O2 | Plain `10 / v` with `v = 0`; `NULL / 0` | -400 `<DIVIDE>`; NULL. The connection stays usable |
| O3 | `%PATTERN '0.1(1"-",1"+")1.N'` on 18 inputs | Accepts signed digit strings, rejects `1.5`, `1e3`, `1.`, `.5`, `''`, `-`; also accepts `'１'` (full-width) |
| O3 | `ISNUMERIC(TRIM(s))` on the same inputs | Rejects `'１'`, `'0x1F'`; accepts `1.5`, `1e3` |
| O3 | Combined rule (§7.7) | Exactly the integer-shaped strings; `'+3'` → 3, `'007'` → 7 |
| O5 | `-0.0` vs `0.0` | Equal in `WHERE`, one group, one `DISTINCT` value (Spark also normalizes) |
| O5 | NaN | Inserted by parameter; groups together and joins to itself (as in Spark); `MAX` ignores it (Spark returns NaN) |
| O5 | `CASE WHEN d <> d` vs `WHERE d <> d` | True for NaN only inside `CASE`; bare predicate folded to false |
| §7.9 | `ORDER BY d` on `DOUBLE` | `-1E19` returned as NULL; NULLs sort at `-1E19`; `-Infinity` sorts after finite values |
| §7.9 | Bucket key plus `d + 0` | Spark order ascending and descending, values intact |
| §7.9 | `GROUP BY d`, `GROUP BY d + 0`, `DISTINCT d`, `COUNT(DISTINCT d)` | `-1E19` merges with NULL |

## 13. Open questions

| # | Question | Needed for |
|---|----------|------------|
| O1 | ~~Where to read a column's collation~~ **Resolved:** `%Dictionary.CompiledProperty.Parameters` (§6.4) | — |
| O2 | ~~A stable error trigger, and how to tell which guard fired~~ **Resolved:** row-dependent `$LISTGET(s, LENGTH(s) - LENGTH(s) - 5)` for casts (draft 0.3), no guard for division, attribution by `%msg` code (§7.5) | — |
| O3 | ~~The exact integer pattern~~ **Resolved:** `ISNUMERIC(TRIM(s)) = 1 AND TRIM(s) %PATTERN '0.1(1"-",1"+")1.N'` (§7.7) | — |
| O4 | Cost of `%EXACT` at 1M rows (index loss) | Benchmark, D10 |
| O5 | ~~How `-0.0` and NaN group, compare and sort~~ **Resolved** (§12.2); `MAX` ignoring NaN goes to the compatibility matrix | — |
| O6 | Re-check everything on IRIS 2026.3.0AI build 154 (D1), including both probe scripts | Before submission |
| O7 | Grouping and `DISTINCT` on `DOUBLE` merge `-1E19` with NULL (§7.9). Candidate: group on an extra NULL flag and return the value via `MIN(d)`, which is correct; needs a probe and a cost check | §7.9, compatibility matrix |
| O8 | Whether to report the IRIS `-1E19` sort/group behavior to InterSystems | Before submission |
