# SQL Compiler Specification

Scientificloud · Mborobak · Control document 1 of 3 · Draft 0.10 · 2026-11-20

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
| `WindowExpr` | `function`, `partition`, `order`, `frame` | `function.over(window)`: an `AggregateCall` or a `WindowFunc` with its window (§7.11). `frame` is `rows` or `range` with offsets, None for unbounded |
| `WindowFunc` | `name`, `args` | `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first`, `last`; only valid inside `WindowExpr` |
| `Func` | `name`, `args` | A scalar function (§7.10). Typing, default name and SQL come from a registry keyed by `name`. Constant arguments (a scale, a pad string) stay `Literal` arguments so the default name shows them |

**Internal nodes added by required translations (§7),** never created by users: `Exact(child)` (renders `%EXACT(...)`), `TextOrder(child)` (renders `%EXACT(' ' || ...)`, §7.1), `CastGuard`, `NullOrderKey`, `DoubleSortKey`. (Draft 0.1 also had `DivisionGuard`; §7.5 explains why it was removed.)

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
| `=`, `<>`, `IN`, `LIKE` operands | `%EXACT(col) = ?` |
| `<`, `<=`, `>`, `>=` and `BETWEEN` operands | `%EXACT(' ' \|\| col) > %EXACT(' ' \|\| ?)` (see below) |
| Join conditions on strings | `%EXACT(a.k) = %EXACT(b.k)` |
| Grouping keys | `GROUP BY %EXACT(col)`, and the matching output column `%EXACT(col) AS "col"`, so original values come back |
| `DISTINCT` and `UNION` (distinct) | Each string output column projected as `%EXACT(col)` |
| `Sort` keys | `ORDER BY %EXACT(' ' \|\| col)` |
| `min`, `max` on strings | `SUBSTRING(MIN(%EXACT(' ' \|\| col)), 2)` |
| `greatest`, `least` on strings | `SUBSTRING(GREATEST(%EXACT(' ' \|\| …), …), 2)` (§7.10) |
| `nullif` on strings | `NULLIF(%EXACT(a), %EXACT(b))` (§7.10) |
| `countDistinct` on strings | `COUNT(DISTINCT %EXACT(col))` |

All forms verified **(probe 2026-11-18b)**. With `string_semantics="iris"`, no wrapping happens and the compatibility matrix lists the differences. In equality, string literals and parameters are never wrapped (they compare exactly already, study §2.5).

**Order of strings that look like numbers** (draft 0.5, probe 2026-11-19, §12.3). With `%EXACT`, and in columns declared `EXACT`, IRIS orders canonical numbers numerically and before all other strings: `'12345' > '5'`, `'10' > '9'`. Spark compares strings by code point: `'5' > '12345'`. Where order matters (`<`, `<=`, `>`, `>=`, `between`, sort keys, `min`/`max`, `greatest`/`least`), each side, literals included, becomes `TextOrder(x)`: `%EXACT(' ' || x)`. The leading space makes every value a plain string, and `%EXACT` keeps case and trailing spaces. `min`/`max` and `greatest`/`least` strip the space with `SUBSTRING(…, 2)`; NULL stays NULL. Equality, `IN`, grouping and `DISTINCT` are not affected (`'10'` and `'10.0'` stay different values) and keep `%EXACT(x)`. Release 0.1 used `%EXACT(x)` everywhere and ordered such strings as numbers (matrix STR-16).

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
| string → date | `TO_DATE(CASE WHEN s IS NULL THEN NULL WHEN TRIM(s) %PATTERN '4N' THEN TRIM(s) \|\| '-01-01' WHEN … '4N1"-"1.2N' THEN … \|\| '-01' WHEN … '4N1"-"1.2N1"-"1.2N' THEN TRIM(s) WHEN … followed by a space or `T` THEN $PIECE($PIECE(TRIM(s), ' ', 1), 'T', 1) ELSE $LISTGET(s, …) END, 'YYYY-MM-DD')` |
| string → timestamp | The same date shapes, plus `… 1(1" ",1"T")1.2N1":"1.2N.1(1":"1.2N.1(1"."1.9N))` → `REPLACE(TRIM(s), 'T', ' ')`, inside `TO_TIMESTAMP(…, 'YYYY-MM-DD HH24:MI:SS.FF')` |
| numeric → string, other numeric → numeric, date/timestamp → string, date ↔ timestamp | Plain `CAST` |

Why each part is there:

- **NULL first.** NULL input gives NULL, and the `ELSE` branch is only reached with a non-NULL `s`, so `LENGTH(s) - LENGTH(s) - 5` is −5 there and `$LISTGET` at position −5 raises `<RANGE>` (§7.5). Draft 0.2 used `LOG(0)`, whose `<ILLEGAL VALUE>` turned out to be ambiguous: IRIS raises the same code for `DOUBLE` division by zero.
- **Range check for double/decimal → integer.** IRIS turns NaN into 0 without an error, returns ±∞ as text (`'inf'`) that the driver can't read, and ignores overflow (`1e20` → `100000000000000000000`); Spark fails in all three cases (CAST-10, CAST-15). `x <> x` catches NaN (inside a `CASE`, §7.9), the bounds catch ±∞ and overflow, and `EXP(100000)` raises `<MAXNUMBER>` ("numeric overflow"). The bounds keep Spark's truncation: `2147483647.9` still casts to `2147483647`.
- **`ISNUMERIC(TRIM(s))`.** Spark trims before casting; `ISNUMERIC(' 1')` is 0 without the trim. It rejects `''`, `'abc'`, `'1-'`, `'0x1F'` and full-width digits such as `'１'` **(probe 2026-11-18c)**.
- **The integer pattern** rejects what `ISNUMERIC` accepts but an integer cast must not: `'1.5'`, `'1e3'`, `'1.'`, `'.5'`. It accepts an optional sign and leading zeros (`'+3'` → 3, `'007'` → 7). `%PATTERN` alone would accept `'１'`, so both tests are needed. The pattern is emitted as SQL text, not a parameter: it is fixed compiler text, never user input (§9.5).

Verified on 18 inputs **(probe 2026-11-18c)**: the combined rule accepts exactly the integer-shaped strings.

- **Text to date and timestamp** (draft 0.7, probe 2026-11-20, §12.5). IRIS's `CAST(s AS DATE)` returns `1840-12-31` (its day zero) for any text it can't read, including `'abc'`, `''`, `'2026-02-30'` and `' 2026-01-05 '`, without an error; `TO_DATE` is lenient (trailing text, `'2026'` as a day number). The shape test admits exactly Spark's forms, the text is normalized to `yyyy-mm-dd` (`'2026'` → `'2026-01-01'`), and `TO_DATE`/`TO_TIMESTAMP` reject impossible dates and times with an error that `map_error` reports as `MborobakCastError`. Release 0.1 used a plain `CAST` (matrix CAST-18). Verified on 17 inputs against Spark.

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

### 7.10 Scalar functions (release 0.2)

Each `Func` has a registry entry (`src/mborobak/scalar.py`) with three parts: a typer (Spark's result type, checked against PySpark 4.3), a namer (PySpark's default column name, e.g. `CEIL(d)`, `ln(d)`, `POWER(i, 2)`, `round(n, 0)`), and a renderer. Arguments of the wrong kind raise `AnalysisError` at the call, as other type errors do (`upper()` of an `int` needs an explicit cast). A renderer renders an argument again at each use, building the text left to right, so the parameters of a repeated argument stay in text order (§9.5). Integer constants that shape the SQL (a scale, a position, a pad length) are written inline; string and numeric values are parameters.

The rules below come from the 2026-11-19 probe (§12.3). Each is checked against PySpark by the compatibility matrix rows FUN-01 to FUN-21.

| Function | IRIS SQL | Why |
|----------|----------|-----|
| `coalesce`, `ifnull`, `nvl` | `COALESCE(…)`, every argument cast to the wider type | IRIS rejects mixed types; its 2-argument `IFNULL(a, b)` returns NULL when `a` is not NULL |
| `nvl2` | `CASE WHEN a IS NOT NULL THEN b ELSE c END` | |
| `nullif` | `NULLIF(a, b)` with `%EXACT` on strings (§7.1); for doubles a `CASE` where NaN equals NaN | `NULLIF` compares with the column's collation |
| `greatest`, `least` | `GREATEST(COALESCE(a, b, …), COALESCE(b, a, …), …)`; strings by `TextOrder` | IRIS returns NULL if any argument is NULL; Spark skips NULLs. The first non-NULL of the arguments is one of them, so the result is unchanged |
| `isnan` | `CASE WHEN x <> x THEN 1 ELSE 0 END = 1`; `1 = 0` for non-doubles | `x <> x` finds NaN only inside `CASE` (§7.9) |
| `isnull`, `isnotnull` | `IsNull` node | |
| `length`, `char_length` | `CHAR_LENGTH(x)` | `LENGTH` ignores trailing spaces |
| `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `initcap`, `reverse` | Same-named function | `TRIM` removes spaces only, as Spark |
| `substring`, `substr`, `Column.substr` | `SUBSTRING(s, pos, len)` for `pos ≥ 1`; position 0 as 1; a negative position from `CHAR_LENGTH(s)`, clipped as Spark's `substringSQL`; `''` for `len ≤ 0`; no length past IRIS's longest string | IRIS returns `''` for negative positions and for lengths past its string limit |
| `concat` | `a \|\| b \|\| …`; integers, booleans, dates and timestamps cast to text | Double and decimal arguments raise `UnsupportedOperationError`: their text differs (matrix CAST-12, CAST-13) |
| `concat_ws` | `SUBSTRING(COALESCE(sep \|\| a, '') \|\| COALESCE(sep \|\| b, '') …, len(sep) + 1)` | Spark skips NULLs and never returns NULL |
| `lpad`, `rpad` | `LPAD`/`RPAD`; `SUBSTRING(s, 1, n)` for an empty pad; `''` for `n ≤ 0` | IRIS pads with NUL characters for an empty pad |
| `instr`, `locate` | `INSTR(s, sub[, pos])`; an empty `sub` gives 1; `pos < 1` gives 0 | `INSTR('abc', '')` is 0 in IRIS. `INSTR` and `REPLACE` are case-sensitive on any collation |
| `replace` | `REPLACE`; an empty search returns the source | `REPLACE('', '', 'x')` is `'x'` in IRIS |
| `repeat` | `REPEAT`; `''` for a count ≤ 0 | |
| `abs` | `ABS` | |
| `round` on decimals and integers | `CAST(ROUND(x, s) AS <Spark's type>)` | Half up in both |
| `round` on doubles | `CASE` returning `x` for NULL, NaN, ±∞ and `\|x\| ≥ 2⁵³`; `0.0` below `1E-100`; else `CAST(ROUND(ROUND(x, 15 - FLOOR(LOG10(ABS(x)))), s) AS DOUBLE)` | Spark rounds the shortest decimal form (`0.285` → 0.29); IRIS rounds the 20-digit expansion (0.28) and fails on NaN, ±∞ and beyond `1E145`. The inner round recovers the shortest form for every double with at most 15 significant digits |
| `bround` | Half even: `CASE WHEN ABS(v·10ˢ − TRUNCATE(v·10ˢ, 0)) = 0.5 AND MOD(TRUNCATE(v·10ˢ, 0), 2) = 0 THEN TRUNCATE(v, s) ELSE ROUND(v, s) END`, with `v` as above for doubles | IRIS has no half-even rounding |
| `ceil`, `ceiling`, `floor` | Decimals: `CAST(CEILING(x) AS NUMERIC(p-s+1,0))`; integers: `CAST(x AS BIGINT)`; doubles: NaN → 0, a result beyond `bigint` reaches the `EXP(100000)` trigger of §7.7, else `CAST(CEILING(x) AS BIGINT)` | Spark (ANSI) fails on ±∞ and overflow, returns 0 for NaN |
| `pow`, `power` | Doubles; `+∞` for `0^negative`, NaN for a negative base and a fractional exponent, else `POWER` | `POWER` fails on both |
| `sqrt` | NaN for `x < 0`, else `SQRT` | `SQRT(-1)` fails |
| `exp` | `+∞` above `709.782712893384`, else `EXP` | Overflow fails |
| `ln`, `log`, `log10`, `log2`, `log1p`, `log(b, x)` | NaN passed through first; NULL for `x ≤ 0` (`x ≤ -1` for `log1p`); `LOG(x) / LOG(2)` for `log2`, as Spark computes it; `log1p` by its series below `1E-4` | `LOG(0)` fails; there is no `LN` or `LOG2`; IRIS evaluates `NaN <= 0` as true; `LOG(1 + x)` loses tiny `x` |
| `signum`, `sign` | `CAST(SIGN(x) AS DOUBLE)`, NaN passed through | `SIGN` returns an integer and NULL for NaN |
| `pmod` | `CASE WHEN MOD(a, n) < 0 THEN MOD(MOD(a, n) + n, n) ELSE MOD(a, n) END`, typed as `%` | Spark's definition; a zero divisor fails as `%` does |

NaN and ±∞ results are parameters (§9.5). Function results are strings or numbers like any expression, so §7.1 applies to them in comparisons, keys and sorts.

**`fillna` and `dropna`** compile to a `Project` and a `Filter` of ordinary expressions: `COALESCE(x, CAST(v AS type))`, or for doubles `CASE WHEN x IS NULL OR isnan(x) THEN v ELSE x END`; and conditions on `IS NOT NULL` and `NOT isnan(x)`, combined by `how` or counted against `thresh`. Columns that can't be NULL are left out (API contract §4.1).

### 7.11 Window functions (release 0.2)

**Where windows may appear.** In `select` and `withColumn` only. In `filter`, `orderBy`, `groupBy`, `agg` and join conditions they raise `AnalysisError` at the call, as Spark does; the user adds the column first and then filters or sorts on it. Spark's other analysis rules are checked at the call too (`src/mborobak/windows.py`): ranking functions and `lag`/`lead` need an ordered window, ranking functions take no frame but `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, `lag`/`lead` take none, distinct aggregates can't be windowed, a frame's start can't follow its end.

**Computed in a subquery.** Each `WindowExpr` of a `Project` becomes a hidden column `mborobak_wN` of the block below, which is then wrapped; the projection reads the column. Two IRIS behaviors make this necessary (probe 2026-11-20, §12.4): a window function inside a `CASE` condition gives wrong results (and a select renders boolean columns as `CASE`, §7.8), and windows run before `TOP`. So a block with `TOP` or `DISTINCT` is wrapped before the windows are added, and a filter on a window column is a plain `WHERE` on the subquery's column. Windows over a grouped block stay in it: `RANK() OVER (ORDER BY SUM("x"))` is valid next to `GROUP BY`.

**`OVER` clause.**

| Part | IRIS SQL | Why |
|------|----------|-----|
| `partitionBy` | `PARTITION BY %EXACT(g)` for strings (§7.1) | Partitions group like `GROUP BY` |
| `orderBy` | The statement's sort terms: `%EXACT(' ' \|\| s)` for strings, the bucket key for doubles (§7.9), a `CASE` flag for non-default NULL placement (§7.4) | IRIS rejects `NULLS FIRST/LAST`; its double and string orders differ from Spark's |
| No frame, ordered | Nothing (IRIS's default frame is Spark's RANGE … CURRENT ROW: ties share a running value) | Explicit `RANGE` frames are rejected |
| No frame, unordered | Nothing (whole partition) | |
| `rowsBetween(a, b)` | `ROWS BETWEEN n PRECEDING/CURRENT ROW/n FOLLOWING/UNBOUNDED …` with constants | IRIS takes no parameters in a frame |
| `rangeBetween(unboundedPreceding, currentRow)` | Nothing (the default) | |
| `rangeBetween(unboundedPreceding, unboundedFollowing)` | `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | Same rows |
| `rangeBetween(currentRow, unboundedFollowing)` | The default frame over the reversed order (each key's direction and NULL placement flipped); `first` and `last` swap | Same rows, peers included |
| `rangeBetween` with value offsets, or `(currentRow, currentRow)` | `UnsupportedOperationError` | No `RANGE` frames |
| `rowsBetween` without `orderBy` (other than the whole partition) | `UnsupportedOperationError` | Rows of the frame would be arbitrary |

**Functions.** `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `PERCENT_RANK()`, `CUME_DIST()`, `NTILE(n)` (constant) with no frame. `lag(x, n, d)` with `n > 0` is `LAG(x, n, d)`, with `n < 0` `LEAD(x, -n, d)` (IRIS fails on a negative offset), with `n = 0` the value itself; the default is cast to the column's type when it differs. `first`/`last` are `FIRST_VALUE`/`LAST_VALUE`; `ignorenulls=True` raises `NotImplementedError` (no `IGNORE NULLS`). Aggregates over a window keep their typing (§3); `min`/`max` of strings render as `SUBSTRING(MIN(%EXACT(' ' || x)) OVER (…), 2)`.

**Default names** follow Spark: the function's display name (`row_number()`, `RANK()`, `lag(x, 1, NULL)`, `count(1)`), then `OVER (PARTITION BY … ORDER BY k ASC NULLS FIRST, … <frame>)`, where the frame is Spark's effective one: `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` for ranking functions (`RANGE` for `cume_dist`), `ROWS BETWEEN n FOLLOWING AND n FOLLOWING` for `lag`/`lead` (negative for `lag`), and for aggregates the given frame, or `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when ordered, `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` when not. Offsets are written `n FOLLOWING`, negative ones included, as Spark writes them.

**`dropDuplicates(subset)`** is a `Project` with `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY keys)` as a hidden column, a `Filter` on it being 1, and a `Project` back to the original columns. Without a subset it is `distinct()`. Which row of a duplicate set is kept is not defined, as in Spark.

Known differences that windows inherit: `max` over a window ignores NaN (matrix FP-06), a `-1E19` partition key merges with NULL (FP-10), and summing `+∞` and `-∞` in one frame fails (FP-12).

### 7.12 Date and time functions (release 0.2)

Date functions take dates, timestamps or date text; text goes through the §7.7 cast (as Spark casts it), timestamps are cast to `DATE` where Spark takes a date, and dates to `TIMESTAMP` where it takes a timestamp. They live in `src/mborobak/dates.py`, in the §7.10 registry.

| Function | IRIS SQL | Why |
|----------|----------|-----|
| `year`, `month`, `dayofmonth`, `day`, `dayofweek`, `dayofyear`, `quarter` | Same-named function | `DAYOFWEEK` counts from Sunday = 1, as Spark |
| `hour`, `minute`, `second` | `HOUR`, `MINUTE`, `DATEPART('ss', …)` of a timestamp | `SECOND()` rounds the fraction; `HOUR(date)` is wrong |
| `weekofyear` | `FLOOR((DAYOFYEAR(DATEADD('dd', 3 - MOD(DAYOFWEEK(d) + 5, 7), d)) - 1) / 7) + 1` | ISO 8601: the Thursday of the week decides the year; `WEEK()` is not ISO |
| `date_add`, `date_sub`, `add_months` | `CAST(DATEADD('dd' \| 'mm', n, d) AS DATE)`; `0 - (n)` for `date_sub` | `DATEADD` returns a timestamp; its month arithmetic clamps as Spark's does |
| `datediff(end, start)` | `DATEDIFF('dd', start, end)` on dates | IRIS's argument order is the reverse |
| `months_between(a, b, roundOff)` | `CASE` on day of month and `LAST_DAY` for whole months, else months + (day difference × 86400 + seconds of day, with microseconds) / 2678400, then `FLOOR(x × 10⁸ + 0.5) / 10⁸` | Spark's `monthsBetween`, with `Math.round` |
| `last_day` | `CASE WHEN d IS NULL THEN NULL ELSE LAST_DAY(d) END` | `LAST_DAY(NULL)` is a fatal error |
| `trunc(d, unit)` | First day of the year, quarter, month or Monday week by `DATEADD` from `DAYOFYEAR`, `MONTH`, `DAYOFMONTH`, `DAYOFWEEK`; NULL for `'day'` and unknown units, as Spark | No `TRUNC` for dates |
| `date_trunc(unit, ts)` | The same for year … week, cast to `TIMESTAMP`; `day`, `hour`, `minute`, `second`, `millisecond` by `DATEADD` from the day's start; unknown units give NULL | No `DATE_TRUNC` |
| `date_format(x, pattern)` | The Java pattern tokenized; each field rendered from date parts (`LPAD(MONTH(t), 2, '0')`, `SUBSTRING(MONTHNAME(t), 1, 3)`, `DAYNAME`, 12-hour clock by `MOD`, fractions from `DATEPART('mcs', …)`), literal text as parameters, joined by `\|\|`; NULL for NULL | `TO_CHAR` fails on timestamps |
| `to_date(s)`, `to_timestamp(s)` | The §7.7 cast | |
| `to_date(s, p)`, `to_timestamp(s, p)` | `TO_TIMESTAMP(CASE WHEN s IS NULL THEN NULL WHEN s %PATTERN '<shape of p>' THEN s ELSE <trigger> END, '<p in Oracle style>')`, cast to `DATE` for `to_date` | Spark parses strictly (`MM` is two digits, no extra text); IRIS's `TO_TIMESTAMP` is lenient, so the shape is checked first. Letters in quoted text (`'T'`) are blanked in both the text and the format |
| `current_date`, `current_timestamp`, `now` | `CURRENT_DATE`, `CURRENT_TIMESTAMP(6)` | `CURRENT_TIMESTAMP` alone has no fraction |

Pattern letters for `date_format`: `y` (`yy` gives two digits), `M` to `MMMM`, `d`, `D`, `H`, `h`, `m`, `s`, `S` (up to 9), `a`, `E` to `EEEE`, `Q`; others raise `UnsupportedOperationError` at the call (Spark itself rejects the week-based ones). For parsing: `yyyy`, `M`, `MM`, `MMM`, `MMMM`, `d`, `dd`, `H`, `HH`, `h`, `hh`, `m`, `mm`, `s`, `ss`, `S…`, `a`; `yy` is refused because IRIS reads two-digit years in the 1900s. The shape and format are emitted as SQL text built from the user's pattern, with quotes doubled; they are constants of the query, so the statement cache keys on them.

**Types.** Spark's `to_timestamp`, `date_trunc`, `current_timestamp` and `now` return `timestamp`, Spark's type with a time zone; Mborobak returns `timestamp_ntz`, like every Mborobak timestamp (scope §6.8, matrix TIME-05). The values are the same when Spark's session time zone is the one the data was written in.

### 7.13 SQL expression strings (release 0.2)

`F.expr`, `selectExpr` and `filter`/`where` with a string parse Spark SQL expressions into the nodes of §4 (`src/mborobak/sqlexpr.py`); nothing downstream knows the expression came from text. The grammar follows Spark's `SqlBaseParser` precedence: `OR` < `AND` < `NOT` < predicates (`BETWEEN`, `IN`, `LIKE`, `IS NULL`) < comparisons < `+ - ||` < `* / %` < unary `- +` < primaries (literals, columns, `CASE`, `CAST`, function calls with `OVER`). Function calls go through `mborobak.sql.functions` by name, so a SQL call and its Column-API call compile alike; constant arguments that PySpark takes as Python values (`ntile(n)`, `lag`'s offset and default) must be literals.

Literal typing is Spark SQL's: integers are `int`, then `bigint`, then `decimal`; a decimal point makes a `decimal` (`1.5` is `decimal(2,1)`); an exponent or `D` a `double`; `L` a `bigint`; `BD` a `decimal`. String literals take backslash escapes, and adjacent literals join (`'a' 'b'`).

Where Spark names a SQL expression differently from its Column-API twin, the parser keeps Spark's name with an internal alias that only sets the default column name: `x BETWEEN a AND b` is `between(x, a, b)` (compiled as `x >= a AND x <= b`, so strings compare by code point), `if(c, a, b)` is `(IF(c, a, b))` (a `CASE`), one-argument `log(x)` is `LOG(E(), x)`, `pow` keeps `pow`, `+x` is `(+ x)`, and a window frame written `1 PRECEDING` keeps those words. `a != b` and `a <> b` are `NOT (a = b)`, as in Spark.

`RLIKE` and `REGEXP` parse to `rlike` (§7.16), and the `regexp_*` functions are called by name (`regexp_extract(s, p)` with Spark SQL's default group 1). Not supported, each with `UnsupportedOperationError`: `split` and `regexp_extract_all` (arrays), `ILIKE`, `DIV`, bitwise operators, subqueries, `INTERVAL`, `sum(DISTINCT …)`, smallint/tinyint/float. Malformed text raises `ParseError`, an `AnalysisError`, at the call, with the position.

### 7.14 Comparisons with NaN

Spark orders NaN above every double, `+∞` included, and NaN equals NaN: `NaN > 0`, `NaN >= NaN` and `NaN = NaN` are true, `NaN < x` is false for every `x`. IRIS's answer depends on the plan (probe 2026-11-20, §12.7):

- between two columns of a row, NaN sorts **below** every value (`NaN > 0` false, `NaN <= 0` true) and `NaN = NaN` is false;
- across a join, `NaN = NaN` is true;
- against a NaN parameter, `a <> ?` and `a >= ?` are true for a NaN `a`, `a = ?` false;
- `a IN (?nan, …)` matches a NaN `a` in `WHERE`, not in `CASE`.

Only `x <> x` inside `CASE` finds NaN reliably (it is never true in a bare `WHERE`, matrix FUN-04). So `Renderer.pred` decides every case where a side may be NaN with that test, and lets IRIS compare only numbers. A side **may be NaN** when its type is `double` and it is not a literal; a NaN literal **is** NaN; integers, decimals and finite literals never are; a NULL literal leaves the comparison as it was (the result is NULL).

| Sides | Rendering of `a op b` |
|-------|----------------------|
| Neither may be NaN | `a op b`, unchanged |
| `a` may be NaN, `b` a number literal (the common filter `d > 0`) | Where Spark's result for NaN is true (`>`, `>=`, `<>`): `(a op b OR CASE WHEN a <> a THEN 1 ELSE 0 END = 1)`; where it is false (`<`, `<=`, `=`): `(a op b AND CASE WHEN a <> a THEN 0 ELSE 1 END = 1)`. The plain comparison stays a conjunct, so an index can still serve `<`, `<=` and `=`. Mirrored when the literal is on the left |
| `a = b`, `a <> b`, both may be NaN (join keys) | `(a = b OR CASE WHEN a <> a AND b <> b THEN 1 ELSE 0 END = 1)`, negated for `<>`; the `=` stays usable as a join condition |
| Otherwise (`<` … `>=` between two doubles; a NaN literal) | `(CASE WHEN a IS NULL OR b IS NULL THEN NULL WHEN a <> a THEN <both NaN / a NaN> WHEN b <> b THEN <b NaN> WHEN a op b THEN 1 ELSE 0 END = 1)`, with Spark's constant result in each NaN branch |
| `a <=> b` with a side that may be NaN | The same `CASE`, with the NULL branches of null-safe equality |
| `x IN (…)`, `x` may be NaN, all values literals | `(CASE WHEN x IS NULL THEN NULL WHEN x <> x THEN <1 if a value is NaN> WHEN x IN (numbers) THEN 1 ELSE 0 END = 1)` (NULL instead of 0 when the list holds NULL, as Spark's three-valued `IN`); with non-literal values, an `OR` of equalities. NaN values are dropped from a list whose `x` can't be NaN |

Every form is a predicate with NULL where Spark's result is NULL, so `NOT`, `CASE WHEN p THEN 1 WHEN NOT p THEN 0 END` (§7.8) and `between` (two comparisons) work unchanged. The operands are rendered once per use, so their parameters repeat in order. `explain()` lists the translation once. Release 0.2.0 and earlier passed comparisons through: `filter(d > 0)` dropped NaN rows, and `d < e` was wrong whenever one side was NaN (matrix FP-13).

### 7.15 Semi and anti joins

`join(other, cond, "left_semi")` keeps the left rows for which some right row makes `cond` true; `"left_anti"` keeps the others, rows whose condition is NULL included. Both output the left columns only (`P.Join.output`). They compile to a correlated subquery in `WHERE`, not to a `JOIN`, so a left row never repeats however many right rows match:

```sql
SELECT t1."id", t1."k", t1."s"
FROM "SQLUser"."A" AS t1
WHERE [NOT] EXISTS (SELECT 1 FROM "SQLUser"."B" AS t2 WHERE t1."k" = t2."k")
```

Each side is a `FromItem` as in other joins (the table itself, or a subquery when filtered or aggregated); the condition is translated like a join condition (collation §7.1, NaN §7.14) and rendered after the right side, so parameters stay in text order. Without a condition, the subquery has no `WHERE`. The block has a `WHERE` from then on, so a later join or `limit` wraps it as a subquery. A join on names (`on="k"`) adds a projection with the keys first, as Spark orders them.

IRIS evaluates the condition correctly, NaN and NULL keys included (probe 2026-11-20), with one exception from the `-1E19` family (§7.9, matrix FP-10): when the condition on a `DOUBLE` key is more than a bare `=` (the NaN form of §7.14 is), a NULL outer key matches a right row whose value is `-1E19`, even with an explicit `IS NULL` guard. Values other than `-1E19` are not affected.

`unionByName` needs no compiler support: the DataFrame layer reorders the right side by name, adds `CAST(NULL AS type)` columns for `allowMissingColumns`, and builds a `union` (§9.1).

### 7.16 Regular expressions (release 0.3)

IRIS SQL has no regular expressions: `$MATCH` and `$LOCATE` are rejected in SQL, `%MATCHES` is a wildcard syntax (`*`, `?`, `[0-9]`; `.` is a literal dot), and `%PATTERN` uses IRIS's pattern codes. ObjectScript has ICU's regular expressions, and `CREATE FUNCTION … LANGUAGE OBJECTSCRIPT { … }` makes them callable from SQL with plain DDL, without a class file (probe 2026-11-20, §12.8). So Mborobak keeps six small functions in the schema `Mborobak` (`src/mborobak/irisfuncs.py`):

| Spark | IRIS function | Body |
|-------|---------------|------|
| `rlike`, `regexp`, `regexp_like`, `RLIKE`, `REGEXP` | `RegexpLike1(s, p)` | `$LOCATE(s, p) > 0`, as fast as `LIKE` |
| `regexp_replace(s, p, r)` | `RegexpReplace1(s, p, r)` | `%Regex.Matcher.ReplaceAll(r)` |
| `regexp_extract(s, p, i)` | `RegexpExtract1(s, p, i)` | `Locate()`, then `Group(i)`; `ztrap "MBGROUP"` for a group beyond `GroupCount` |
| `regexp_count`, `regexp_substr`, `regexp_instr` | `RegexpCount1`, `RegexpSubstr1`, `RegexpInstr1` | `Locate()` in a loop; `Group(0)`; `Start` |

The bodies follow Spark's results:

- **NULL and `''`.** A SQL NULL arrives as `""` and an empty string as `$c(0)`. A NULL argument returns `""` (NULL). Empty results return `$c(0)`, so `regexp_extract` without a match is `''` as in Spark, while `regexp_substr` without a match returns NULL.
- **Empty patterns.** ICU rejects `''`, which Java matches everywhere; it becomes `(?:)`.
- **Compiled patterns** are cached per connection in the process-private `%mborobakRx`, keyed by the pattern (by a hash beyond 200 characters), and cleared after 200 patterns. Compiling a matcher for every row made a filter over 100,000 rows take 0.9 s; with the cache it takes 0.06 s.
- **Each call is wrapped:** `CASE WHEN 1 = 0 THEN NULL ELSE "Mborobak"."RegexpLike1"(s, ?) END`. IRIS evaluates `NOT (f(…) = 1)` as **true** when a user function returns NULL, and the same for `>` and string comparisons, so `~rlike` kept NULL rows. Inside a `CASE`, the NULL is a NULL (probe 2026-11-20). `rlike` is a predicate: `<call> = 1`, and a boolean value through §7.8.

**Installation on first use.** The compiler records the functions a statement calls (`SqlStatement.functions`). Before running it (in `_execute`, and in a write before its transaction, §9.7), the session looks up the missing ones in `INFORMATION_SCHEMA.ROUTINES` and creates them; a session does this once per function. When creation fails (no `CREATE FUNCTION` permission), the error names the function and suggests `session.installFunctions()`, which an administrator runs once. Function names carry a version (`…1`), so a later Mborobak that changes a body creates `…2` instead of changing what an older Mborobak calls.

**Errors.** IRIS reports a failed function as SQLCODE -149 with the ObjectScript error: `<REGULAR EXPRESSION>` becomes "invalid regular expression", `<ZMBGROUP>` "the group index is beyond the pattern's groups", both `MborobakExecutionError` at the action, where Spark also fails at run time.

**Dialect.** ICU's syntax is Java's for classes, quantifiers, groups, anchors, lookaround, flags such as `(?i)`, and `$1` and `\$` in replacements. Its `\w`, `\d`, `\b` and `\p{Alpha}` are Unicode-aware where Java's are ASCII only: `regexp_replace('é1', '\\w', '_')` gives `'__'` here and `'é_'` in Spark (matrix REGEX-07).

### 7.17 Spark SQL statements (release 0.3)

With `mborobak.sql.dialect = "spark"`, `session.sql(query)` parses a Spark SQL `SELECT` (`src/mborobak/sparksql.py`) and builds it from DataFrame operations: no IRIS SQL is written for it, so every rule of this document applies as it does to the DataFrame API, and the statement becomes one IRIS query. The parser extends the expression parser of §7.13 with the statement grammar of API contract §3.5, `?` and `:name` markers (literal values, as Spark binds them) and `t.*`.

| Clause | DataFrame operation |
|--------|---------------------|
| `FROM t [alias]`, `FROM (query) alias`, a CTE name | `session.table(t)`, the query's DataFrame, the CTE's; `.alias(alias)` (a CTE and a table without alias are qualified by their name) |
| `JOIN … ON c`, `USING (k)`, `,`, `CROSS JOIN`, `LEFT SEMI`, `LEFT ANTI` | `join(other, c, how)`, `join(other, ["k"], how)`, `crossJoin`; semi and anti per §7.15 |
| `WHERE c` | `filter(c)` |
| select list without aggregates | `select(items)`; `*` and `t.*` expand to the columns whose qualifier is `t` |
| `ORDER BY` of such a select | `orderBy` **before** `select`, after replacing ordinals and select aliases by their expressions, so it may use columns the select drops (the order survives the projection, matrix ORD-06) |
| `DISTINCT` | `select(...).distinct()`, then `orderBy` over output columns |
| `GROUP BY`, `HAVING`, aggregates | see below |
| `UNION ALL`, `UNION [DISTINCT]` | `union`, plus `distinct()`; `ORDER BY` over output names and ordinals |
| `LIMIT n` | `limit(n)` |

**Aggregation.** The grouping keys are the `GROUP BY` expressions, with ordinals and select aliases replaced by their expressions (an alias only when no input column has that name, as Spark's `groupByAliases`), or for `GROUP BY ALL` the select items without aggregates. Each distinct aggregate call in the select list, `HAVING` and `ORDER BY` is computed once: `groupBy(key AS __mborobak_g0, …).agg(agg AS __mborobak_a0, …)`. Then the select list, `HAVING` and `ORDER BY` are rewritten top-down, replacing every key expression and aggregate call (compared with names case-insensitive) by its hidden column, and become `filter`, `orderBy` and `select(... AS <Spark's name>)`. An expression left that is neither (a column not grouped) fails to resolve, reported as an `AnalysisError`. Without aggregates, `GROUP BY k` is `select(k).distinct()`. The compiler merges all of this into one `SELECT … GROUP BY … HAVING … ORDER BY` (§9.2).

**`{name}` placeholders** (PySpark's `spark.sql(query, df=df)`) are filled before parsing: a DataFrame becomes a CTE-like relation named `__mborobak_<name>`, other values their text.

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
- **Decimal and finite double literals are cast:** `CAST(? AS NUMERIC(p,s))`, `CAST(? AS DOUBLE)` (draft 0.8). IRIS types a parameter from the column beside it, and next to an integer column it truncates or rounds a fractional value: `i + ?` with 1.5 gives `i + 2`, `i - ?` with 0.25 gives `i`, `COALESCE(i, ?)` with 1.5 gives 2 (probe 2026-11-20, matrix NUM-14). Multiplication and comparisons were not affected. Release 0.1 sent bare parameters. Integer, string, date and NaN or infinity parameters stay bare.
- The compiler counts parameters. Above **900**, it raises `PlanTooLargeError` with the count and the operation that contributed most (usually an `isin()` list). IRIS fails at about 999 (study §2.3); 900 leaves room for literals that IRIS itself extracts.
- **Fixed compiler text is inline, not a parameter:** the `%PATTERN` string (§7.7), the `DOUBLE` bucket constants (§7.9), the `CASE` results of `NullOrderKey` (§7.4) and boolean projections (§7.8). None of it comes from users, and it stays out of the 900 count. (A `%PATTERN ?` parameter also works, probe 2026-11-18c, but the text form keeps `explain()` readable.)
- `lit(None)` renders as `NULL` text (wrapped in a `CAST` when the type matters), not as a parameter. A comparison with `None` (`col("x") == None`) follows SQL rules and yields NULL, as in Spark; users test for nulls with `isNull()`.

### 9.6 Fingerprint and cache

`SqlStatement.fingerprint` is a hash of the optimized plan with literal values removed. The session caches `fingerprint → SQL text`, so running the same pipeline shape again with different values skips analysis and generation (scope §7). Parameter values are bound per execution.

### 9.7 Writing tables (release 0.2.x)

`df.write.saveAsTable(name, mode)` and `df.write.insertInto(name, overwrite)` (API contract §4.7) compile the DataFrame to one `SELECT` and wrap it in DDL and `INSERT … SELECT`, so the rows never leave IRIS (`src/mborobak/writer.py`). Every write is one transaction: `START TRANSACTION`, the statements, `COMMIT`, and `ROLLBACK` on any error. IRIS rolls back `CREATE TABLE`, `DROP TABLE` and `ALTER TABLE … RENAME` too (probe 2026-11-20, §12.6), so a failed write leaves the database as it was.

| Mode | Table missing | Table exists |
|------|---------------|--------------|
| `errorifexists` (default, also `error`) | Create | `AnalysisError`, nothing runs |
| `ignore` | Create | Nothing runs |
| `append` | Create | `INSERT … SELECT`, columns matched by name |
| `overwrite` | Create | Staging table, then swap (below) |
| `insertInto(name)` | `TableNotFoundError` | `INSERT … SELECT`, columns matched by position; `overwrite=True` runs `DELETE FROM` first |

**Create:** `CREATE TABLE "schema"."name" (…)` with one column per output attribute, then `INSERT INTO "schema"."name" (cols) <SELECT>`. Column types read back as the DataFrame's types:

| Type | IRIS column |
|------|-------------|
| `int`, `bigint`, `double` | `INTEGER`, `BIGINT`, `DOUBLE` |
| `decimal(p,s)` | `NUMERIC(p,s)`; IRIS keeps at most 19 digits before the point, so `p − s > 19` reads back as `decimal(19+s, s)` (matrix WRITE-07) |
| `string` | `VARCHAR(n) COLLATE EXACT`, `n` from `mborobak.write.stringLength` (default 3641144, IRIS's maximum; `VARCHAR` without a length holds one character). `EXACT` makes later comparisons, grouping and sorting follow Spark without the `%EXACT` rewrite (§7.1). With `stringSemantics="iris"` the collation clause is left out |
| `boolean`, `date`, `timestamp_ntz` | `BIT`, `DATE`, `TIMESTAMP` (keeps microseconds; `TIMESTAMP(6)` is a fatal error) |

Columns are nullable, as in tables Spark creates. Before anything runs, the writer rejects names IRIS can't hold (`.`, `,` or `^`, which default names such as `date_add(d, 1)` contain; empty names) and names that differ only in case (IRIS rejects them with -306): `AnalysisError`, asking for `alias()`.

**Overwrite** never drops the old table before the new rows exist: `CREATE TABLE` a staging table `<name>_mborobak_<8 hex digits>` in the same schema, `INSERT` into it, `DROP TABLE` the old table, `ALTER TABLE <staging> RENAME <name>`. The obvious `DROP`, `CREATE`, `INSERT` loses data: when that `INSERT` fails (a division by zero, -400; a NOT NULL violation, -108), `ROLLBACK` brings back the old table's columns but **not its rows**, apparently because the new table reuses the old one's storage (probe 2026-11-20). With the staging table the old rows survive any failure, and the DataFrame may read the table it replaces, as Delta tables allow. IRIS keeps the staging table's class name after the rename (`Schema.Namemborobak1a2b3c4d`); only ObjectScript code sees it.

**Append and `insertInto`** read the target's columns afresh (the catalog entry is refreshed first) and add a projection: each target column takes the matching source column, cast when the types differ. Spark's ANSI store assignment decides what may be cast: numbers into numbers, anything into strings, dates and timestamps into each other; anything else (`string` into `int`, `boolean` into a number) is an `AnalysisError` naming both types. Overflow fails when the row is written: the cast's checks (§7.7), or IRIS's own field validation (-104), which the error mapping reports as "a value does not fit its column". Matching by name is case-insensitive; a column count that differs, a missing name or two source columns with the same name are `AnalysisError`s. Target columns of types Mborobak can't read raise `UnsupportedTypeError`. `insertInto(…, overwrite=True)` on a table the DataFrame reads is an `AnalysisError`, since `DELETE` would empty its own input.

**`INSERT … SELECT` forms** (probe 2026-11-20): `ORDER BY`, `TOP`, `UNION ALL`, windows, `GROUP BY` and parameters work after `INSERT`; `LIMIT` and a parenthesized `SELECT` don't, and the compiler emits neither. A `WITH` clause must come **before** `INSERT INTO` (`INSERT INTO t WITH …` is invalid SQL), so `SqlStatement.cte` keeps it apart and the writer puts it in front. The writer always compiles a `Project` over the plan, so a lone `sql()` source is wrapped like any other and its `WITH` comes out separately. `INSERT INTO t SELECT … FROM t` appends a snapshot (no repeated rows), and a failed `INSERT … SELECT` inserts nothing even outside a transaction.

**Existence** is checked in `INFORMATION_SCHEMA.TABLES`, case-insensitively, as `table()` does; a `VIEW` there is an `AnalysisError` for `append`, `overwrite` and `insertInto`. After a write, the catalog entry of the target is refreshed, so `session.table(name)` sees the new columns. `session.lastExecution` records the action, all statements, the `INSERT`'s parameters and its row count.

### 9.8 Local data: `createDataFrame` (release 0.2.x)

`session.createDataFrame(data, schema)` (API contract §3.6, `src/mborobak/local.py`) is the one call that sends rows to IRIS. It runs at the call, not at an action:

1. **Schema.** A `StructType`, a DDL string (`"a int, b: string NOT NULL"`, parsed with the type names of §7.13), an atomic type (`"int"`: one column named `value`), a list of names, or `None`. Missing types are inferred from the values as PySpark does: `bool` → `boolean`, `int` → `bigint`, `float` → `double`, `str` → `string`, `Decimal` → `decimal(38,18)`, `date` → `date`, `datetime` → `timestamp_ntz` (Spark: `timestamp`, matrix CDF-07); `None` merges with any type, other mixes are a `TypeError`. A pandas DataFrame gives its types by dtype (integers → `bigint`, floats → `double`, booleans, datetimes → `timestamp_ntz`) and object or string columns by their values, with `None`, NaN, `pd.NA` and `NaT` as NULL. Dict rows are matched by sorted key, or by name when a schema is given.
2. **Values** are checked and converted as PySpark's `verifySchema` does (matrix CDF-03, CDF-04): no `int` into `double` or `decimal`, no `bool` into `int`, integer ranges, decimals rounded HALF_UP to the scale and checked against the precision, anything into `string` with Python's `str()` (`True` → `'true'`), a `datetime` into `date` keeps its date, an aware `datetime` into `timestamp_ntz` becomes UTC.
3. **Table.** `CREATE GLOBAL TEMPORARY TABLE "MborobakTemp"."DF<16 hex digits>" ("c1" …, "c2" …)` with the column types of §9.7 (strings `COLLATE EXACT`), then one `executemany` `INSERT`. A global temporary table's definition is shared by the namespace, but its rows belong to the connection that inserted them and go when it closes (probe 2026-11-20: another connection counts 0 rows; 20,000 rows insert in about 0.2 s). Columns are named `c1`, `c2`, … so that any Spark column name works (`a.b`, `c,d`, `a` and `A`, CDF-09).
4. **Plan.** A `TableScan` of the temporary table (string columns `EXACT`) under a `Project` that renames `c1`, `c2`, … to the DataFrame's names. From here it is an ordinary table: it joins with IRIS tables, and `df.write` can save it.

`session.stop()` drops the session's temporary tables; so does an `atexit` hook, for scripts that never call `stop()`. Dropping a definition another connection still uses would empty that connection's table, so Mborobak only ever drops its own.

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

### 12.3 Probe 2026-11-19: scalar functions

Script [`probes/scalar_function_probe.py`](../probes/scalar_function_probe.py), raw output [`probes/results-2026-11-19-scalar-functions-iris-2026.2.json`](../probes/results-2026-11-19-scalar-functions-iris-2026.2.json), IRIS 2026.2, a table with a default (SQLUPPER) and an `EXACT` string column. Later checks made while running the differential tests are marked †.

| Check | Result |
|-------|--------|
| `''` in results of `SUBSTRING`, `TRIM`, `REPLACE`, `UPPER`, `CASE`, parameters | Empty string, never NULL |
| `TRIM` of tabs; `UPPER('straße')`, `UPPER('ß')` | Tabs kept; `'STRAßE'`, `'ß'` (no full case mapping) |
| `INSTR`, `REPLACE`, `POSITION` on a SQLUPPER column | Case-sensitive |
| `NULLIF(s, 'HELLO WORLD')` on `'Hello World'` | NULL: compares with the column's collation |
| `GREATEST(1, NULL, 3)`, `LEAST(…)` | NULL; mixed types need explicit casts |
| 2-argument `IFNULL(a, b)` | `b` when `a` is NULL, else NULL |
| `INSTR('abc', '')`, `REPLACE('', '', 'x')`, `REPLACE('abc', '', 'x')` | 0, `'x'` †, `'abc'` |
| `SUBSTRING('Hello', -3, 2)`, `(…, 0, 2)`, `SUBSTRING('abcdef', 3, 2147483647)` | `''`, `'H'`, `''` † |
| `LPAD('abcdef', 3, '*')`, `LPAD('ab', 5, '')`, `LPAD('ab', -1, 'x')` | `'abc'`, NUL characters, NULL |
| `INITCAP('hello-world foo_bar  baz\tqux')` | `'Hello-world Foo_bar  Baz\tQux'` (word starts after a tab) |
| `CONCAT('a', 'b', 'c')`; `LN`, `LOG2` | Syntax error; not found |
| `ROUND(2.5)`, `ROUND(-2.5)`, `ROUND(0.125, 2)` on `NUMERIC` | 3, −3, 0.13 (half up) |
| `ROUND(d, 2)` of `DOUBLE` 0.285; `CAST(0.285 AS VARCHAR)` | 0.28; `.28499999999999997557` |
| `ROUND` of NaN, ±∞, `1E300` | Fatal error -400 |
| `CEILING`, `FLOOR`, `ABS` of NaN, ±∞ | NaN, ±∞ |
| `POWER(-8, 0.5)`, `POWER(0, -1)`, `POWER(10.0, 400)`, `SQRT(-1)`, `EXP(1000)`, `LOG(0)`, `LOG(-1)` | Fatal error -400 for each |
| `LOG(NaN)`, `EXP(NaN)`, `SQRT(NaN)`; `CASE WHEN NaN <= 0` † | NaN; true |
| `SIGN(NaN)`, `SIGN(2.5)` | NULL, 1 (integer) |
| `WHERE d <> d`, `WHERE (d IS NOT NULL AND d <> d)` on NaN † | No rows (as §7.9); `CASE WHEN d <> d THEN 1 ELSE 0 END = 1` finds it |
| `+∞ + −∞`, `SUM`/`AVG` over both † | Fatal error -400 `<ILLEGAL VALUE>` |
| `%EXACT(s) > %EXACT(t)` for `'12345'`/`'5'`, `'10'`/`'9'`; `ORDER BY %EXACT(s)` † | Numeric order, numbers first |
| `%EXACT(' ' \|\| s)` in the same comparisons, sorts, `MIN`/`MAX` † | Code-point order, as Spark |

### 12.4 Probe 2026-11-20: window functions

Script [`probes/window_probe.py`](../probes/window_probe.py), raw output [`probes/results-2026-11-20-windows-iris-2026.2.json`](../probes/results-2026-11-20-windows-iris-2026.2.json), IRIS 2026.2.

| Check | Result |
|-------|--------|
| `SUM(x) OVER (PARTITION BY … ORDER BY k)` with tied `k` | Ties share the running sum: RANGE semantics, as Spark's default |
| `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (explicit), `RANGE BETWEEN 1 PRECEDING AND CURRENT ROW`, `RANGE … UNBOUNDED FOLLOWING` | Invalid SQL statement for all |
| `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`, `… 1 FOLLOWING AND 2 FOLLOWING`, `… CURRENT ROW AND UNBOUNDED FOLLOWING` | Correct |
| `ROWS BETWEEN ? PRECEDING …` | Invalid SQL statement |
| `PARTITION BY g` vs `PARTITION BY %EXACT(g)` | Collation merges `'a'` and `'A'`; `%EXACT` keeps them apart |
| Window `ORDER BY k` / `k DESC` with NULL | NULL first ascending, last descending (Spark's default) |
| `ORDER BY k NULLS LAST` | Invalid SQL statement; a `CASE` flag works |
| `ORDER BY d` on doubles; with the §7.9 bucket key | IRIS's order (`-1E19` with NULL, `-∞` after finite values); Spark's order with the bucket |
| `ORDER BY %EXACT(' ' \|\| g)` | Code-point order |
| `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`, `NTILE(4)`, `NTILE(?)` | As Spark |
| `LAG(x, 2, -1)`, `LEAD(x, 1, 0)`, `LAG(x, ?, ?)`; `LAG(x, -1)` | Correct; fatal error -400 |
| `FIRST_VALUE`, `LAST_VALUE` with the default and `ROWS` frames | As Spark (`LAST_VALUE` over an ordered window is the current row) |
| `FIRST_VALUE(x IGNORE NULLS)`, `… IGNORE NULLS OVER` | Invalid SQL statement |
| `NTH_VALUE(x, 2)` | NULL for every row (not offered) |
| `MIN(%EXACT(' ' \|\| g)) OVER ()` | `' 10'` (leading space kept: stripped with `SUBSTRING`) |
| `COUNT(DISTINCT x) OVER ()` | Works (Spark rejects it; so does Mborobak) |
| `RANK() OVER (ORDER BY SUM(x) DESC)` with `GROUP BY` | Correct |
| Window in `ORDER BY`; in `WHERE` | Works; SQLCODE -367 |
| `SELECT TOP 3 …, ROW_NUMBER() OVER (ORDER BY id DESC)` | Numbers computed before `TOP` (9, 8, 7) |
| `CASE WHEN ROW_NUMBER() OVER (ORDER BY id) = 1 THEN …` | **Never true**: wrong result, no error |
| `x - LAG(x) OVER (…)`, `CAST(ROW_NUMBER() OVER (…) AS DOUBLE) / 2` | Correct |
| Result types | `ROW_NUMBER`, `RANK`, `COUNT`, `NTILE`: `BIGINT`; ratios and `AVG`: `DOUBLE` (Mborobak converts to Spark's types) |

### 12.5 Probe 2026-11-20: dates and times

Script [`probes/date_probe.py`](../probes/date_probe.py), raw output [`probes/results-2026-11-20-dates-iris-2026.2.json`](../probes/results-2026-11-20-dates-iris-2026.2.json), IRIS 2026.2; later checks while building the casts are marked †.

| Check | Result |
|-------|--------|
| `DATEADD('mm', 1, d)` for 2026-01-31, 2024-02-29; `DATEADD('mm', 12, 2024-02-29)` | 2026-02-28, 2024-03-29, 2025-02-28 (clamped, as Spark); type `TIMESTAMP` |
| `DATEADD('dd', ?, d)`, `DATEADD('dd', column, d)` | Work |
| `DATEDIFF('dd', a, b)` on timestamps | Calendar days between the dates, `b − a` |
| `LAST_DAY(d)`, `LAST_DAY(ts)`; `LAST_DAY(NULL)` † | Correct; fatal error -400 |
| `WEEK(2027-01-01)`, `DATEPART('isoweek', …)` | 1 (ISO: 53); invalid DATEPART code |
| `SECOND(23:59:59.999999)`, `DATEPART('ss', …)` † | 60, 59 |
| `HOUR(d)` of a `DATE` | 18 (wrong) |
| `DATEPART('ms' / 'mcs', ts)` | 123, 123456 |
| `CAST(text AS DATE)` for `'abc'`, `''`, `'2026-02-30'`, `'2026-13-01'`, `' 2026-01-05 '`, `'2026/01/05'` † | `1840-12-31` for all, no error |
| `TO_DATE(text, 'YYYY-MM-DD')`: `'2026-1-5'`, `'2026-01-05x'`, `'2026'`; `'2026-02-30'`, `'abc'` † | Accepted, accepted, day 2026 of IRIS's calendar (1846-07-19); fatal errors |
| `TO_TIMESTAMP` with `HH24`, `MI`, `SS`, `FF`, `HH12 … AM`, `MON`, `MONTH`; missing time parts; `'2026-01-05 25:00:00'`; `YY` † | Work, lenient about digit counts; error; `26` → 1926 |
| `TO_CHAR(ts, …)`, `TO_CHAR(d, 'HH24:MI')` | Fatal error -400 |
| `DAYNAME(d)`, `MONTHNAME(d)` | `'Saturday'`, `'January'` |
| `CURRENT_TIMESTAMP`, `CURRENT_TIMESTAMP(6)` | No fraction; microseconds |

### 12.6 Probe 2026-11-20: writing tables

Script [`probes/write_probe.py`](../probes/write_probe.py), raw output [`probes/results-2026-11-20-write-iris-2026.2.json`](../probes/results-2026-11-20-write-iris-2026.2.json), IRIS 2026.2.

| Check | Result |
|-------|--------|
| `VARCHAR` without a length | `MAXLEN` 1: any longer value fails with -104 |
| `VARCHAR(3641144)`; `VARCHAR(3641145)` | Holds 5000-character values; also accepted |
| `COLLATE EXACT`, `COLLATE %EXACT`, `%EXACT` | All three give `COLLATION` `EXACT`: `'apple'` ≠ `'Apple'`, `'  x '` ≠ `'  x'` |
| `TIMESTAMP(6)` | Fatal error -400; plain `TIMESTAMP` keeps microseconds |
| `NUMERIC(p,s)` for `p − s > 19` | Catalog precision `19 + s` (`NUMERIC(38,10)` → 29); values of 20 integer digits fail with -104 |
| Round trip by `INSERT … SELECT` | NaN, ±∞ and `-0.0` in `DOUBLE`; `1.005` into `NUMERIC(10,2)` rounds to 1.01; 1 / 0 from `CASE` into `BIT` |
| Out-of-range values: 2^32 into `INTEGER`, 20 digits into `NUMERIC`, 2 or text into `BIT`/`INTEGER` | -104; text into `DATE`: `<ARGUMENT ERROR>`; `8.7` into `INTEGER`: **8**, silently |
| Column names `(i + 1)`, `count(1)`, `1st`, `with space`, `%pct`, `x"q`, `a;b`, `a=b`, `ç`, 129 characters | Accepted and kept as written |
| Column names with `.` or `,`; empty; `a` and `A` | -1 invalid SQL; -1; -306 |
| `INSERT … SELECT … ORDER BY`, `TOP`, `UNION ALL`, window, `GROUP BY … HAVING`, parameters | Work |
| `INSERT … SELECT … LIMIT 2`; `INSERT INTO t (SELECT …)`; `INSERT INTO t WITH q AS …` | -25; -1; -1. `WITH q AS (…) INSERT INTO t SELECT …` works |
| `INSERT INTO t SELECT … FROM t` with 1000 rows | 2000 rows: a snapshot |
| Failing `INSERT … SELECT` (division by zero, NULL into NOT NULL), no transaction | No rows inserted |
| `ROLLBACK` after `INSERT`, `CREATE TABLE`, `DROP TABLE` with recreate, `ALTER TABLE … RENAME` | All undone |
| `DROP TABLE t`, `CREATE TABLE t`, failing `INSERT INTO t`, `ROLLBACK` | Old columns back, **old rows gone** (also after -108) |
| Same with a staging table: `CREATE s`, failing `INSERT INTO s`, `ROLLBACK`; or `CREATE s`, `INSERT`, `DROP t`, `RENAME s t`, `COMMIT` | Old rows kept; staging table gone. Swap: new columns and rows |
| `DELETE FROM t`, failing `INSERT`, `ROLLBACK` | Rows back |
| `ALTER TABLE s RENAME t` onto an existing `t`; to another schema | -201; -1 |
| `CREATE TABLE` of an existing name in other case; in a new schema; unqualified | -201; creates the schema; `SQLUser` |

### 12.7 Probe 2026-11-20: comparisons with NaN

Script [`probes/nan_comparison_probe.py`](../probes/nan_comparison_probe.py), raw output [`probes/results-2026-11-20-nan-iris-2026.2.json`](../probes/results-2026-11-20-nan-iris-2026.2.json), IRIS 2026.2. Pairs of NaN, ±∞, 1, 0, `-1E19` and `1E300` in two `DOUBLE` columns of one row:

| Check | Result |
|-------|--------|
| `a op b` in `WHERE` and in `CASE`, against Spark | Same answer in both places. Differs from Spark only where a side is NaN: `=` and `<>` for NaN, NaN (false, true); `<`, `<=`, `>`, `>=` for NaN against any number, NaN sorting below everything (6 pairs each). ±∞ and `-1E19` compare correctly |
| `CASE WHEN a <> a THEN 1 ELSE 0 END`; the same `= 1` in `WHERE`; bare `WHERE a <> a` | 1 for NaN only; finds the NaN rows; finds none |
| `x.a = y.b` as a join condition and as a cross-join `WHERE` | NaN matches NaN |
| `a op ?` with a NaN parameter, for `=`, `<>`, `<`, `<=`, `>`, `>=` | NaN `a`: false, true, false, true, false, true; `1.0`: false, true, false, true, false, true |
| `a IN (?nan, ?1.0)` in `WHERE`; `CASE WHEN a IN (?nan)` | Matches NaN and 1.0; 0 for NaN |
| The fixes: `(x.a = y.b OR CASE WHEN x.a <> x.a AND y.b <> y.b …)` as a join condition; `(a > ? OR CASE WHEN a <> a …)` | NaN matches NaN only; NaN and 1.0 for `> 0` |

### 12.8 Probe 2026-11-20: regular expressions

Script [`probes/regex_probe.py`](../probes/regex_probe.py), raw output [`probes/results-2026-11-20-regex-iris-2026.2.json`](../probes/results-2026-11-20-regex-iris-2026.2.json), IRIS 2026.2. Prompted by a Developer Community thread (2017) listing `%MATCHES`, `%PATTERN` and a `$MATCH` class method.

| Check | Result |
|-------|--------|
| `SELECT $MATCH('abc', 'a.c')`, `$LOCATE(…)` | SQLCODE -12: not SQL functions |
| `s %MATCHES 'a*'`, `'a[0-9]c'`, `'a.c'` | Wildcards: `abc`, `a1c`; `a1c`; nothing (`.` is literal) |
| `CREATE FUNCTION … LANGUAGE OBJECTSCRIPT { … }` with `$LOCATE`, `%Regex.Matcher` | Created and callable from SQL; also `LANGUAGE PYTHON` |
| Arguments | NULL arrives as `""`, `''` as `$c(0)`; returning `""` gives NULL |
| `(?i)abc`; replacement `$3/$2/$1`; `x*` replaced in `abc`; `\$1` | Case-insensitive match; `31/01/2026`; `-a-b-c-`; `a$1c`, all as in Java |
| `\w` in `é1`; `\p{Alpha}` on `é` | `__`; a match (Java: `é_`, no match) |
| Empty pattern; `(?:)` | `$LOCATE` finds nothing; matches |
| Invalid pattern `(` | SQLCODE -149, `<REGULAR EXPRESSION>` in the message |
| `WHERE NOT (f(s) = 1)` with `f(NULL)` NULL; the same inside `CASE WHEN 1 = 0 THEN NULL ELSE … END` | NULL row kept; NULL row dropped |
| 100,000 rows: a new `%Regex.Matcher` per row, a cached matcher, `$LOCATE`, `LIKE` | 0.90 s, 0.06 s, 0.06 s, 0.05 s |

## 13. Open questions

| # | Question | Needed for |
|---|----------|------------|
| O1 | ~~Where to read a column's collation~~ **Resolved:** `%Dictionary.CompiledProperty.Parameters` (§6.4) | — |
| O2 | ~~A stable error trigger, and how to tell which guard fired~~ **Resolved:** row-dependent `$LISTGET(s, LENGTH(s) - LENGTH(s) - 5)` for casts (draft 0.3), no guard for division, attribution by `%msg` code (§7.5) | — |
| O3 | ~~The exact integer pattern~~ **Resolved:** `ISNUMERIC(TRIM(s)) = 1 AND TRIM(s) %PATTERN '0.1(1"-",1"+")1.N'` (§7.7) | — |
| O4 | ~~Cost of `%EXACT` at 1M rows (index loss)~~ **Resolved** by the [benchmark](benchmark-report-2026-11-18.md) §3.2: the index is still used, but equality filters on indexed strings read every candidate row (3.3× slower for a count at 1M rows); no measurable cost elsewhere. D10 stands | — |
| O5 | ~~How `-0.0` and NaN group, compare and sort~~ **Resolved** (§12.2); `MAX` ignoring NaN goes to the compatibility matrix | — |
| O6 | ~~Re-check everything on IRIS build 154~~ **Withdrawn:** D1 keeps IRIS 2026.2 (scope 1.9) | — |
| O7 | Grouping and `DISTINCT` on `DOUBLE` merge `-1E19` with NULL (§7.9). Candidate: group on an extra NULL flag and return the value via `MIN(d)`, which is correct; needs a probe and a cost check | §7.9, compatibility matrix |
| O8 | Whether to report the IRIS `-1E19` sort/group behavior to InterSystems | Before submission |
