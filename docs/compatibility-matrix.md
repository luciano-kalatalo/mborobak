# Compatibility Matrix

Scientificloud · Mborobak · Control document 3 of 3 · Draft 0.3 · 2026-11-18

Lists, behavior by behavior, how Mborobak on IRIS compares with the reference, **PySpark 4.3.x with ANSI mode on** (D9), and what Mborobak does about each difference. Implements scope §8.2. The other two control documents are the [compiler specification](compiler-spec.md) (how SQL is generated) and the [API contract](api-contract.md) (the public API).

## 1. How to read this document

### 1.1 Status

| Status | Meaning |
|--------|---------|
| **Same** | IRIS already behaves like PySpark; Mborobak passes it through |
| **Emulated** | IRIS differs; Mborobak's compiler or result conversion makes the result match PySpark. The rule is cited |
| **Known difference** | Results differ in P0, and this document is the place users learn it. Each has a reason and, where planned, a fix |
| **Unsupported** | Mborobak raises an error instead of producing a result that might differ |
| **Open** | Not decided yet; linked to an open question |

### 1.2 Evidence

Every row records where its two sides come from:

| Code | Source |
|------|--------|
| `S§x` | [IRIS behavior study](iris-behavior-study.md), IRIS 2026.2 |
| `P-b`, `P-c` | Follow-up probes 2026-11-18b and 2026-11-18c, [compiler spec](compiler-spec.md) §12 |
| `P-d` | Probe 2026-11-18d, made for this document (§6) |
| `doc` | PySpark / Spark SQL documentation for 4.x. **Not yet run** |
| `diff` | Confirmed by the differential test suite against PySpark 4.3 (§3) |
| `—` | No evidence yet; the row is an expectation |

**Status of this draft (0.2):** the differential suite (§3, [`tests/compat`](../tests/compat/test_matrix_rows.py)) runs the same DataFrame code on Mborobak (IRIS 2026.2) and on PySpark 4.3.0 with ANSI mode on (OpenJDK 21, `local[1]`, session time zone UTC). Rows whose PySpark side is `diff` passed it; rows still marked `doc` have no differential test yet. A row can only claim compatibility once its PySpark side is `diff` (scope §8.2: "Full PySpark compatibility will not be claimed without matching test coverage"). Known differences are tested too: each is a strict expected failure, so a difference that disappears fails the suite until its row is updated.

### 1.3 Row IDs

Each row has an ID (`STR-01`, `NUM-03`, …). Differential tests carry the ID in their name (`test_STR_01_equality_case`), so the test suite and this document can be checked against each other automatically (§3.3).

## 2. The matrix

### 2.1 Strings and collation (D10)

Rows marked † depend on `mborobak.sql.stringSemantics`. With the default `"pyspark"`, the status shown applies. With `"iris"`, every † row is a known difference by design, showing IRIS's result.

| ID | Behavior | PySpark | IRIS default | Mborobak | Status | Evidence |
|----|----------|---------|--------------|----------|--------|----------|
| STR-01 † | `col == 'APPLE'` on mixed-case data | Case-sensitive: 1 row | Case-insensitive: 4 rows | `%EXACT(col) = ?` | Emulated (spec §7.1) | S§2.5, P-b / diff |
| STR-02 † | Trailing spaces in `=` (`'apple '` vs `'apple'`) | Different values | Equal | `%EXACT` | Emulated (§7.1) | S§2.5 / diff |
| STR-03 † | `like`, `isin`, `between` on strings | Case-sensitive | Case-insensitive | `%EXACT` | Emulated (§7.1) | P-b / diff |
| STR-04 † | Join on string keys | Exact match | `'x'` joins `'X'` | `%EXACT` on both sides | Emulated (§7.1) | S§2.5, P-b / diff |
| STR-05 † | `groupBy` a string column | One group per exact value, original values | Groups merged, values uppercased | `GROUP BY %EXACT(col)` and `%EXACT(col)` in output | Emulated (§7.1) | S§2.5, P-b / diff |
| STR-06 † | `distinct()`, distinct `union` on strings | Exact values | Merged, uppercased | `%EXACT` in output | Emulated (§7.1) | S§2.5 / diff |
| STR-07 † | `orderBy` a string column | Binary order (`'B' < 'a'`) | Case-insensitive order | `ORDER BY %EXACT(col)` | Emulated (§7.1) | P-b / diff |
| STR-08 † | `min`, `max`, `countDistinct` on strings | Exact values | Uppercased / merged | `MIN(%EXACT(col))`, `COUNT(DISTINCT %EXACT(col))` | Emulated (§7.1) | S§2.5, P-b / diff |
| STR-09 | Column declared `COLLATE %EXACT` | — | Already exact | No wrapper | Same | P-c |
| STR-10 | Columns with other collations (`SQLSTRING`, `TRUNCATE(n)`) | — | Differ | `%EXACT` | Emulated (§6.4) | P-c |
| STR-11 | Comparisons between literals or parameters | Exact | Exact | Passed through | Same | S§2.5 |
| STR-12 | `''` vs NULL | Distinct | Distinct | Passed through | Same | S§2.6 / diff |
| STR-13 | NULL in string concatenation | NULL | NULL | — (concat is P1) | Same | S§2.6 / doc |
| STR-14 | `like` escape character | `\` by default | No default escape | P0 has no escape support | Known difference: patterns containing `\` differ. Escapes are P1 | — / doc |
| STR-15 | Cost of `%EXACT` | — | May lose index use | — | Open (spec O4): measured in the benchmark | S§2.5 |

### 2.2 NULL

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| NULL-01 | Comparison with NULL | NULL (row filtered out) | Same | Passed through | Same | — / diff |
| NULL-02 | `eqNullSafe` (`<=>`) | NULL `<=>` NULL is true | No `<=>` operator | Rendered with `IS NULL` checks | Emulated | — / diff |
| NULL-03 | Default NULL order | First ascending, last descending | Same | Passed through | Same | S§2.6 / diff |
| NULL-04 | `asc_nulls_last`, `desc_nulls_first` | Supported | No `NULLS FIRST/LAST` syntax | Extra `CASE` sort key | Emulated (spec §7.4) | S§2.6 / diff |
| NULL-05 | NULL join keys | Never match | Same | Passed through | Same | — / diff |
| NULL-06 | `groupBy` a nullable key | One NULL group | Same | Passed through | Same | S§2.6 / diff |
| NULL-07 | Aggregates on empty input | `count` 0, others NULL; grouped: no rows | Same | Passed through | Same | S§2.6 / diff |
| NULL-08 | `isNull` in a `select` | Boolean column | `IS NULL` rejected in `SELECT` list | `CASE WHEN … THEN 1 ELSE 0 END`, converted to `bool` | Emulated (spec §7.8) | S§2.3 / diff |
| NULL-09 | NULL divided by zero | NULL | NULL | Passed through | Same | P-c / diff |

### 2.3 Numbers and types

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| NUM-01 | `int / int` | `DOUBLE` (`7/2` = 3.5) | `Decimal` 3.5 | Dividend cast to `DOUBLE`; result converted | Emulated (spec §3) | S§2.6 / diff |
| NUM-02 | `avg` of integers | `DOUBLE` | `Decimal` | Result converted to schema type | Emulated (spec §3) | S§2.6 / diff |
| NUM-03 | `avg`, `sum` of decimals | Spark's precision/scale rules (`p+10`, `p+4`/`s+4`) | `AVG` with 17–18 decimals, `SUM` keeps scale | Result quantized to Spark's scale | Emulated (spec §3); the last digit may differ where IRIS rounds before Mborobak does: **to verify** | S§2.6 / diff |
| NUM-04 | Integer overflow (`int + 1` at the maximum) | Error (ANSI) | No overflow (2147483648) | Passed through | Known difference (spec §7.6). Checking every operation costs too much in P0 | S§2.6 / diff |
| NUM-05 | `sum` of `long` beyond 64 bits | Error (ANSI) | No overflow | Passed through | Known difference (spec §7.6) | — / diff |
| NUM-06 | Decimal precision overflow (`CAST(123456.789 AS DECIMAL(5,2))`) | Error (ANSI) | `123456.79`, precision ignored | Passed through | Known difference (spec §7.6) | S§2.6 / diff |
| NUM-07 | Division by zero | Error `DIVIDE_BY_ZERO` | Fatal error -400: `<DIVIDE>` for integers, **`<ILLEGAL VALUE>` for `DOUBLE`** | `MborobakArithmeticError` for both codes | Emulated (spec §7.5) | S§2.6, P-c / diff |
| NUM-08 | `%` with a negative operand (`-7 % 3`, `7 % -3`) | `-1`, `1` (sign of the dividend) | `MOD`: `-1`, `1`. The `#` operator follows the divisor (`2`, `-2`) | `MOD(n, d)`, never `#` | Same | P-d / diff |
| NUM-09 | Floating-point `sum` | Order-dependent last digits | Same, also between serial and parallel plans | Compared with relative tolerance 1e-12 | Same within tolerance (scope §8.2) | S§2.8 / diff |
| NUM-10 | Decimal and integer results | Exact | Exact | Compared exactly | Same | S§2.8 |
| NUM-11 | Type of `count` | `LONG`, never NULL | Integer | Converted | Emulated (spec §3) | — / diff |
| NUM-12 | `decimal` divided by an integer column or literal | `decimal` (`n / 2` → `decimal(14,6)`, `n / i` → `decimal(21,13)`); `decimal / double` → `double` | Decimal division | Spark's decimal typing, an integer literal counted by its digits; no cast to `DOUBLE` | Emulated (spec §3) | diff |
| NUM-13 | Decimal division whose Spark type has more digits than IRIS computes (`decimal(12,4) / bigint` → `decimal(38,25)`) | Exact to the type's scale (`0.8571428571428571428571429`) | About 19 significant digits (`0.857142857142857142 9`) | Result quantized to the type's scale | Known difference: IRIS's decimal arithmetic precision | diff |

### 2.4 Casts

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| CAST-01 | `'abc'` → int or double | Error `CAST_INVALID_INPUT` | `0` / `0.0`, silently | Validity check, `MborobakCastError` | Emulated (spec §7.7) | S§2.6, P-c / diff |
| CAST-02 | `' 1'` → int (spaces) | `1` (trimmed) | `ISNUMERIC` rejects untrimmed text | `TRIM` before checking | Emulated (§7.7) | P-c / diff |
| CAST-03 | `'1.5'`, `'1e3'`, `'1.'`, `'.5'` → int | Error | Accepted by `ISNUMERIC` | Integer pattern rejects them | Emulated (§7.7) | P-c / diff |
| CAST-04 | `'+3'`, `'007'` → int | 3, 7 | — | Accepted | Emulated (§7.7) | P-c / diff |
| CAST-05 | `''` → int | Error | — | `MborobakCastError` | Emulated (§7.7) | P-c / diff |
| CAST-06 | Full-width digits (`'１'`) → int | Error | `ISNUMERIC` 0, `%PATTERN` 1 | `MborobakCastError` | Same | P-c / diff |
| CAST-07 | `'NaN'`, `'Infinity'`, `'-Infinity'`, `'inf'`, `'+Inf'` (any case, trimmed) → double | NaN, ±∞ | `ISNUMERIC` is 0 for all of them | Extra `WHEN UPPER(TRIM(s)) IN (…)` branches; NaN and ±∞ sent as parameters | Emulated (spec §7.7) | P-d / diff |
| CAST-08 | 20-digit text → `long` | Error (overflow) | Accepted, no overflow | Passed through | Known difference (spec §7.7) | P-c / diff |
| CAST-09 | `double` or `decimal` → `int` (`2.7`, `-2.7`, `1.5`, `-0.1`) | Truncates toward zero | Truncates toward zero (2, -2, 1, 0) | Plain `CAST` | Same | P-d / diff |
| CAST-10 | `double` or `decimal` → `int`/`long` out of range (`1e20`) | Error (ANSI) | `100000000000000000000`, no error | Range check before the `CAST`; `MborobakArithmeticError` | Emulated (spec §7.7) | P-d / diff |
| CAST-11 | `int`, `date`, `timestamp` → string | `'-7'`, `'2026-01-02'`, `'2026-01-02 03:04:05.123456'` | Same text | Plain `CAST` | Same | P-d / diff |
| CAST-12 | `double` → string | Shortest form (`'2.7'`, `'0.1'`, `'1.0E20'`) | Full binary expansion (`'2.7000000000000001776'`, `'.10000000000000000556'`, `'100000000000000000000'`) | Plain `CAST` | Known difference: rendering Spark's text in SQL is not practical in P0. Fix candidate: raise `UnsupportedOperationError` instead, so no wrong text is produced | P-d / diff |
| CAST-13 | `decimal(10,4)` → string (`1.5000`) | `'1.5000'` (keeps scale) | `'1.5'` (drops trailing zeros) | Plain `CAST` | Known difference; same fix candidate as CAST-12 | P-d / diff |
| CAST-14 | string → `date`, `timestamp` (ISO text) | Parsed | Parsed (`'2026-01-02'`, `'… 03:04:05.5'`) | Plain `CAST` | Same; invalid text **to verify** | P-d / diff |
| CAST-15 | `double` NaN, +∞, −∞ → `int`/`long` | Error (ANSI) | NaN becomes **0 silently**; ±∞ come back as text (`'inf'`) the driver can't read | Range check (NaN caught by `x <> x`); `MborobakArithmeticError` | Emulated (spec §7.7) | diff |
| CAST-16 | `boolean` → string | `'true'` / `'false'` | `'1'` / `'0'` (BIT, or the 1/0 `CASE` of a condition) | `CASE … THEN 'true' … THEN 'false' END` | Emulated (spec §7.7) | diff |
| CAST-17 | Nested casts (`x.cast("string").cast("double").cast("int")`) | Same values as one by one | — | Each guarded cast's operand is computed once, in a subquery column | Same (spec §7.7) | diff |

### 2.5 Floating-point special values

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| FP-01 | `-0.0` vs `0.0` in `=`, grouping, `distinct` | Equal; normalized | Equal; one group | Passed through | Same | S§2.7, P-c / diff |
| FP-02 | `-0.0` returned as a value | `-0.0` from storage; `0.0` after grouping | `-0.0` from storage; `0.0` after grouping | Passed through | Same | P-c / diff |
| FP-03 | NaN = NaN, NaN in grouping and joins | True; one group; joins match | Same | Passed through | Same | P-c / diff |
| FP-04 | NaN in sort order; `-0.0` and `0.0` tie | Largest value (after +∞); ±0 compare equal | After +∞ | Bucket sort key | Emulated (spec §7.9) | P-c / diff |
| FP-05 | `-Infinity` in sort order | Smallest value | Sorted after all finite values | Bucket sort key | Emulated (spec §7.9) | P-c / diff |
| FP-06 | `max` with NaN present | NaN | Ignores NaN | Passed through | Known difference: a NaN-aware `max` needs an extra aggregate per column; candidate for P1 | P-c / diff |
| FP-07 | `min` with NaN present | Ignores NaN (NaN is largest) | Ignores NaN | Passed through | Same | P-c / diff |
| FP-08 | NaN or ±∞ as a literal (`F.lit(float("nan"))`) | Supported | Fails as SQL text; works as a parameter | Always a parameter | Emulated (spec §9.5) | S§2.7 / diff |
| FP-09 | `DOUBLE` value `-1E19` in `orderBy` | Normal value | Returned as NULL; NULLs misplaced | Bucket key plus `d + 0` | Emulated (spec §7.9) | P-c / diff |
| FP-10 | `DOUBLE` value `-1E19` in `groupBy`, `distinct`, `countDistinct` | Normal value | Merged with NULL | Passed through | Known difference (spec O7) | P-c / diff |
| FP-11 | Functions returning NaN or ∞ in Spark (`log(0)`, `sqrt(-1)`) | NaN / -∞ | Error -400 `<ILLEGAL VALUE>` | Not in P0 (math functions are P1) | Unsupported in P0 | S§2.7, P-c / doc |

### 2.6 Ordering and limits

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| ORD-01 | `orderBy().limit()` then more transformations | Order and limit kept | `ORDER BY` rejected in subqueries without `TOP` | `TOP n` inside, order re-applied outside | Emulated (spec §7.2) | S§2.1 / diff |
| ORD-02 | `orderBy` followed by `groupBy`, `join`, `distinct` or `union` | Order lost | — | Sort dropped | Same (spec §7.2) | — / doc |
| ORD-03 | `limit` without `orderBy` | Any `n` rows | Any `n` rows | `TOP n` | Same; tests compare as sets | — / diff |
| ORD-04 | Ties in `orderBy` | Unspecified order | Unspecified order | Passed through | Same; tests sort by all columns before comparing | — / doc |
| ORD-05 | `limit` inside a subquery | Supported | `LIMIT` rejected; `TOP` works | `TOP` everywhere | Emulated (spec §7.3) | S§2.2 / diff |
| ORD-06 | `orderBy` → `select` without the sort key → `limit` → `filter` | Order kept | — | Sort key carried as a hidden subquery column; order applied again outside | Emulated (spec §7.2) | diff |

### 2.7 Grouping, joins and union

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| GRP-01 | `groupBy` by a column position in `sql()` (`GROUP BY 1`) | Position | Constant 1 | User's SQL passed through | Known difference in `sql()` only; the compiler never emits positions (spec §7.8) | S§2.3 / doc |
| GRP-02 | Filter after `agg()` | `HAVING` semantics | Same | `HAVING` or outer `WHERE` | Same | — / diff |
| GRP-03 | `groupBy(k).min()` / `.avg()` without arguments | All numeric columns, grouping columns included | — | Same | Same | diff |
| JOIN-01 | Join types inner, left, right, full, cross | Supported | Supported | Passed through | Same | P-b / diff |
| JOIN-02 | `join(on="id")` output columns | Key once, then left, then right | — | Built by the analyzer | Emulated (spec §6.3) | — / diff |
| JOIN-03 | `join(on="id", how="full")` key value | Coalesced key | — | `COALESCE(a.id, b.id)` | Emulated (spec §6.3) | P-b / diff |
| JOIN-04 | Duplicate column names after an expression join | Allowed; ambiguous by name | — | Allowed; `AmbiguousColumnError` by name | Same | — / diff |
| JOIN-05 | Semi and anti joins | Supported | — | P1 | Unsupported in P0 | — |
| JOIN-06 | Join output row order | Unspecified | Unspecified | Passed through | Same; compared as sets | — / doc |
| UNI-01 | `union` | `UNION ALL` by position | — | `UNION ALL` | Same (D4) | — / diff |
| UNI-02 | `union` of different numeric types | Widened (`int` + `decimal(10,2)` → `decimal(12,2)`) | — | Widened (int → long → decimal → double) | Same | — / diff |
| UNI-03 | `union` of `int` and `string` | Widened to `bigint`; strings cast at run time (`'7'` works, `'a'` fails) | — | `AnalysisError` at `union()` | Known difference: stricter than Spark (spec §6.2) | — / diff |
| UNI-04 | `union` where one side lists a column twice (`select(col("k"), col("k").alias("k2"))`) | Both columns kept | — | Each output column has its own id | Same | diff |

### 2.8 Dates and time

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| TIME-01 | Timestamp semantics | `TimestampType` is an instant shown in the session time zone | `TIMESTAMP` has no time zone | IRIS timestamps are `TimestampNTZType` | Known difference by design (scope §6.8): compare with Spark's `timestamp_ntz`, not `timestamp` | S§2.7 / doc |
| TIME-02 | Timestamp literal with an offset | Converted to an instant | Fatal error -400 | Literals are parameters; Python `datetime` with `tzinfo` raises `TypeError` | Unsupported | S§2.7 / — |
| TIME-03 | `DATE` values in results | `datetime.date` | Driver returns `str` | Converted | Emulated (API contract §5.2) | S§2.7 / diff |
| TIME-04 | Microseconds | Kept | Kept | Passed through | Same | S§2.7 / diff |

### 2.9 Names and identifiers

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| NAME-01 | Column names are case-insensitive | Yes (default) | Yes | Analyzer matches case-insensitively | Same | S§2.4 / diff |
| NAME-02 | Output name keeps the user's spelling | Yes | Cursor keeps alias case | Quoted aliases | Same | S§2.4 / doc |
| NAME-03 | Default names of unaliased expressions (`sum(amount)`, `count(1)`, `(a + b)`) | Spark's generated names | — | Generated by Mborobak | Emulated (API contract §6.5) | — / diff |
| NAME-04 | Default name of a plain cast and of `when()` | `d` for `col("d").cast("int")`; `CASE WHEN (i > 1) THEN hi END` | — | Same names | Emulated (API contract §6.5; A1 resolved) | — / diff |
| NAME-05 | Reserved words as names | Allowed | Allowed quoted | Always quoted | Same | S§2.4 / doc |
| NAME-06 | Unqualified table name | Current database | `SQLUser` | `SQLUser` | Same in effect | S§2.4 |
| NAME-07 | `filter` or `orderBy` on a column an earlier `select` dropped (`df.select("id").filter(col("i") > 1)`) | Allowed: Spark resolves the column from the input | — | Same: the column is added back below the `select`, then projected away again | Emulated (spec §6.1) | diff |
| NAME-08 | Names from `agg({"x": "mean", "*": "count"})` | `avg(x)`, `count(1)` (dict order not kept) | — | Same names, in insertion order | Same (column order may differ) | diff |

### 2.10 Results and pandas

| ID | Behavior | PySpark | IRIS driver | Mborobak | Status | Evidence |
|----|----------|---------|-------------|----------|--------|----------|
| RES-01 | Python value types | Per Spark type | `Decimal` for some `DOUBLE`s, `str` for dates | Converted to schema types | Emulated (API contract §5.2) | S§2.7 / doc |
| RES-02 | `toPandas()` dtypes (Arrow off) | `int32`/`int64`, `float64` with NULLs, `object` for decimals and dates | — | Same table | Emulated (API contract §5.3) | — / diff |
| RES-03 | `toPandas()` with Arrow on | Nullable types differ | — | Not offered | Open (API contract A2) | — |
| RES-04 | `show()` format, NULL printed as `NULL` | Spark 4 format, including the blank line after a full table | — | Same text, byte for byte | Emulated | — / diff |
| RES-05 | `Row` behavior (tuple, named fields, `asDict`) | — | — | Same | Same | — / doc |

### 2.11 API-level differences

| ID | Behavior | PySpark | Mborobak | Status | Source |
|----|----------|---------|----------|--------|--------|
| API-01 | SQL dialect of `session.sql()` | Spark SQL | IRIS SQL | Known difference by design | API contract §3.5 |
| API-02 | Named `sql()` parameters (`args` as a dict) | Supported | P1 | Unsupported in P0 | API contract §3.5 |
| API-03 | `filter("x > 1")`, `selectExpr`, `F.expr` | Supported | P1 | Unsupported in P0 | API contract §4.1, §4.6 |
| API-04 | `cache()`, `persist()` | Caches | Raises `UnsupportedOperationError` | Unsupported | API contract §4.6 |
| API-05 | `spark.*` configuration keys | Accepted | Raise | Unsupported by design | API contract §3.1 |
| API-06 | Implicit cross join (`join` without `on`) | Allowed (Spark 3+) | Allowed | Same | API contract §4.1 |
| API-07 | Unsupported column types (streams, `%List`, binary) | — | `UnsupportedTypeError` on use | Unsupported | Compiler spec §3 |

## 3. Differential test suite

### 3.1 How a row is verified

1. Load the same dataset into IRIS (through the driver) and into PySpark 4.3 (`createDataFrame` with an explicit schema, ANSI on, session time zone UTC).
2. Run the same DataFrame code on both, with only the session differing.
3. Compare schemas (names and types) exactly, then rows:
   - integers, decimals, strings, dates, booleans: exactly;
   - doubles: relative tolerance 1e-12, NaN equal to NaN, `-0.0` equal to `0.0` unless the row is about the sign;
   - row order compared only when the plan ends with an `orderBy` on a unique key; otherwise as multisets.
4. Errors: both sides must raise, and Mborobak's error class must be the one this matrix names.
5. Run once with optional rewrites on and once off (scope §6.10).

A row moves from `doc` to `diff` when its test passes. A **Same** or **Emulated** row whose test fails becomes a bug, not a new known difference, unless the fix is out of scope, which needs a recorded decision.

### 3.2 Datasets

Each section of §2 has a small targeted dataset (scope §8.2):

| Dataset | Content |
|---------|---------|
| `strings_mixed` | The study's set: `apple`, `Apple`, `APPLE`, `banana`, `apple ` (trailing space), ` apple` (leading space), `Banana`, `''`, NULL |
| `numbers_edge` | 0, ±1, int maximum and minimum, values around `-1E19`, ±1E300, `-0.0`, NaN, ±∞, NULL; decimals with scale 2 and 4 |
| `cast_text` | The 18 inputs of probe 2026-11-18c plus `'NaN'`, `'Infinity'`, `'2.7'`, `'-2.7'` |
| `join_keys` | Keys with NULLs, duplicates, unmatched rows on both sides, mixed-case strings |
| `times` | Dates and timestamps with microseconds, around midnight and month ends |
| `sales_small` | The scope §5.3 example: a few hundred rows with skew (scope §8.3) |

### 3.3 Keeping the matrix and tests in step

A check in CI reads the IDs from this document and the test names from the suite. It fails when a row has no test, when a test names an ID that doesn't exist, or when a row is marked `diff` but its test is skipped.

## 4. Summary for users

For the README and the docs site, the P0 known differences fit in one short list:

- Integer and decimal overflow in arithmetic and text casts are not detected (NUM-04 to NUM-06, CAST-08).
- Decimal division keeps about 19 significant digits, fewer than Spark's widest decimal results (NUM-13).
- `max` ignores NaN (FP-06).
- The `DOUBLE` value `-1E19` groups together with NULL (FP-10).
- `like` has no escape character (STR-14).
- Casting `double` and `decimal` values to string gives IRIS's text, not Spark's (CAST-12, CAST-13).
- `union` is stricter about mixing types (UNI-03).
- Timestamps have no time zone (TIME-01).
- `session.sql()` takes IRIS SQL, where `GROUP BY 1` means the constant 1 (API-01, GRP-01).
- With `stringSemantics="iris"`, string comparison, grouping and sorting follow IRIS's case-insensitive rules (§2.1).

## 5. Open items

| # | Item | Rows |
|---|------|------|
| M1 | ~~Make PySpark 4.3 runnable~~ Done: PySpark 4.3.0 on OpenJDK 21 (`pip install -e ".[compat]"`, `brew install openjdk@21`) | — |
| M2 | ~~Probe IRIS `MOD` with negative operands~~ Done (P-d): same as Spark | NUM-08 |
| M3 | ~~Probe `ISNUMERIC('NaN')`, double → int, casts to text~~ Done (P-d) | CAST-07, CAST-09 to CAST-14 |
| M7 | Decide between keeping CAST-12 and CAST-13 as known differences or applying their fix candidate (CAST-07 is now emulated) | CAST-12, CAST-13 |
| M4 | ~~Confirm Spark's handling of full-width digits~~ Done: Spark rejects them | CAST-06 |
| M5 | ~~Confirm generated names for casts and `when()`~~ Done | NAME-03, NAME-04 |
| M8 | Write differential tests for the rows still marked `doc` | §2 |
| M6 | Re-check all IRIS evidence on build 154 (spec O6) | All |

## 6. Probe 2026-11-18d

Script [`probes/matrix_probe.py`](../probes/matrix_probe.py), raw output [`probes/results-2026-11-18d-iris-2026.2.json`](../probes/results-2026-11-18d-iris-2026.2.json). Same IRIS 2026.2 instance, scratch table in `MborobakProbe`, dropped afterwards.

| Check | Result |
|-------|--------|
| `MOD(a, b)` for (−7, 3), (7, −3), (−7, −3), (7, 3) | −1, 1, −1, 1 (sign of the dividend, as Spark) |
| `a # b`, same pairs | 2, −2, −1, 1 (sign of the divisor) |
| `ISNUMERIC` of `'NaN'`, `'Infinity'`, `'-Infinity'`, `'inf'` | 0 for all |
| `CAST(d AS INTEGER)` and `AS BIGINT` for 2.7, −2.7, 0.1, 1e20 | 2, −2, 0, 100000000000000000000 |
| `CAST(n AS INTEGER)` for `NUMERIC(10,4)` 1.5, −0.1, 100 | 1, 0, 100 |
| `CAST(… AS VARCHAR)` of `DOUBLE` 2.7, −2.7, 0.1, 1e20 | `2.7000000000000001776`, `-2.7000000000000001776`, `.10000000000000000556`, `100000000000000000000` |
| `CAST(… AS VARCHAR)` of `NUMERIC(10,4)` 1.5, `INTEGER` −7, `DATE`, `TIMESTAMP` | `1.5`, `-7`, `2026-01-02`, `2026-01-02 03:04:05.123456` |
| `CAST('2026-01-02' AS DATE)`, `CAST('2026-01-02 03:04:05.5' AS TIMESTAMP)` | Parsed |
