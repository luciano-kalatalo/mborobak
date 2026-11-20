# Compatibility Matrix

Scientificloud · Mborobak · Control document 3 of 3 · Draft 0.10 · 2026-11-20

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
| `P-k` | Regular-expression probe 2026-11-20 ([`probes/regex_probe.py`](../probes/regex_probe.py)), [compiler spec](compiler-spec.md) §12.8 |
| `P-j` | Temporary-table probe 2026-11-20 ([`probes/temp_table_probe.py`](../probes/temp_table_probe.py)), [compiler spec](compiler-spec.md) §9.8 |
| `P-i` | NaN comparison probe 2026-11-20 ([`probes/nan_comparison_probe.py`](../probes/nan_comparison_probe.py)), [compiler spec](compiler-spec.md) §12.7 |
| `P-h` | Write probe 2026-11-20 ([`probes/write_probe.py`](../probes/write_probe.py)), [compiler spec](compiler-spec.md) §12.6 |
| `P-g` | Date and time probe 2026-11-20 ([`probes/date_probe.py`](../probes/date_probe.py)), [compiler spec](compiler-spec.md) §12.5 |
| `P-f` | Window-function probe 2026-11-20 ([`probes/window_probe.py`](../probes/window_probe.py)), [compiler spec](compiler-spec.md) §12.4 |
| `P-e` | Scalar-function probe 2026-11-19 ([`probes/scalar_function_probe.py`](../probes/scalar_function_probe.py)), [compiler spec](compiler-spec.md) §12.3 |
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
| STR-07 † | `orderBy` a string column | Binary order (`'B' < 'a'`) | Case-insensitive order | `ORDER BY %EXACT(' ' \|\| col)` (STR-16) | Emulated (§7.1) | P-b / diff |
| STR-08 † | `min`, `max`, `countDistinct` on strings | Exact values | Uppercased / merged | `SUBSTRING(MIN(%EXACT(' ' \|\| col)), 2)` (STR-16), `COUNT(DISTINCT %EXACT(col))` | Emulated (§7.1) | S§2.5, P-b / diff |
| STR-09 | Column declared `COLLATE %EXACT` | — | Already exact | No wrapper | Same | P-c |
| STR-10 | Columns with other collations (`SQLSTRING`, `TRUNCATE(n)`) | — | Differ | `%EXACT` | Emulated (§6.4) | P-c |
| STR-11 | Comparisons between literals or parameters | Exact | Exact | Passed through | Same | S§2.5 |
| STR-12 | `''` vs NULL | Distinct | Distinct | Passed through | Same | S§2.6 / diff |
| STR-13 | NULL in string concatenation | NULL | NULL | `\|\|` passed through (`concat`, 0.2) | Same | S§2.6, P-e / diff |
| STR-14 | `like` escape character | `\` by default | No default escape | P0 has no escape support | Known difference: patterns containing `\` differ. Escapes are P1 | — / doc |
| STR-15 | Cost of `%EXACT` | — | Index still used, but equality filters on indexed strings read every candidate row: 3.3× slower count at 1M rows; no cost on grouping, joins, sorts | — | Measured ([benchmark](benchmark-report-2026-11-18.md) §3.2) | S§2.5 |
| STR-16 † | Strings that look like numbers in `<`, `>`, `between`, `orderBy`, `min`/`max`, `greatest`/`least` (`'5'` vs `'12345'`, `'9'` vs `'10'`) | Code-point order: `'5' > '12345'`, `'9' > '10'` | With `%EXACT`, and in `EXACT` columns, canonical numbers compare and sort as numbers and before other strings: `'12345' > '5'` | `%EXACT(' ' \|\| x)` in ordering contexts, on both sides and on literals; equality, grouping and `distinct` keep `%EXACT(x)`. Release 0.1 ordered such strings as numbers | Emulated (spec §7.1) | P-e / diff |

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
| NUM-14 | A fractional constant beside an integer column: `i + 1.5`, `i - 0.25`, `coalesce(i, 1.5)` | `8.5`, `6.75`, `1.5` | **The `?` parameter takes the column's integer type: `7 + 1.5` gives 9**, `COALESCE(i, ?)` gives 2; `*` and comparisons are not affected | Decimal and finite double constants sent as `CAST(? AS NUMERIC(p,s))` / `CAST(? AS DOUBLE)` (spec §9.5). Release 0.1 returned IRIS's result | Emulated (spec §9.5) | diff |

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
| CAST-14 | string → `date`, `timestamp` | Spark's shapes, trimmed: `yyyy`, `yyyy-m`, `yyyy-m-d`, then for a date anything after a space or `T`, for a timestamp `h:m[:s[.f]]` | `CAST` accepts other forms and reads `' 2026-01-05 '` as day zero | Shape checked with `%PATTERN`, then `TO_DATE`/`TO_TIMESTAMP` (spec §7.7) | Emulated (spec §7.7) | P-d, P-g / diff |
| CAST-18 | Invalid text to `date` (`'2026-02-30'`, `'abc'`, `''`, `'2026/01/05'`) | Error `CAST_INVALID_INPUT` | **`CAST` returns `1840-12-31`, no error** | `MborobakCastError`: the shape test reaches the cast trigger; `TO_DATE` fails on impossible dates, mapped to the same error. Release 0.1 returned IRIS's day zero | Emulated (spec §7.7) | P-g / diff |
| CAST-15 | `double` NaN, +∞, −∞ → `int`/`long` | Error (ANSI) | NaN becomes **0 silently**; ±∞ come back as text (`'inf'`) the driver can't read | Range check (NaN caught by `x <> x`); `MborobakArithmeticError` | Emulated (spec §7.7) | diff |
| CAST-16 | `boolean` → string | `'true'` / `'false'` | `'1'` / `'0'` (BIT, or the 1/0 `CASE` of a condition) | `CASE … THEN 'true' … THEN 'false' END` | Emulated (spec §7.7) | diff |
| CAST-17 | Nested casts (`x.cast("string").cast("double").cast("int")`) | Same values as one by one | — | Each guarded cast's operand is computed once, in a subquery column | Same (spec §7.7) | diff |

### 2.5 Floating-point special values

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| FP-01 | `-0.0` vs `0.0` in `=`, grouping, `distinct` | Equal; normalized | Equal; one group | Passed through | Same | S§2.7, P-c / diff |
| FP-02 | `-0.0` returned as a value | `-0.0` from storage; `0.0` after grouping | `-0.0` from storage; `0.0` after grouping | Passed through | Same | P-c / diff |
| FP-03 | NaN = NaN, NaN in grouping and joins | True; one group; joins match | One group; joins match; `=` between two columns of a row is false (FP-13) | Grouping passed through; `=` tests NaN first (spec §7.14) | Same | P-c, P-i / diff |
| FP-04 | NaN in sort order; `-0.0` and `0.0` tie | Largest value (after +∞); ±0 compare equal | After +∞ | Bucket sort key | Emulated (spec §7.9) | P-c / diff |
| FP-05 | `-Infinity` in sort order | Smallest value | Sorted after all finite values | Bucket sort key | Emulated (spec §7.9) | P-c / diff |
| FP-06 | `max` with NaN present | NaN | Ignores NaN | Passed through | Known difference, in windows too (`max(d).over(w)`): a NaN-aware `max` needs an extra aggregate per column; candidate for P1 | P-c / diff |
| FP-07 | `min` with NaN present | Ignores NaN (NaN is largest) | Ignores NaN | Passed through | Same | P-c / diff |
| FP-08 | NaN or ±∞ as a literal (`F.lit(float("nan"))`) | Supported | Fails as SQL text; works as a parameter | Always a parameter | Emulated (spec §9.5) | S§2.7 / diff |
| FP-09 | `DOUBLE` value `-1E19` in `orderBy` | Normal value | Returned as NULL; NULLs misplaced | Bucket key plus `d + 0` | Emulated (spec §7.9) | P-c / diff |
| FP-10 | `DOUBLE` value `-1E19` in `groupBy`, `distinct`, `countDistinct`, window partitions, `dropDuplicates`, and semi and anti join conditions | Normal value | Merged with NULL; a NULL key matches `-1E19` in a semi or anti join condition (spec §7.15) | Passed through | Known difference (spec O7) | P-c, P-i / diff |
| FP-11 | Functions returning NaN, NULL or ∞ in Spark (`sqrt(-1)`, `log(0)`, `pow(0, -1)`, `exp(1000)`) | NaN, NULL, ∞ | Fatal error -400 | Out-of-domain inputs routed before the IRIS function (spec §7.10) | Emulated (spec §7.10) | S§2.7, P-e / diff |
| FP-13 | Comparisons with NaN: `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`, `isin`, `between`, in filters and as values | NaN above every value, `+∞` included; NaN = NaN | Varies by plan: NaN below every value between columns of a row, `NaN = NaN` false there and true across a join, NaN parameters unequal to NaN | NaN decided by `x <> x` in `CASE` before IRIS compares (spec §7.14). Fixed in 0.2.1: `filter(d > 0)` dropped NaN rows | Emulated (spec §7.14) | P-i / diff |
| FP-12 | `+∞` plus `-∞` (`sum`, `avg`, `+` of both infinities) | NaN | Fatal error -400 `<ILLEGAL VALUE>` | `MborobakArithmeticError` | Known difference: emulating it would need a check per row in every sum | P-e / diff |

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
| GRP-01 | `groupBy` by a column position in `sql()` (`GROUP BY 1`) with the IRIS dialect | Position | Constant 1 | User's SQL passed through (the Spark dialect, the default, takes positions: SQL-03) | Known difference with `mborobak.sql.dialect = "iris"` only; the compiler never emits positions (spec §7.8) | S§2.3 / doc |
| GRP-02 | Filter after `agg()` | `HAVING` semantics | Same | `HAVING` or outer `WHERE` | Same | — / diff |
| GRP-03 | `groupBy(k).min()` / `.avg()` without arguments | All numeric columns, grouping columns included | — | Same | Same | diff |
| JOIN-01 | Join types inner, left, right, full, cross | Supported | Supported | Passed through | Same | P-b / diff |
| JOIN-02 | `join(on="id")` output columns | Key once, then left, then right | — | Built by the analyzer | Emulated (spec §6.3) | — / diff |
| JOIN-03 | `join(on="id", how="full")` key value | Coalesced key | — | `COALESCE(a.id, b.id)` | Emulated (spec §6.3) | P-b / diff |
| JOIN-04 | Duplicate column names after an expression join | Allowed; ambiguous by name | — | Allowed; `AmbiguousColumnError` by name | Same | — / diff |
| JOIN-05 | Semi and anti joins (`left_semi`, `left_anti` and their aliases): by condition or by name, duplicate and NULL keys, string and NaN keys | Left rows with (without) a match, once each; left columns, keys first for a join on names | Correlated `EXISTS` evaluates correctly, except FP-10 | `[NOT] EXISTS (SELECT 1 FROM right WHERE cond)` (spec §7.15) | Same (0.2.x) | P-i / diff |
| JOIN-06 | Join output row order | Unspecified | Unspecified | Passed through | Same; compared as sets | — / doc |
| UNI-01 | `union` | `UNION ALL` by position | — | `UNION ALL` | Same (D4) | — / diff |
| UNI-02 | `union` of different numeric types | Widened (`int` + `decimal(10,2)` → `decimal(12,2)`) | — | Widened (int → long → decimal → double) | Same | — / diff |
| UNI-03 | `union` of `int` and `string` | Widened to `bigint`; strings cast at run time (`'7'` works, `'a'` fails) | — | `AnalysisError` at `union()` | Known difference: stricter than Spark (spec §6.2) | — / diff |
| UNI-05 | `unionByName`, with and without `allowMissingColumns` | Columns matched by name, case-insensitively; missing ones NULL; error when a column is on one side only | — | Right side reordered, NULL columns added, then `union` | Same (0.2.x) | diff |
| UNI-04 | `union` where one side lists a column twice (`select(col("k"), col("k").alias("k2"))`) | Both columns kept | — | Each output column has its own id | Same | diff |

### 2.8 Dates and time

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| TIME-01 | Timestamp semantics | `TimestampType` is an instant shown in the session time zone | `TIMESTAMP` has no time zone | IRIS timestamps are `TimestampNTZType` | Known difference by design (scope §6.8): compare with Spark's `timestamp_ntz`, not `timestamp` | S§2.7 / doc |
| TIME-02 | Timestamp literal with an offset | Converted to an instant | Fatal error -400 | Literals are parameters; Python `datetime` with `tzinfo` raises `TypeError` | Unsupported | S§2.7 / — |
| TIME-03 | `DATE` values in results | `datetime.date` | Driver returns `str` | Converted | Emulated (API contract §5.2) | S§2.7 / diff |
| TIME-04 | Microseconds | Kept | Kept | Passed through | Same | S§2.7 / diff |
| TIME-05 | Result type of `to_timestamp`, `date_trunc`, `current_timestamp`, `now` | `timestamp` (with a time zone) | — | `timestamp_ntz`, the type of every Mborobak timestamp (TIME-01); same values when Spark's session time zone is the data's | Known difference by design | P-g / diff |

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
| RES-02 | `toPandas()` dtypes with PySpark's default conversion (Arrow on in 4.3) | `int32`/`int64`, `float64` with NULLs, `object` for decimals and dates | — | Same table | Emulated (API contract §5.3); booleans and timestamps not yet tested | — / diff |
| RES-03 | `toPandas()` with Arrow turned off (`spark.sql.execution.arrow.pyspark.enabled=false`) | Not compared | — | Mborobak has one conversion, matching the default | Open: whether any type differs when Arrow is off; a Mborobak option is not planned | — |
| RES-04 | `show()` format, NULL printed as `NULL` | Spark 4 format, including the blank line after a full table | — | Same text, byte for byte | Emulated | — / diff |
| RES-05 | `Row` behavior (tuple, named fields, `asDict`) | — | — | Same | Same | — / doc |

### 2.11 API-level differences

| ID | Behavior | PySpark | Mborobak | Status | Source |
|----|----------|---------|----------|--------|--------|
| API-01 | SQL dialect of `session.sql()` | Spark SQL | Spark SQL `SELECT` statements by default since 0.3 (§2.19); IRIS SQL with `mborobak.sql.dialect = "iris"` | Same by default; known difference by design with `"iris"` | API contract §3.5 |
| API-02 | Named `sql()` parameters (`args` as a dict, `:name` markers) | Supported | Supported (0.2.x): bound as `?` parameters | Same | API contract §3.5 |
| API-03 | `filter("x > 1")`, `where(...)`, `selectExpr`, `F.expr` | Supported | Supported (0.2): Spark SQL expressions, §2.15 | Same | API contract §4.1, §6.4 |
| API-04 | `cache()`, `persist()` | Caches | Raises `UnsupportedOperationError` | Unsupported | API contract §4.6 |
| API-05 | `spark.*` configuration keys | Accepted | Raise | Unsupported by design | API contract §3.1 |
| API-06 | Implicit cross join (`join` without `on`) | Allowed (Spark 3+) | Allowed | Same | API contract §4.1 |
| API-07 | Unsupported column types (streams, `%List`, binary) | — | `UnsupportedTypeError` on use | Unsupported | Compiler spec §3 |

### 2.12 Scalar functions and missing values (release 0.2)

Null, string and math functions, and `fillna`/`dropna`. The dataset `funcs` (§3.2) holds empty strings, NULLs, NaN, ±∞, `1E300`, `-1E-300`, `.5` ties and strings that look like numbers. The default column names and result types are PySpark's and are checked by the same tests.

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| FUN-01 | `coalesce`, `ifnull`, `nvl`, `nvl2` | Wider type of the arguments | `COALESCE` needs one type; 2-argument `IFNULL` returns NULL when the first value is not NULL | `COALESCE` with arguments cast to the wider type; `nvl2` as `CASE` | Emulated (spec §7.10) | P-e / diff |
| FUN-02 † | `nullif` on strings | Case-sensitive | Compares with the column's collation: `NULLIF('Hello', 'HELLO')` is NULL | `%EXACT` on both arguments; NaN equals NaN for doubles | Emulated (§7.10) | P-e / diff |
| FUN-03 | `greatest`, `least` with NULLs | NULLs skipped; NULL only if all are | NULL if any argument is NULL | Each argument replaced by the first non-NULL of the arguments | Emulated (§7.10) | P-e / diff |
| FUN-04 | `isnan`, `isnull`, `isnotnull` | `isnan(NULL)` is false | `x <> x` finds NaN inside `CASE` only, never in `WHERE` | `CASE WHEN x <> x THEN 1 ELSE 0 END = 1` | Emulated (§7.10) | P-e / diff |
| FUN-05 | `upper`, `lower`, `trim`, `ltrim`, `rtrim`, `initcap`, `reverse`, `repeat` | `trim` removes spaces only | Same; `REPEAT` of a count ≤ 0 differs | Direct; `repeat(s, 0)` is `''` | Emulated (§7.10) | P-e / diff |
| FUN-06 | `length`, `char_length` with trailing spaces | Counted | `LENGTH` ignores them; `CHAR_LENGTH` counts them | `CHAR_LENGTH` | Emulated (§7.10) | P-e / diff |
| FUN-07 | `substring`/`substr` with position 0, negative positions, lengths past the end | 0 counts as 1; negative counts from the end, clipped | `SUBSTRING('Hello', -3, 2)` is `''`; position 0 shortens the result | Spark's window computed with `CHAR_LENGTH` | Emulated (§7.10) | P-e / diff |
| FUN-08 | `concat` (NULL in, NULL out) and `concat_ws` (NULLs skipped, never NULL) | As stated | `\|\|` returns NULL for a NULL part; `CONCAT` takes two arguments | `\|\|`; `concat_ws` from `COALESCE(sep \|\| x, '')` parts. Double and decimal arguments are refused (CAST-12, CAST-13) | Emulated (§7.10) | P-e / diff |
| FUN-09 | `lpad`, `rpad`: truncation, empty pad, length ≤ 0 | Truncates; empty pad only truncates; `''` for length ≤ 0 | Truncates; empty pad inserts NUL characters; NULL for length < 0 | `LPAD`/`RPAD`, with `SUBSTRING` for an empty pad and `''` for length ≤ 0 | Emulated (§7.10) | P-e / diff |
| FUN-10 | `instr`, `locate` (empty substring, start before 1) | Empty substring found at 1; `pos < 1` gives 0 | `INSTR('abc', '')` is 0 | `INSTR`, with Spark's results for those cases | Emulated (§7.10) | P-e / diff |
| FUN-11 | `replace` with an empty search string | Source unchanged | `REPLACE('', '', 'x')` is `'x'` | Source returned | Emulated (§7.10) | P-e / diff |
| FUN-12 | `round` on decimals and integers | Half up; Spark's result type | `ROUND` half up | `ROUND`, cast to Spark's type | Same | P-e / diff |
| FUN-13 | `round` on doubles (`round(0.285, 2)`, NaN, ±∞, `1E300`) | Rounds the shortest decimal form: 0.29; NaN and ±∞ unchanged | Rounds the 20-digit binary expansion: 0.28; NaN, ±∞ and values past `1E145` fail | Rounds to 15 significant digits first; NaN, ±∞ and values above 2⁵³ returned as they are | Emulated (§7.10) | P-e / diff |
| FUN-14 | `bround` (half to even) | Half even | No half-even rounding | Half-even test on the scaled value with `TRUNCATE` | Emulated (§7.10) | P-e / diff |
| FUN-15 | `ceil`, `floor`: types, NaN, ±∞ | `bigint` for doubles; NaN → 0; ±∞ and values beyond `bigint` fail (ANSI); `decimal(p-s+1,0)` for decimals | Returns a double | Spark's types; NaN → 0; overflow raises `MborobakArithmeticError` | Emulated (§7.10) | P-e / diff |
| FUN-16 | `pow`, `sqrt`, `exp`, `log`, `ln`, `log10`, `log2`, `log1p`, `log(base, x)` out of domain | NaN, NULL or ∞ | Fatal errors; no `LN`, `LOG2`; `NaN <= 0` is true | Guards before the IRIS function (NaN first); `log1p` by series for small values | Emulated (§7.10) | P-e / diff |
| FUN-17 | `abs`, `signum`, `sign` | `signum` is a double; `signum(NaN)` is NaN | `SIGN` returns an integer; `SIGN(NaN)` is NULL | Cast to double; NaN passed through | Emulated (§7.10) | P-e / diff |
| FUN-18 | `pmod` | Non-negative result; division by zero fails | `MOD` has the dividend's sign | `CASE` on `MOD`; zero divisor raises `MborobakArithmeticError` | Emulated (§7.10) | P-e / diff |
| FUN-19 † | Functions in filters, `groupBy` keys, sort keys and aggregates | — | Function results compare with their own collation | Same rules as columns (§7.1) | Emulated (§7.1) | P-e / diff |
| FUN-20 | `fillna`/`na.fill` | Fills NULL and NaN in the columns of the value's kind; the value is cast to the column's type | — | `COALESCE`, or `CASE` for doubles | Emulated (API contract §4.1) | diff |
| FUN-21 | `dropna`/`na.drop` (`any`, `all`, `thresh`, NaN) | NaN counts as missing | — | `WHERE` on `IS NOT NULL` and the NaN test | Emulated (API contract §4.1) | diff |
| FUN-22 | `initcap` after a tab or other non-space separator | Only a space starts a word: `'tab\there'` → `'Tab\there'` | `INITCAP` capitalizes after any whitespace | Passed through | Known difference | P-e / diff |
| FUN-23 | `upper`/`lower` of letters whose case form is longer (`'ß'` → `'SS'`) | Java's full case mapping | One character per character (`'ß'` stays) | Passed through | Known difference | P-e / doc |
| FUN-24 | `pow` overflow (`pow(10.0, 400.0)`) | Infinity | Fatal error | `MborobakArithmeticError` | Known difference (as NUM-04) | P-e / diff |
| FUN-25 | Integer overflow in `abs(-2147483648)`, `round(i, -1)` near the maximum | Error (ANSI) | Larger value returned | Passed through | Known difference (as NUM-04) | P-e / diff |
| FUN-26 | `round`/`bround` of doubles whose shortest form has 16 or 17 digits | Rounds that form | — | Rounds the 15-digit form, which can differ at a `.5` tie | Known difference | P-e / doc |

### 2.13 Window functions and `dropDuplicates` (release 0.2)

`Window.partitionBy().orderBy().rowsBetween()/rangeBetween()` with `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first`, `last` and the aggregates over a window. The dataset `wins` (§3.2) has ties, NULL keys, NaN, ±∞, `-1E19` and numeric-looking strings. Default column names (`RANK() OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN …)`) and types are PySpark's and are checked by the same tests. Every window expression is computed as a hidden column of a subquery (compiler spec §7.11).

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| WIN-01 | `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile` | Standard; `int`, or `double` for the ratios | Same functions | Direct | Same | P-f / diff |
| WIN-02 | `lag`, `lead` with offsets, defaults, a negative offset, offset 0 | Negative `lag` is `lead`; offset 0 is the value | `LAG(x, -1)` fails | `LAG`/`LEAD` with a positive offset; offset 0 renders the value | Emulated (spec §7.11) | P-f / diff |
| WIN-03 | Aggregates over an ordered window without a frame | RANGE frame: tied rows share the running value | Same default | Frame left implicit | Same | P-f / diff |
| WIN-04 | Aggregates over a whole partition, `count(*)`, strings in `min`/`max` | Exact string order | Same; strings by collation | `SUBSTRING(MIN(%EXACT(' ' \|\| g)) OVER (…), 2)` | Emulated (§7.1) | P-f / diff |
| WIN-05 | `rowsBetween` with offsets and unbounded ends | Supported | `ROWS` frames with constant offsets; no parameters | Constants inline | Same | P-f / diff |
| WIN-06 | `rangeBetween` from unbounded preceding to current row, over the whole partition, and from current row to unbounded following | Supported | **No explicit `RANGE` frames** | Implicit default frame; `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`; the default frame over the reversed order (with `first` and `last` swapped) | Emulated (§7.11) | P-f / diff |
| WIN-07 † | Order keys: NULL placement, descending, doubles (NaN, ±∞, `-1E19`), strings that look like numbers | Spark's sort order | `NULLS FIRST/LAST` rejected; IRIS's double and string order | The statement's sort rules (§7.1, §7.4, §7.9) inside `OVER` | Emulated (§7.11) | P-f / diff |
| WIN-08 † | Partition keys: case, NULL | Exact values; NULLs together | Collation merges `'a'` and `'A'` | `PARTITION BY %EXACT(g)` | Emulated (§7.1) | P-f / diff |
| WIN-09 | `first`, `last` over default and explicit frames | `last` over an ordered window is the current row's value | Same | `FIRST_VALUE`, `LAST_VALUE` | Same | P-f / diff |
| WIN-10 | Window results in `filter`, `when`, arithmetic, comparisons | Supported (after `withColumn`) | Window in `WHERE` rejected; **inside a `CASE` condition it is silently wrong** | Windows computed in a subquery; expressions read a plain column | Emulated (§7.11) | P-f / diff |
| WIN-11 | Windows after `limit`, after `groupBy().agg()`, after `orderBy` | Window sees the limited rows; ranks over aggregates; output order not defined after a window | Windows run before `TOP` | Wrapped after `TOP`/`DISTINCT`; over aggregates in the grouped query | Emulated (§7.11) | P-f / diff |
| WIN-12 | Ranking without `orderBy`, a frame on `lag` or ranking, `countDistinct` over a window, a window in `filter`/`orderBy`/`agg`, `row_number()` without `over`, frame start after end | Analysis errors | — | `AnalysisError` at the call | Same | diff |
| WIN-13 | `rangeBetween` with value offsets (`rangeBetween(-1, 1)`) or `(currentRow, currentRow)` | Supported | No `RANGE` frames | `UnsupportedOperationError` naming `rowsBetween` or a self-join instead | Unsupported | P-f / diff |
| WIN-14 | `dropDuplicates(subset)`, `drop_duplicates`, `dropDuplicates()` | One row per key (which one is not defined); without a subset, `distinct()` | — | `ROW_NUMBER() OVER (PARTITION BY keys) = 1`; `DISTINCT` without a subset | Emulated (§7.11) | diff |
| WIN-15 | `first`/`last` with `ignorenulls=True`; `rowsBetween` without `orderBy` | Supported | No `IGNORE NULLS`; frame rows arbitrary without an order | `NotImplementedError`, `UnsupportedOperationError` | Unsupported | P-f / doc |

### 2.14 Date and time functions (release 0.2)

The dataset `dates` (§3.2) has a leap day, month ends, the year boundary where ISO weeks differ, `23:59:59.999999` and date text in each of Spark's shapes. Values are compared with Spark's session and process in UTC; timestamp results are `timestamp_ntz` (TIME-05).

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| DATE-01 | `year`, `month`, `dayofmonth`/`day`, `dayofweek`, `dayofyear`, `quarter`, `hour`, `minute`, `second` on dates, timestamps and date text | Text and dates cast first; `hour(date)` is 0; `second` truncates | `SECOND()` rounds the fraction (`.999999` → 60); `HOUR(date)` is wrong | `DATEPART('ss', …)`; dates cast to `TIMESTAMP` for time parts; text through CAST-14 | Emulated (spec §7.12) | P-g / diff |
| DATE-02 | `weekofyear` | ISO 8601 (2027-01-01 is week 53) | `WEEK()` is not ISO (1) | Week of the Thursday of the date's week | Emulated (§7.12) | P-g / diff |
| DATE-03 | `date_add`, `date_sub`, `add_months` with constants and columns, on timestamps and text | Date result; months clamp to the month's last day | `DATEADD` returns a timestamp; clamps the same way | `CAST(DATEADD(…) AS DATE)` | Emulated (§7.12) | P-g / diff |
| DATE-04 | `datediff(end, start)` | Days between the dates (times ignored) | `DATEDIFF('dd', start, end)`: the opposite order | Arguments swapped, both cast to `DATE` | Emulated (§7.12) | P-g / diff |
| DATE-05 | `months_between(a, b, roundOff)` | Whole months for equal days or two month ends, else a 31-day fraction including seconds, rounded to 8 digits | No such function | Spark's formula from date parts | Emulated (§7.12) | P-g / diff |
| DATE-06 | `last_day` | NULL for NULL | `LAST_DAY(NULL)` is a fatal error | NULL guard | Emulated (§7.12) | P-g / diff |
| DATE-07 | `trunc(date, unit)`: year, quarter, month, week (Monday); `'day'` and unknown units | NULL for `'day'` and unknown units | No `TRUNC` for dates | `DATEADD` from date parts | Emulated (§7.12) | P-g / diff |
| DATE-08 | `date_trunc(unit, ts)`: year … millisecond | Unknown units give NULL | No `DATE_TRUNC` | `DATEADD` from date and time parts | Emulated (§7.12) | P-g / diff |
| DATE-09 | `date_format` with `y M d D H h m s S a E Q` and quoted text | Java patterns | `TO_CHAR` fails on timestamps | Built from date parts, padded with `LPAD`; English month and day names | Emulated (§7.12) | P-g / diff |
| DATE-10 | `to_date(s)`, `to_date(s, pattern)` | Strict: `'2026-1-5'` fails with `yyyy-MM-dd`; impossible dates fail | `TO_DATE` is lenient and reads `'2026'` as a day number | Shape checked with `%PATTERN` from the pattern, then `TO_TIMESTAMP` with the translated format; failures raise `MborobakCastError` | Emulated (§7.12) | P-g / diff |
| DATE-11 | `to_timestamp(s)`, `to_timestamp(s, pattern)` | As `to_date`, with time fields | As `TO_DATE` | As DATE-10. Two-digit-year patterns (`yy`) raise `UnsupportedOperationError`: IRIS reads them in the 1900s | Emulated (§7.12) | P-g / diff |
| DATE-12 | `current_date`, `current_timestamp`, `now` | One value per query | `CURRENT_TIMESTAMP` has no fraction | `CURRENT_DATE`, `CURRENT_TIMESTAMP(6)` | Same | P-g / diff |
| DATE-13 | Date functions in filters and grouping keys | — | — | Ordinary expressions | Same | diff |
| DATE-14 | Week-based pattern letters (`w`, `Y`, `u`), `L`, `k`, `K`, zones; `unix_timestamp`, `from_unixtime`, `to_utc_timestamp` | Supported (week letters fail in Spark 3+) | — | `UnsupportedOperationError`, or not offered yet | Unsupported | P-g / doc |

### 2.15 SQL expression strings (release 0.2)

`F.expr("…")`, `df.selectExpr("…", …)` and `df.filter("…")`/`where("…")` take Spark SQL expressions. A parser builds the same nodes as the Column API (`src/mborobak/sqlexpr.py`), so every row above applies to them too. The default column names are Spark's for the SQL text, which differ from the Column API in a few places (`pow(i, 2)`, `LOG(E(), d)`, `between(i, 1, 2)`, `(IF(…))`, frames written `1 PRECEDING`).

| ID | Behavior | PySpark | Mborobak | Status | Evidence |
|----|----------|---------|----------|--------|----------|
| EXPR-01 | Literals: `1` int, `10000000000` bigint, `1L`, `1.5` decimal(2,1), `.5`, `1BD`, `1e3` and `2.0D` double, `'…'` and `"…"` with backslash escapes, adjacent strings joined, `true`, `DATE '…'` | Spark SQL's literal types | Same types; decimal and double constants cast (NUM-14) | Same | diff |
| EXPR-02 | Arithmetic, comparisons (`=`, `==`, `!=`, `<>`, `<=>`), `NOT`, `AND`, `OR`, precedence | Spark's grammar | Same precedence and names (`!=` is `(NOT (a = b))`) | Same | diff |
| EXPR-03 | `IS [NOT] NULL`, `[NOT] IN (…)`, `[NOT] LIKE`, `[NOT] BETWEEN` | — | As the Column API; strings by code point (STR-16) | Same | diff |
| EXPR-04 | `CASE` (searched and simple), `CAST(x AS type)`, `if()`, `\|\|` | — | `CAST` type names: `int`, `bigint`, `long`, `double`, `decimal(p,s)`, `string`, `varchar(n)`, `boolean`, `date`, `timestamp` (as `timestamp_ntz`, TIME-05) | Same | diff |
| EXPR-05 | Function calls (case-insensitive) and aggregates, `count(*)`, `count(DISTINCT x)` | — | Every function of §2.12 to §2.14 by its Spark name | Same | diff |
| EXPR-06 | Windows: `… OVER (PARTITION BY … ORDER BY … [ASC\|DESC] [NULLS FIRST\|LAST] [ROWS\|RANGE BETWEEN …])` | — | As §2.13 | Same | diff |
| EXPR-07 | `filter("…")`, `where("…")` | — | Parsed condition | Same | diff |
| EXPR-08 | `selectExpr("*", "x AS y", "x y", "`a b`")`, `F.expr("x AS y")` | — | Aliases and `*` | Same | diff |
| EXPR-09 | `ILIKE`, `DIV`, bitwise operators, subqueries, `INTERVAL`, `sum(DISTINCT …)`, `count(a, b)`, smallint/tinyint/float literals and types | Supported | `UnsupportedOperationError` (or a parse error for `count(a, b)`) naming the construct | Unsupported | diff |
| EXPR-10 | Malformed text | `ParseException` | `ParseError` (an `AnalysisError`) with the position, at the call | Same | diff |

### 2.16 Writing tables (release 0.2.x)

`df.write.saveAsTable(name, mode)` and `df.write.insertInto(name)` run `CREATE TABLE` and `INSERT … SELECT` inside IRIS, in one transaction ([compiler spec](compiler-spec.md) §9.7, [API contract](api-contract.md) §4.7). The differential tests write the same DataFrame on both sides and read it back with `table()`; Spark's tables are external tables in a temporary directory.

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| WRITE-01 | Round trip of `int`, `bigint`, `double` (NaN, ±∞, `-0.0`, `-1E19`), `decimal`, `string`, `boolean`, `date`, `timestamp_ntz`, and default names such as `(id + 1)` | Same types and values back | `VARCHAR` without a length holds one character; `TIMESTAMP(6)` fails | Column types per compiler spec §9.7; strings as `VARCHAR(3641144)` | Same | P-h / diff |
| WRITE-02 | Strings in a written table: equality, grouping, ordering | Exact, by code point | Case-insensitive by default (`SQLUPPER`) | Columns created `COLLATE EXACT`, so no `%EXACT` rewrite is needed later | Emulated (spec §9.7) | P-h / diff |
| WRITE-03 | Save modes `errorifexists` (default), `ignore`, `append`, `overwrite` (new columns) | As named | — | Same; existence checked case-insensitively | Same | diff |
| WRITE-04 | `append` into an existing table: columns matched by name (any order, any case), types cast by ANSI store assignment (`double` into `int` truncates, numbers into strings) | As described | `8.7` into `INTEGER` gives 8 | Projection with casts before `INSERT` | Same | P-h / diff |
| WRITE-05 | Unsafe types (`string` into `int`) and overflow (`bigint` 2^40 into `int`) | `AnalysisException`; runtime error, nothing written | -104 | `AnalysisError`; `MborobakExecutionError`, nothing written | Same | P-h / diff |
| WRITE-06 | `insertInto`: columns by position; `overwrite=True` replaces the rows | As described | — | `INSERT … SELECT`; `DELETE` first | Same | diff |
| WRITE-07 | `decimal(p,s)` with more than 19 digits before the point (`p − s > 19`), such as `sum` of `decimal(18,2)` | Kept as `decimal(p,s)` | `NUMERIC` holds at most 19 integer digits: precision `19 + s`; larger values fail with -104 | Created as `NUMERIC(p,s)`; reads back as `decimal(19+s, s)` | Known difference: an IRIS limit | P-h / diff |
| WRITE-08 | `overwrite` of a table the DataFrame reads | Error for file-source tables; allowed for Delta | `DROP`, `CREATE`, failing `INSERT`, `ROLLBACK` loses the old rows | Staging table, then drop and rename: allowed, as in Delta | Emulated (spec §9.7) | P-h |
| WRITE-09 | Column names with `.`, `,` or `^` (default names such as `date_add(d, 1)` or `round(x, 2)`), or differing only in case | Written | -1, -306 | `AnalysisError` asking for `alias()` | Unsupported | P-h / diff |
| WRITE-10 | A write that fails part-way (division by zero, overflow, NULL into NOT NULL) | Table creation is undone; a file-source `overwrite` may leave the table emptied; Delta is atomic | DDL and DML roll back | One transaction: the database is as before, as in Delta | Same (as Delta) | P-h |
| WRITE-11 | String length | Unlimited | `VARCHAR` maximum 3641144 characters | `mborobak.write.stringLength`, default 3641144; longer values raise `MborobakExecutionError` | Known difference: an IRIS limit | P-h |
| WRITE-12 | `format()`, `partitionBy()`, `bucketBy()`, `sortBy()`, options; `save()`, `parquet()`, `csv()`, `json()`, the `path` and `mergeSchema` options | Choose the file layout; write files | — | No effect; `UnsupportedOperationError` for the file writers and the two options | Same / Unsupported | doc |

### 2.17 `createDataFrame` (release 0.2.x)

`session.createDataFrame(data, schema=None)` loads local rows or a pandas DataFrame into a temporary IRIS table of the connection ([compiler spec](compiler-spec.md) §9.8, [API contract](api-contract.md) §3.6). Inference and value checks follow PySpark 4.3 (`verifySchema=True`).

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| CDF-01 | Inferred types: `int` → `bigint`, `float` → `double` (NaN, ±∞, `-0.0`), `str`, `bool`, `date`, `Decimal` → `decimal(38,18)`; names `_1`, `_2`, … | As listed | — | Same; rows in a `GLOBAL TEMPORARY` table | Same | P-j / diff |
| CDF-02 | Dicts (keys sorted), `Row`s (field order), lists; a list of names, shorter than the rows | As described | — | Same | Same | diff |
| CDF-03 | Explicit schema (`StructType`, `"a int, b string"`, `"a: int"`, an atomic type such as `"int"`); `Decimal` rounded HALF_UP to the scale; any value into a `string` (`True` → `'true'`, `1e20` → `'1e+20'`); `datetime` into a `date` | As described | — | Same | Same | diff |
| CDF-04 | Rejected input: `int` into `double` or `decimal`, `float` into `int`, `bool` into `int`, out-of-range integers and decimals, `date` into `timestamp_ntz`, wrong row length, `None` in a non-nullable field, empty data or an all-`None` column without a schema, `int` and `float` in one column | `PySparkTypeError` / `PySparkValueError` | — | `TypeError` / `ValueError` at the call | Same | diff |
| CDF-05 | pandas DataFrame: integer columns → `bigint` (any width), floats keep NaN, booleans, strings, an integer column with gaps → `double` | As described | — | Same | Same | diff |
| CDF-06 | A created DataFrame in joins, grouping and comparisons with IRIS tables | Spark's string semantics | — | Strings created `COLLATE EXACT` | Same | diff |
| CDF-07 | `datetime` values without a schema | `timestamp` (with time zone) | — | `timestamp_ntz` (TIME-01); same values | Known difference, as TIME-05 | diff |
| CDF-08 | Missing values (`None`, NaN) in a pandas string column | `'nan'` with pandas 3 (PySpark 4.3 warns it doesn't fully support pandas 3) | — | NULL | Known difference: Mborobak keeps the missing value | diff |
| CDF-09 | Column names IRIS tables can't hold (`a.b`, `c,d`, `a` and `A`) | Allowed | -1, -306 as column names | Allowed: the table's columns are `c1`, `c2`, …, renamed in the query | Same | P-j / diff |
| CDF-10 | `Decimal` values with more than 19 digits before the point (PySpark infers `decimal(38,18)`) | Kept | `NUMERIC` holds at most 19 integer digits (WRITE-07) | `MborobakExecutionError` when loading | Known difference: an IRIS limit | P-h |

### 2.18 Regular expressions (release 0.3)

`rlike`, `regexp_replace`, `regexp_extract`, `regexp_count`, `regexp_substr`, `regexp_instr` and SQL's `RLIKE`/`REGEXP` run in ICU-regex SQL functions Mborobak creates in IRIS on first use ([compiler spec](compiler-spec.md) §7.16).

| ID | Behavior | PySpark | IRIS | Mborobak | Status | Evidence |
|----|----------|---------|------|----------|--------|----------|
| REGEX-01 | `rlike`, `regexp`, `regexp_like`: a match anywhere; anchors, `(?i)`, an empty pattern, a column pattern; NULLs; `~rlike` in a filter | Java regex | No regex in SQL; `NOT (f() = 1)` true for a NULL function result | `$LOCATE` in `Mborobak.RegexpLike1`, wrapped in `CASE` | Emulated (spec §7.16) | P-k / diff |
| REGEX-02 | `regexp_replace`: groups (`$3/$2/$1`), empty matches, `\$`, column pattern, `(?i)` | Java `replaceAll` | — | `%Regex.Matcher.ReplaceAll` | Emulated | P-k / diff |
| REGEX-03 | `regexp_extract`: group, whole match, a group that took no part, no match | `''` for the last two | — | Same | Emulated | diff |
| REGEX-04 | `regexp_count`, `regexp_substr` (NULL without a match), `regexp_instr` (1-based, 0 without a match) | As described | — | Same | Emulated | diff |
| REGEX-05 | SQL text: `RLIKE`, `REGEXP`, `NOT RLIKE`, `regexp_extract(s, p)` (group 1) | As described | — | Same names (`RLIKE(s, p)` for both) | Same | diff |
| REGEX-06 | Invalid pattern; group index beyond the pattern's groups | Error at run time | -149 `<REGULAR EXPRESSION>` | `MborobakExecutionError` | Same | P-k / diff |
| REGEX-07 | `\w`, `\d`, `\b`, `\p{Alpha}` on non-ASCII letters (`é`) | ASCII only | ICU: Unicode | Passed through | Known difference: ICU's classes are Unicode-aware | P-k / diff |
| REGEX-08 | `split`, `regexp_extract_all` | Arrays | — | `UnsupportedOperationError`: no array type | Unsupported | doc |
| REGEX-09 | Functions created on first use (`CREATE FUNCTION` permission), or by `session.installFunctions()` | Nothing to install | — | One lookup per session, then one `CREATE FUNCTION` per missing function | Mborobak only | P-k |

### 2.19 Spark SQL statements (release 0.3)

With `mborobak.sql.dialect = "spark"`, the default since 0.3, `sql()` takes Spark SQL `SELECT` statements and builds them from DataFrame operations ([compiler spec](compiler-spec.md) §7.17). The differential tests run the same statement on both sides, with the datasets as `{t}` placeholders.

| ID | Behavior | PySpark | Mborobak | Status | Evidence |
|----|----------|---------|----------|--------|----------|
| SQL-01 | `SELECT *`, expressions, aliases (`AS` or bare), `CAST`, functions, `RLIKE`, `WHERE`, table aliases; default names | Spark's names and types | DataFrame operations | Same | diff |
| SQL-02 | Joins: `[INNER]`, `LEFT/RIGHT/FULL [OUTER]`, `CROSS`, `LEFT SEMI`, `LEFT ANTI`, comma; `ON`, `USING`; `t.*` | As described | Same | Same | diff |
| SQL-03 | `GROUP BY` expression, select alias, ordinal, `ALL`; `HAVING`; global aggregates; aggregates of expressions; `ORDER BY` an aggregate alias | As described | Keys and aggregates computed once, rewritten (spec §7.17) | Same | diff |
| SQL-04 | `ORDER BY` alias, ordinal, `NULLS LAST`, a column the select drops; `DISTINCT`; `LIMIT` | As described | `orderBy` before `select` | Same | diff |
| SQL-05 | `UNION ALL`, `UNION`, `UNION DISTINCT` with `ORDER BY` and `LIMIT` | As described | `union` + `distinct` | Same | diff |
| SQL-06 | `WITH` (several, one using another), subqueries in `FROM`, a CTE joined with itself | As described | Same | Same | diff |
| SQL-07 | `?` and `:name` parameters | Literal values | Same | Same | diff |
| SQL-08 | `CREATE`, `INSERT`, `SELECT` without `FROM`, `EXCEPT`, `OFFSET`, window functions in an aggregating select | Supported | `UnsupportedOperationError` | Unsupported | diff |
| SQL-09 | A column neither grouped nor aggregated; an unknown column; a `GROUP BY` ordinal out of range | `AnalysisException` | `AnalysisError` | Same | diff |

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
| `dates` | Eight rows: 2024-02-29, month ends, 2026-12-31 / 2027-01-01 (ISO weeks), 2021-01-03, `23:59:59.999999`, `.5` seconds, NULL; date text in each of Spark's shapes and a `dd/MM/yyyy HH:mm` column (§2.14) |
| `wins` | Twelve rows in five partitions (`'a'`, `'A'`, `'b'`, `'10'`, `'9'`, NULL) with tied order keys, NULL keys, NaN, ±∞, `-1E19` and `1E300` (§2.13) |
| `funcs` | Strings with empty values, spaces, a tab, accents and numeric-looking text; doubles with NaN, ±∞, `1E300`, `-1E-300` and `.5` ties; decimals around rounding ties; integers and longs with NULLs (§2.12) |

### 3.3 Keeping the matrix and tests in step

A check in CI reads the IDs from this document and the test names from the suite. It fails when a row has no test, when a test names an ID that doesn't exist, or when a row is marked `diff` but its test is skipped.

## 4. Summary for users

For the README and the docs site, the P0 known differences fit in one short list:

- Integer and decimal overflow in arithmetic and text casts are not detected (NUM-04 to NUM-06, CAST-08).
- Decimal division keeps about 19 significant digits, fewer than Spark's widest decimal results (NUM-13).
- `max` ignores NaN (FP-06).
- The `DOUBLE` value `-1E19` groups together with NULL (FP-10).
- `like` has no escape character (STR-14).
- Adding `+∞` and `-∞` raises an error instead of giving NaN (FP-12); `pow` overflow raises an error instead of giving infinity (FUN-24); integer overflow in `abs` and `round` is not detected (FUN-25).
- `initcap` also starts words after tabs (FUN-22); `upper('ß')` stays `'ß'` (FUN-23).
- SQL expression strings don't support `ILIKE`, `DIV`, bitwise operators or subqueries (EXPR-09).
- Regular expressions use ICU's syntax: `\w`, `\d` and `\p{Alpha}` also match non-ASCII letters (REGEX-07); `split` and `regexp_extract_all` are not supported (REGEX-08). Their SQL functions are created in the schema `Mborobak` on first use (REGEX-09).
- `rangeBetween` with value offsets is not supported, because IRIS has no `RANGE` frames (WIN-13); `first`/`last` with `ignorenulls=True` neither (WIN-15).
- Casting `double` and `decimal` values to string gives IRIS's text, not Spark's (CAST-12, CAST-13).
- `union` is stricter about mixing types (UNI-03).
- Timestamps have no time zone (TIME-01); `to_timestamp`, `date_trunc` and `current_timestamp` return `timestamp_ntz` (TIME-05).
- `session.sql()` takes Spark SQL `SELECT` statements (§2.19), not other statements; with `mborobak.sql.dialect = "iris"` it takes IRIS SQL, where `GROUP BY 1` means the constant 1 (API-01, GRP-01).
- With `stringSemantics="iris"`, string comparison, grouping and sorting follow IRIS's case-insensitive rules (§2.1).
- `createDataFrame` infers `timestamp_ntz` for `datetime` values (CDF-07) and makes missing pandas strings NULL (CDF-08).
- Writing tables: decimals keep at most 19 digits before the point (WRITE-07); column names with `.`, `,` or `^`, which some default names contain, need an alias (WRITE-09); file writers (`save`, `parquet`, `csv`) are not supported (WRITE-12).

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
| M6 | ~~Re-check all IRIS evidence on build 154~~ Withdrawn: all evidence is on IRIS 2026.2, the D1 build (scope 1.9) | — |

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
