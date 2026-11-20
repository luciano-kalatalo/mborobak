SCIENTIFICLOUD · PRODUCT SCOPE · VERSION 1.13 · 2026-11-20

# Mborobak

**Python DataFrame Engine for InterSystems IRIS**

Scope document covering functionality, architecture, implementation and validation.

### Revision history

| Version | Date       | Changes |
| ------- | ---------- | ------- |
| 1.0     | 2026-11-18 | Initial version. |
| 1.1     | 2026-11-18 | Lazy evaluation and schema lookups (§5.2); `union()` semantics aligned with PySpark; wider P0 list (actions, schema, expressions, join types); new section on SQL compiler requirements for IRIS (§6); parameter policy; minimum IRIS/Python versions as a prerequisite; working prototype in the Architecture phase; new risks; open decisions section (§15). |
| 1.2     | 2026-11-18 | Changes from the [research review](research-review-2026-11.md): required vs. optional rewrites and duplicate-count tests (§6.10); compiled-SQL cache and separate Python-side timing (§7); targeted test data, edge-case checklists and a numeric tolerance policy (§8.2); data skew parameters and benchmark record keeping (§8.3); IRIS behavior study in Phase 1 (§9, §15); new decision D9, the PySpark reference version and ANSI mode (§15). |
| 1.3     | 2026-11-18 | Changes from the [IRIS behavior study](iris-behavior-study.md) on IRIS 2026.2: concrete ordering and limit rules (§6.2, §6.3); parameter policy corrected and the ~999 literal/parameter limit added (§6.4); measured type and NULL differences with required guards (§6.8); string collation rule (§6.11); constructs the compiler must never emit (§6.12); new decision D10 on string semantics; D1 proposed as IRIS 2026.2; new risk on silent wrong results (§12). |
| 1.4     | 2026-11-18 | Decided D1 (minimum IRIS 2026.2 GA) and D9 (PySpark 4.3.x, ANSI mode on); §6.8 and §8.2 updated to match. |
| 1.5     | 2026-11-18 | Decided D2 (Python 3.11+), D3 (driver 5.4+), D4 (`union()` keeps duplicates), D5 (100 ms threshold), D6 (MIT), D7 (rewrite the core, port reviewed IRISpark parts) and D10 (PySpark string behavior by default); §6.11, §7, §8.3 and §16 updated to match. Only D8 remains open. |
| 1.6     | 2026-11-18 | Project renamed from **Fluxora** to **Mborobak** (API names follow, e.g. `MborobakSession`). D8 reopened for the new name. |
| 1.7     | 2026-11-18 | Added the InterSystems contest requirements (§1.1). D1 changed to the required IRIS build (2026.3.0AI.154.0, AI Hub Community image). |
| 1.8     | 2026-11-18 | §6.8 aligned with compiler spec draft 0.2: divisions need no guard (IRIS's error names the cause); new `DOUBLE` sorting rule for IRIS's `-1E19` NULL marker. |
| 1.9     | 2026-11-19 | D1 changed back: IRIS 2026.2 (build 221U) for development, testing and submission; build 154 will not be used. §1.1 item 6 marked as a known deviation from the contest rules. |
| 1.10    | 2026-11-19 | Release 0.3 gains a concrete item: columnar (Arrow-style) result transfer, after the benchmark showed per-row conversion dominates large results (§13). |
| 1.11    | 2026-11-19 | Release 0.2 reordered for proofs of concept with Databricks users, following the [POC readiness plan](poc-readiness-plan.md): notebook scanner, column functions, window functions, SQL-string expressions first (§13). |
| 1.12    | 2026-11-20 | Release 0.2.0 shipped with items 1 and 2 of §13 (tier 1 of the POC readiness plan); item 3 becomes the 0.2.x releases. Exit criterion met: 91% of in-scope calls on public Databricks notebooks. |
| 1.13    | 2026-11-20 | The table write API (P1) is built for 0.2.x: `saveAsTable` and `insertInto`, inside IRIS and in one transaction (§13). |

---

## 1. Executive summary

Mborobak is a Python library for declarative data processing that uses InterSystems IRIS to store data and to plan and run queries.

It offers a programming experience similar to Snowpark and PySpark: developers build operations on DataFrames without writing SQL. The library turns those operations into logical plans, generates SQL for InterSystems IRIS and hands execution to the database.

**Value proposition:** let Python developers use the analytical capabilities of IRIS through a modern DataFrame API, without the operational complexity of an Apache Spark environment.

Mborobak does not aim to reproduce Spark's distributed architecture or to replace the IRIS SQL optimizer.

### 1.1 Delivery constraints: InterSystems contest

Mborobak will be submitted to an InterSystems Open Exchange contest. Its requirements take precedence over the rest of this document where they conflict:

1. **Fully functional.** The library must work end to end, not be a design or a thin wrapper around an existing library in another language. It must not be a copy of an existing application or library; IRISpark code is ported only after review and the core is rewritten (D7).
2. **New or significantly improved.** Accepted entries are new to Open Exchange, or existing ones with a significant improvement. The organizers review every entry before approving it.
3. **Open source on GitHub or GitLab.** Published under the MIT license (D6).
4. **README in English,** with installation steps and either a video demo or a description of how the library works.
5. **At most 3 submissions** per developer or team.
6. **Required IRIS build:** 2026.3.0AI build 154, the AI Hub Community Docker image from the InterSystems Early Access Program portal (not build 162). **Known deviation (decided 2026-11-19, D1):** Mborobak is developed, tested and submitted on IRIS 2026.2 (build 221U); build 154 will not be used. The organizers may treat this as not meeting the requirement. Files: `iris_arm64-community-2026.3.0AI.154.0-docker.tar.gz` (ARM64, including Apple Silicon) or `iris-community-2026.3.0AI.154.0-docker.tar.gz` (x64). The download requires the developer's own portal login.

## 2. References and positioning

| Reference          | Role in the project |
| ------------------ | ------------------- |
| Snowpark           | Main benchmark for architecture, features and developer experience |
| PySpark            | Reference for syntax and semantic compatibility |
| InterSystems IRIS  | Initial execution backend |
| Native IRIS SQL    | Baseline for measuring overhead and execution efficiency |

Snowpark evaluates DataFrames lazily: operations are generated and run in Snowflake only when an action is called. This is Mborobak's reference model.

## 3. Product goals

**General goal**

Build and release a Python library for writing data transformation and analysis pipelines as declarative operations that run mostly inside InterSystems IRIS.

**Specific goals**

1. Provide an intuitive DataFrame API, inspired by Snowpark and compatible with a documented subset of PySpark.
2. Implement lazy evaluation and an intermediate representation (IR) of transformation plans.
3. Translate DataFrame operations into native IRIS SQL.
4. Minimize data transfer between the database and the Python application.
5. Let users inspect logical plans and generated SQL before execution.
6. Measure performance, resource use and the efficiency of generated queries.
7. Keep the architecture extensible, so other backends remain possible later.

## 4. Conceptual architecture

```
Python Application        MborobakSession · DataFrame · functions
        │
Mborobak API & Logical Plan   Expressions · Projections · Filters · Joins · Aggregations
        │
Analyzer                  Name and type resolution · Catalog (metadata)
        │
SQL Compiler              Validation · IRIS rewrites · SQL · Parameters
        │
InterSystems IRIS         SQL Optimizer · Execution · Storage
        │
Python Result             collect() · show() · toPandas() · count()
```

Remote execution uses the official IRIS Python driver (DB-API, package `intersystems-irispython`). Data is only materialized when an action is called.

### Architectural components

| Component           | Responsibility |
| ------------------- | -------------- |
| Session Manager     | Connections, configuration and lifecycle |
| DataFrame API       | Public interface for transformations and actions |
| Expression Engine   | Typed representation of expressions |
| Logical Planner     | Builds operation plans (immutable IR) |
| Analyzer / Catalog  | Resolves columns, types and ambiguities; caches table metadata |
| SQL Compiler        | Translates plans into the IRIS dialect, including required rewrites (§6) |
| Execution Adapter   | Execution, parameters, results, type conversion and errors |
| Diagnostics         | Shows SQL, plans, metrics and query counts |
| Compatibility Layer | PySpark compatibility contracts |

The first version uses remote Python connectivity over DB-API. Embedded Python may be investigated later for server-side execution. The IRIS ecosystem supports both, with different characteristics.

## 5. MVP functional scope

P0 = required for the MVP; P1 = planned next; P2 = research or future version.

### 5.1 Features

| Area | Feature | Priority |
| ---- | ------- | -------- |
| Session | `MborobakSession.builder`, `table()`, `sql()` | P0 |
| Transformations | `select()`, `filter()`, `where()` | P0 |
| Transformations | `withColumn()`, `withColumnRenamed()`, `drop()`, `alias()` | P0 |
| Transformations | `orderBy()`/`sort()`, `limit()`, `distinct()` | P0 |
| Transformations | `union()`/`unionAll()` (PySpark semantics: `UNION ALL` by position) | P0 |
| Transformations | `unionByName()` | P1 |
| Joins | `join()` with `inner`, `left`, `right`, `full`, `cross`; keys by name (`on="id"` or a list) or by expression | P0 |
| Joins | `left_semi`, `left_anti` | P1 |
| Aggregations | `groupBy().agg()`, `count`, `sum`, `avg`, `min`, `max`, `countDistinct` | P0 |
| Expressions | `col()`, `lit()`, `when().otherwise()`, `df["col"]` | P0 |
| Expressions | Arithmetic, comparison, logical (`&`, `\|`, `~`), `cast()`, `isNull()`, `isNotNull()`, `isin()`, `like()`, `between()`, `asc()`/`desc()` | P0 |
| Actions | `collect()`, `show()`, `toPandas()`, `count()`, `take()`, `first()`, `head()` | P0 |
| Schema | `schema`, `columns`, `dtypes`, `printSchema()` | P0 |
| Diagnostics | `explain()` (logical plan + generated SQL + parameters) | P0 |
| Diagnostics | Per-action metrics: client/server time, rows, number of queries | P0 |
| Data | `createDataFrame()` (release 0.2.x) | P1 |
| Analytics | Window functions and `row_number()`, `rank()`, `lag()`, `lead()` | P1 |
| Analytics | Date and string functions (documented subset) | P1 |
| Writing | Table write API (`saveAsTable`, `insertInto`; release 0.2.x) | P1 |
| Execution | Python UDFs and UDAFs | P2 |
| Execution | Vectorized execution | P2 |
| Storage | Parquet/MinIO integration | P2 |
| Backends | Multiple backends | P2 |

### 5.2 Lazy evaluation and schema lookups

- **Transformations** (`select`, `filter`, `join`, etc.) only build the plan and never run data queries.
- **Actions** (`collect`, `show`, `toPandas`, `count`, `take`, `first`, `head`) compile the plan and run **exactly one data query**, unless a documented exception applies.
- **Schema lookups** may run before an action when the plan needs to resolve names or types: `table()`, `select("*")`, `drop()`, `withColumn()` replacing a column, `columns`/`schema`, and joins by column name. They:
  - read only the catalog (`INFORMATION_SCHEMA` or equivalent), never data;
  - are cached per session, with explicit invalidation (`session.catalog.refresh()` or equivalent);
  - are counted separately from data queries in the diagnostics metrics.
- The schema of arbitrary `sql()` is obtained by preparing the statement or reading its metadata, without fetching rows.

### 5.3 Expected usage example

```python
import os
from mborobak import MborobakSession
from mborobak.sql import functions as F

session = (
    MborobakSession.builder
    .config("host", os.environ["IRIS_HOST"])
    .config("port", int(os.environ.get("IRIS_PORT", 1972)))
    .config("namespace", os.environ.get("IRIS_NAMESPACE", "USER"))
    .config("user", os.environ["IRIS_USER"])
    .config("password", os.environ["IRIS_PASSWORD"])
    .getOrCreate()
)

sales = session.table("Sales")

result = (
    sales
    .filter(F.col("status") == "PAID")
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.col("revenue").desc())
)

result.explain()   # plan + SQL + parameters, without running anything
result.show()      # runs one data query
```

This is the intended API, not a claim that these operations already exist. Credentials come from environment variables or secure configuration, never from source code. Importing `functions as F` avoids hiding Python's built-in `sum`, `min` and `max`.

## 6. SQL compiler requirements for IRIS

The compiler specification must detail these requirements and cover them with tests:

1. **Composition through subqueries.** Every plan node compiles on its own and can be combined as a subquery. The compiler flattens trivial subqueries so the SQL stays readable and close to hand-written SQL.
2. **Ordering.** IRIS rejects `ORDER BY` inside a subquery, a CTE or one arm of a `UNION` unless `TOP` is present. `ORDER BY` is emitted only at the outermost level. Where an order must survive inside a subquery (for example `orderBy().limit()` followed by more transformations), the compiler emits it together with `TOP n` or `TOP ALL`, and still re-applies the final ordering at the outermost level. Ordering must never be lost silently.
3. **Limits and paging.** Inside subqueries, `limit()` compiles to `TOP n` (IRIS rejects `LIMIT` there). At the outermost level, `TOP`, `LIMIT n` and `LIMIT n OFFSET m` all work; an offset is only possible at the outermost level.
4. **Parameter policy.** User-supplied values (`lit()`, comparisons, `isin()`) are sent as `?` parameters, for safety against SQL injection. Parameters work in `WHERE`, `IN`, `LIKE`, `HAVING`, `TOP`, `LIMIT`, the `SELECT` list, and in expressions repeated in `SELECT` and `GROUP BY`. IRIS caches plans by turning literals into parameters itself, so parameters don't affect plan reuse. Identifiers are never parameterized; they are validated and quoted. **Limit:** a statement may hold about 999 literals and parameters in total; beyond that IRIS fails with a misleading error. The compiler counts them and raises a clear Mborobak error before the limit (for example for a long `isin()` list).
5. **Identifier resolution.** Define handling of case (IRIS identifiers are case-insensitive), quoted identifiers, reserved words and the default schema (`SQLUser`) for unqualified names in `table()`.
6. **Joins and ambiguity.** Rules for same-named columns after a join, key deduplication in `join(on="id")` (as in PySpark), `df["col"]` vs. `col("col")` references, self-joins via `alias()`, and an explicit error for ambiguous references.
7. **Composing `sql()`.** User SQL is wrapped as a subquery. An inner `ORDER BY` follows rule 2. Columns are resolved as in §5.2.
8. **Types and NULL.** The measured differences are listed in the [IRIS behavior study](iris-behavior-study.md) §2.6–2.7 and go into the compatibility matrix. Required handling:
   - **Division:** IRIS stops the whole query with a fatal error on division by zero. Following the reference's ANSI mode (D9), Mborobak raises its own clear division-by-zero error. IRIS's error message names the cause (`<DIVIDE>`), so divisions need no guard: the execution adapter turns the fatal IRIS error into a Mborobak error ([compiler spec](compiler-spec.md) §7.5).
   - **Casts:** IRIS returns 0 for an invalid string-to-number cast and doesn't reject numeric overflow. Under ANSI mode both are errors in Spark. String-to-number casts get a validity check that raises a Mborobak error. Numeric overflow raises an error where the compiler can check it cheaply; otherwise it is a documented known difference.
   - **NULL ordering:** IRIS's default (NULL first ascending, last descending) matches Spark, but there is no `NULLS FIRST`/`NULLS LAST` syntax. Explicit `nulls_first()`/`nulls_last()` compile to an extra `CASE WHEN x IS NULL …` sort key.
   - **`DOUBLE` sorting:** IRIS uses `-1E19` as an internal NULL marker when it sorts or groups `DOUBLE` values, and sorts `-Infinity` after finite values. `DOUBLE` sort keys get an extra bucket key, which restores Spark's order ([compiler spec](compiler-spec.md) §7.9). Grouping on a `-1E19` value is a documented known difference until a fix is verified.
   - **Empty string:** `''` and `NULL` are distinct in IRIS, as in Spark; no special handling.
   - **Result types:** IRIS returns `Decimal` where Spark returns `DOUBLE` (for example `7/2`, `AVG` of integers), and the driver returns `DATE` as a string. Results are converted using the DataFrame's schema, not the driver's types.
   - **Timestamps:** IRIS `TIMESTAMP` has no time zone; Mborobak treats it as a timestamp without time zone.
9. **Untranslatable operations.** Raise an explicit error at compile time; never fall back silently to running on the client.
10. **Required vs. optional rewrites.** Every rewrite is labeled either *required translation* (it defines the query's meaning) or *optional optimization* (for example subquery flattening or predicate placement), which may change performance but never results. Each optional rewrite can be switched off, and the test suite runs with optional rewrites on and off and compares results. Any rewrite that can change duplicate counts (`union`, join-key deduplication, `distinct` placement, pre-aggregation) needs its own dedicated test.
11. **String collation.** IRIS string columns use case-insensitive collation by default, which ignores trailing spaces. It affects `=`, `IN`, `LIKE`, joins, `GROUP BY`, `DISTINCT`, `UNION`, `ORDER BY` and `MIN`/`MAX`, and grouping, `DISTINCT`, `MIN` and `MAX` return uppercased values. Wrapping a column in `%EXACT(...)` gives Spark's behavior. Following D10, the compiler does this by default wherever collation affects the result (comparisons, joins, grouping, `DISTINCT`, `UNION`, sorting, `MIN`/`MAX`); a session setting switches to IRIS's behavior. Columns already declared `COLLATE %EXACT` are left as they are, which requires reading each column's collation from the catalog. The benchmark measures what `%EXACT` costs (lost index use).
12. **Constructs never emitted.** `GROUP BY` with column positions (IRIS treats `GROUP BY 1` as the constant 1); `IS NULL` directly on an expression in the `SELECT` list (use `CASE WHEN … IS NULL`); `NaN` or infinity as SQL literals (send them as parameters); `%PARALLEL` after the table name (it goes right after `FROM`).

## 7. Non-functional requirements

| Dimension       | Requirement |
| --------------- | ----------- |
| Performance     | Minimal overhead over equivalent native SQL; compiled SQL cached per plan shape, so repeating a pipeline skips analysis and compilation |
| Security        | Parameterized queries (§6.4), validated identifiers, respect for IRIS permissions |
| Portability     | Pure Python 3.11+, no Spark/JVM dependency; official `intersystems-irispython` driver 5.4+ |
| Compatibility   | Minimum IRIS version and supported Python versions declared and tested in CI (§15) |
| Predictability  | Deterministic plans and SQL for equivalent operations |
| Observability   | Generated SQL, parameters, timings and query counts (data and schema lookups); Python-side time (plan building, compilation, schema lookups) reported separately from IRIS time |
| Robustness      | Explicit handling of types, NULL and errors, with messages that point to the originating DataFrame operation |
| Testability     | Compiler unit tests without IRIS; integration tests against IRIS in a container |
| Distribution    | Installable, versioned Python package (SemVer) |

The library must not assume administrator privileges, change IRIS configuration automatically or run arbitrary Python expressions on the server.

## 8. Benchmarking

Three independent evaluations, so different systems are never compared as if they shared one execution environment.

### 8.1 Feature benchmark: Snowpark

Compare features between Snowpark and Mborobak: developer experience, semantics, lazy plans, expressions, aggregations, analytical functions and inspection tools. Each item is rated full, partial, unsupported or not applicable.

### 8.2 Compatibility benchmark: PySpark

A differential test suite over identical datasets, checking:

- matching results and schemas;
- handling of NULL, empty strings, casting and decimal precision;
- semantics of joins (including duplicate columns), aggregations and `union`;
- behavior of date, string and window functions (from release 0.2);
- cases where syntax matches but semantics differ.

The reference is **PySpark 4.3.x with ANSI mode on** (D9), Spark 4's default. Under ANSI mode, overflow, invalid casts and division by zero raise errors instead of returning NULL. The reference moves to Spark 4.5, the planned 4.x long-term-support release, once it ships. IRISpark's comparison results were recorded against PySpark 3.5.8 and must be re-run before reuse.

Besides the shared datasets, the suite includes:

- **Targeted test data:** for each compiler rule, small datasets built to expose the errors that rule could cause. Tools that generate such data automatically (for example LASSO) may be evaluated, but are not required.
- **Edge-case checklists:**
  - NULL in comparisons, filters and join keys;
  - HAVING (`groupBy().agg().filter()`) with empty input, groups whose aggregate is NULL, all-NULL groups, duplicate rows and boundary comparisons;
  - Spark-matching pitfalls: time zones, −0.0 vs. 0.0, NaN ordering and equality, collations, decimal AVG precision, join output order.

**Numeric tolerance policy:** integer and decimal results must match exactly. Floating-point results are compared with a documented relative tolerance, because execution order (for example in parallel execution) can change the last digits.

Full PySpark compatibility will not be claimed without matching test coverage.

### 8.3 Performance benchmark: native IRIS

Mborobak versus equivalent native SQL, on the same IRIS instance and through **the same DB-API driver**.

| Metric    | Description |
| --------- | ----------- |
| Latency   | Total time and server time |
| Throughput| Rows processed per second |
| Overhead  | Extra time in the Python layer (plan building, compilation, schema lookups, result conversion) |
| Transfer  | Rows and bytes received |

The initial suite covers filters, projections, joins, aggregations, sorting and chains of transformations. Window functions join with release 0.2.

Each scenario uses repeated runs, warm-up, cache control (including Mborobak's metadata cache and IRIS cached queries), a recorded SQL plan and a check that results match. The initial dataset is synthetic and reproducible (versioned generator with a fixed seed), at 10 thousand, 100 thousand and 1 million rows, scaled up as lab capacity allows. The generator takes skew and cardinality as parameters, so scenarios cover uneven key distributions and not only uniform data.

Every benchmark run records its environment: IRIS version and build, Mborobak commit, Python and driver versions, CPU model, core pinning, IRIS parallel-query settings, warm-up runs and the number of measured runs. Raw per-run data is published alongside the summaries. Results are reported per scenario family (filter, join, aggregation, chain), never as a single global number. Time is broken down into IRIS execution, network fetch, conversion to Python objects and pandas DataFrame construction.

IRISpark's past results are kept as a baseline, but measurements will be redone with a documented configuration.

**Proposed initial target:** median overhead of at most 10% over equivalent native SQL for substantial queries (server time ≥ 100 ms, D5), measured end to end through the same access path. For small queries, absolute overhead (ms) is reported separately. This is an acceptance criterion to validate, not demonstrated performance.

## 9. Development method: waterfall with a validation prototype

| Phase | Main deliverables | Exit criterion |
| ----- | ----------------- | -------------- |
| 1. Requirements | Functional spec, Snowpark/PySpark matrix, IRIS behavior study (§15), open decisions resolved (§15) | Scope and semantics approved |
| 2. Architecture | C4, ADRs, IR model, compiler spec, API contract; **working prototype** `table → filter → groupBy → agg → orderBy → collect` against a real IRIS | Technical design approved and prototype producing correct SQL |
| 3. Implementation | DataFrame core, analyzer, SQL compiler and IRIS adapter | P0 operations implemented |
| 4. Integration | Tests against IRIS in a container, reproducible schemas and data | Integration suite passing |
| 5. Benchmarking | Performance and compatibility report | Acceptance criteria verified |
| 6. Release | Package, documentation, examples and changelog | Release candidate approved |

Each phase produces versioned artifacts and goes through formal review before moving on. The Phase 2 prototype exists to validate the IR model before it is frozen; it is not production code.

## 10. MVP acceptance criteria

Sign-off checklist (a tracking tool; it does not reflect current implementation status):

- [ ] Every P0 operation has automated tests.
- [ ] Results match native SQL for the reference suite, within the numeric tolerance policy (§8.2).
- [ ] The test suite passes with optional rewrites both on and off (§6.10).
- [ ] Known semantic differences from PySpark are documented in the compatibility matrix.
- [ ] Generated SQL and parameters can be inspected before execution.
- [ ] Lazy transformations trigger no data queries; schema lookups follow §5.2 and are counted.
- [ ] Each action runs exactly one data query, unless a documented exception applies.
- [ ] External values are sent as parameters, with the exceptions documented in §6.4.
- [ ] Documentation covers installation, connectivity, examples and the compatibility matrix.
- [ ] A reproducible benchmark is published with a documented environment and raw per-run data.
- [ ] Python packaging is validated in a clean environment on every supported Python version.

## 11. Out of scope for the first version

The first version does not include: its own distributed processing cluster; Spark RDD or Structured Streaming compatibility; a standalone vector engine; pipeline orchestration; a graphical interface; distributed machine learning; replacing the IRIS SQL optimizer; silent fallback to client-side execution.

There is also no generic multi-database layer in the MVP. The architecture must allow one later, but the first implementation is optimized for IRIS.

## 12. Main risks

| Risk | Mitigation |
| ---- | ---------- |
| Semantic differences between PySpark and IRIS (NULL, empty string, division, decimals) | Compatibility matrix and differential tests (§8.2) |
| Silent wrong results from IRIS defaults: case-insensitive string comparison, uppercased grouping results, invalid casts returning 0 | `%EXACT` handling (§6.11, D10), cast guards (§6.8), tests with mixed-case, trailing-space and invalid-number data |
| Column ambiguity in joins and self-joins | Dedicated analyzer with specified rules (§6.6) and explicit errors |
| Ordering lost in subqueries | Propagation rule in the compiler (§6.2) and targeted tests |
| Overly complex generated SQL | Subquery flattening and inspection via `explain()` |
| Schema lookups breaking the laziness expectation | Explicit policy and caching (§5.2) |
| Operations that cannot be translated to SQL | Explicit errors; fallback only if authorized and measured |
| High overhead on small operations | Measure client, network and server separately; report absolute overhead |
| Driver speed when transferring results | Measure Python/pandas conversion separately; evaluate batched fetching |
| SQL feature differences across IRIS versions | Declared minimum version and CI against it |
| IR design flaws found late | Working prototype in Phase 2 |
| Scope creep | P0/P1/P2 priorities and a requirements freeze |
| The name Mborobak is unavailable | Check trademark, repository and PyPI in Phase 1 |

## 13. Product roadmap

**Release 0.1: DataFrame Core.** Declarative API, lazy evaluation, analyzer, SQL compiler, IRIS execution, diagnostics and P0 benchmark.

**Release 0.2: Analytical Functions (POC readiness).** Ordered by what PySpark users on Databricks reach for first ([POC readiness plan](poc-readiness-plan.md)). Release 0.2.0 (2026-11-20) delivers items 1 and 2; item 3 follows in 0.2.x releases:

1. A notebook compatibility scanner, to measure a client's code against what Mborobak supports.
2. Null, string and math functions; window functions; date and time functions; SQL-string expressions (`F.expr`, `selectExpr`, `filter("…")`); `dropDuplicates`.
3. Then table writes, `createDataFrame`, `unionByName`, semi/anti joins and named `sql()` parameters (all done in 0.2.x), and planner improvements.

Exit criterion: at least 80% of the calls in representative Databricks notebooks supported, each new function with a PySpark comparison row. Met by 0.2.0: 91% of in-scope calls on the public notebook set, 93% on the sample notebook.

**Release 0.3: Advanced Execution.** Research and prototypes for UDF/UDAF, vectorized execution, external storage and parallelism, and:

- **Regular expressions** (`rlike`, `regexp_replace`, `regexp_extract`, done): IRIS SQL has no regex functions, so Mborobak creates ICU-regex SQL functions (`CREATE FUNCTION … LANGUAGE OBJECTSCRIPT`) on first use (POC readiness plan, tier 3; compiler spec §7.16).
- **Columnar (Arrow-style) result transfer.** Today the IRIS driver returns rows, and every value becomes a Python object before pandas copies it again. At 1M rows of four columns this costs about 1.7 s, against 0.85 s for the raw driver ([benchmark report](benchmark-report-2026-11-18.md) §5). PySpark's Arrow path cut the same kind of transfer from about 20 s to 0.7 s in published tests, because whole columns arrive as typed memory blocks that pandas uses with little or no copying. The goal is a columnar path for `toPandas()` (and an Arrow result for other tools such as Polars and DuckDB). Candidate routes, to be evaluated in this order:
  1. Check whether the IRIS Python driver can return results in a columnar or Arrow form; if so, use it.
  2. An opt-in server-side component (Embedded Python) that runs the query next to the data and returns Arrow batches. Because the library must not install server-side code by itself (§7), administrators would enable it explicitly.
  3. A Parquet export read by pandas, only for very large results, where the file's latency pays off.

  Success measure: `toPandas()` on 1M rows close to the raw driver's time or better, with the same values and types as today.

**Release 1.0: Production Readiness.** Stable API, documented compatibility, robustness testing, observability, security and an operations guide.

## 14. Expected outcome

At the end of the MVP, Scientificloud will have an installable Python library for building analytical queries with DataFrames and running them on IRIS, with correct results, documented behavior, measured performance and no Spark dependency.

The project succeeds if Mborobak shows three properties: **usability on par with a modern DataFrame API, efficient translation to native SQL, and room to evolve without destabilizing the core.**

The strategy is to give InterSystems IRIS an experience comparable to what Snowpark gives Snowflake, without assuming the two databases match in features, infrastructure or performance.

## 15. Open decisions (resolve in Phase 1)

| # | Decision | Proposal |
| - | -------- | -------- |
| D1 | IRIS version | **Decided (2026-11-19):** InterSystems IRIS 2026.2 (build 221U) for development, testing and submission. All measurements (behavior study, probes, test suites, benchmark) are on this build. The contest's required build 154 (§1.1) will not be used; this is an accepted deviation |
| D2 | Supported Python versions | **Decided:** Python 3.11 and newer (3.10 reached end of life in October 2026), tested in CI |
| D3 | Driver | **Decided:** official `intersystems-irispython` DB-API driver, version 5.4 or later |
| D4 | `union()` semantics | **Decided:** same as PySpark (`UNION ALL` by position); explicit `distinct()` to deduplicate |
| D5 | "Substantial query" threshold for the benchmark | **Decided:** server time ≥ 100 ms |
| D6 | Package license | **Decided:** MIT, as IRISpark |
| D7 | IRISpark migration | **Decided:** rewrite the plan model, analyzer and compiler from the specifications. Port reviewed IRISpark parts: type mappings, the function list, test cases, the comparison harness and the lessons-learned record |
| D8 | Name availability | **Checked 2026-11-18 for "mborobak", partly open.** PyPI: `mborobak` and `mborobak-iris` are free. GitHub: no account and no repository uses the name. Web: no existing use found; the closest trademarks found are different words ("MOMBAK" in the US, "ROBOBAK" in Canada). The official registers (INPI, USPTO, EUIPO) were not searched directly. **Remaining:** a formal register search, ideally by a trademark professional, before the public release |
| D9 | PySpark reference for the compatibility matrix | **Decided:** PySpark 4.3.x with ANSI mode on; move to 4.5 LTS once released |
| D10 | Default string semantics | **Decided:** PySpark's (case-sensitive, via `%EXACT`), with a session setting for IRIS's behavior. See §6.11 |

### IRIS behavior study (Phase 1)

**Status:** first pass done on IRIS 2026.2; results in [iris-behavior-study.md](iris-behavior-study.md), with open points listed in its §4.

Several decisions and compiler rules depend on how IRIS actually behaves. Phase 1 answers these questions from IRIS documentation and a small spike against a real instance at the minimum version (D1):

- ordering of subquery results with and without `TOP` (§6.2);
- `TOP` vs. `LIMIT`/`OFFSET` support (§6.3);
- how parameters interact with `GROUP BY`, and how cached queries treat literals (§6.4);
- identifier case, quoting and default-schema resolution (§6.5);
- empty string vs. `NULL`, integer division, decimal precision and `NULL` ordering (§6.8);
- handling of −0.0, NaN and time zones;
- parallel query behavior and whether it changes floating-point aggregate results;
- result fetch speed of the `intersystems-irispython` driver for 10k–1M rows.

The findings go into the compiler specification and the compatibility matrix.

## 16. Recommended architectural decision

Rewrite the core from the specifications and port reviewed parts of IRISpark (D7). Start with three control documents, in this order:

1. **SQL compiler specification:** IR model, the rules in §6 and the parameter policy.
2. **API contract:** signatures, semantics and errors for every P0 operation.
3. **Compatibility matrix:** Snowpark and PySpark, fed by the differential tests.

The most important point is **not to start by reimplementing functions**. First, formalize how a set of DataFrame operations is represented, resolved, validated and converted into SQL. This reduces the risk of ending up with methods that work on their own but cannot compose complex queries efficiently.

---

Technical references:
[Snowpark DataFrame Programming](https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-dataframes) ·
[InterSystems IRIS Python Connectivity](https://developer.intersystems.com/next-steps/connecting-to-intersystems-iris-from-client-side-python-applications/)
