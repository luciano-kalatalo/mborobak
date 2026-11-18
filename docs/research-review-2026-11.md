# Research Review: Sources for Mborobak (Sep–Nov 2026)

Scientificloud · Mborobak · 2026-11-18 · Companion to [mborobak-scope.md](mborobak-scope.md) v1.1

This review covers the 30 sources on the reading list. For each one it records whether the source could be found, what it says, and what it means for Mborobak. Results reported for other systems (DuckDB, DataFusion, Spark, Oracle, etc.) do not transfer to IRIS automatically; every performance idea below is a hypothesis to test on IRIS.

## 1. Summary

- **Verification:** 25 of 30 sources were found and match their description. 4 were found only partly (the source exists, but some claims or the date could not be confirmed). 1 could not be found as described.
- **Where the value is for the MVP:** most value is in **correctness testing** and **compiler design**, not performance. Mborobak sends SQL to IRIS and does not run its own engine, so engine internals (accumulators, spilling, io_uring, GPU decoding) matter mostly for the P2 roadmap.
- **New open decision:** Spark 4 turns on ANSI SQL mode by default. That changes how errors behave (overflow, invalid casts, division by zero), so Mborobak must state which Spark version and mode it compares against (proposed D9).
- **Ten proposed changes to the scope** are listed in §5. None has been applied yet.

## 2. Verification table

Status: **Verified** = source found and matches the description · **Partial** = source exists, but some claims or the date are unconfirmed · **Not found** = no matching source.
Relevance to the MVP: **High** = changes what we build or test in release 0.1 · **Medium** = shapes design or benchmarks · **Low** = background or P2 only.

| # | Source | Status | Relevance | Notes |
|---|--------|--------|-----------|-------|
| 1 | [DuckDB 2.0.0 LTS](https://duckdb.org/2026/10/21/announcing-duckdb-200) (Oct 21) | Partial | Medium | Release and remote pushdown optimizer confirmed (sends SQL to PostgreSQL/MySQL instead of pulling tables). "Aggregate states" over the remote link not found; the closest feature is partial aggregate pushdown below joins, done locally. |
| 2 | [DataFusion Comet 1.2.0](https://datafusion.apache.org/blog/output/2026/11/02/datafusion-comet-1.2.0/) (Nov 2) | Verified | **High** | Fixes to match Spark results: time-zone labels, −0.0 and NaN, collations, decimal AVG, nested types, join order. |
| 3 | [Query Performance Tuning with Optimal Exploration of Optimizer Cost Model Parameter Space](https://arxiv.org/abs/2610.02607) (Wu, Wang, Narasayya, Chaudhuri) | Partial | Low | Matches the topic and the authors are known Microsoft Research researchers, but affiliation and date were not confirmed from the source. The reading list's claim about comparing native SQL, UDAF and fallback paths goes beyond what the abstract says. |
| 4 | [Velox at Uber](https://www.uber.com/blog/velox-at-uber/) (Nov 10) | Verified | **High** | Phase 1: every query ran on both engines and results were compared (to ~30% of traffic). Phase 2: a blocklist of query fingerprints known to fail. |
| 5 | [LASSO](https://arxiv.org/abs/2610.38294v1) (Jadhav, Sudarshan) | Verified | **High** | Generates test data with "attack strategies", each aimed at one class of semantic error. Code is open source: [xdata-lasso/lasso](https://github.com/xdata-lasso/lasso). Its full title calls it an "LLM-Agent Synthesized System"; the reading list doesn't mention that. |
| 6 | [Tessera](https://arxiv.org/abs/2610.40620) (Oct 29) | Verified | Medium | Join-aggregate operator for skewed sparse relations; avoids materializing intermediate join rows. Up to 1,802× over DuckDB plans (authors' own claim). |
| 7 | [DataFusion: Best Practices for Single Row Lookups](https://datafusion.apache.org/blog/2026/10/24/point-queries/) (Oct 24) | Verified | Medium | Caching the physical plan for prepared statements roughly doubles to triples throughput on point queries. Recommends `target_partitions=1` for lookups. |
| 8 | [EquiTrace](https://arxiv.org/abs/2610.30100) | Verified | **High** | Builds replayable SQL equivalence proofs from rewrite rules, and keeps rule validation separate from proof search. |
| 9 | DataFusion vectorized grouped accumulators (Nov 7) | Not found | Low | No Nov 2026 post found. Related: [PR #26357](https://github.com/apache/datafusion/pull/26357) (`GroupsAccumulator` for `array_agg(DISTINCT)`) and the [2023 design post](https://datafusion.apache.org/blog/2023/08/05/datafusion_fast_grouping/). |
| 10 | Numerical stability after joins (Sep 12) | Partial | Medium | Closest match: [Numerical Stability of Linear Algebra Operations over Relational Databases](https://arxiv.org/abs/2609.13802). It covers error growth from join duplication, but corr/skewness/kurtosis tests specifically were not confirmed. |
| 11 | [CALQA](https://arxiv.org/abs/2610.13249) (Oct 9) | Verified | Low | A fixed-size state built with one GROUP BY, merged by addition, with deletions handled by subtraction. The paper is about forecast-ensemble screening; an "update/merge/finalize" API is an inference, not something the paper names. |
| 12 | [DistRanger](https://arxiv.org/pdf/2609.18501) | Verified | Medium | Bug-finding for distributed DBMSs that varies data distribution, not only individual values. |
| 13 | [Correctness Firewalls for Synthesized Database Components](https://arxiv.org/abs/2611.05173) (Nov 3) | Verified | **High** | Design patterns under which a component can affect performance but never results. Code: [RyanMarcus/correctness-firewalls](https://github.com/RyanMarcus/correctness-firewalls). |
| 14 | [DuckDB: Faster String Aggregations with Dimension Tables](https://duckdb.org/2026/10/02/dimension-tables) (Oct 2) | Verified | Low | Group by narrow integer keys, then join the strings back. Dictionary encoding on disk does not help once data enters the hash table. |
| 15 | DataFusion dictionary compaction after aggregation (Nov 7) | Partial | Low | No blog post; the topic exists as [issue #26696](https://github.com/apache/datafusion/issues/26696), [PR #26703](https://github.com/apache/datafusion/pull/26703) and [issue #27014](https://github.com/apache/datafusion/issues/27014). Date unconfirmed. |
| 16 | [Apache Arrow Rust 60.0.0](https://arrow.apache.org/blog/2026/09/29/arrow-rs-60.0.0/) (Sep 29) | Verified | Low | Kernel speedups, panic removal, faster decimal parsing. Only relevant if the IRIS→Python path ever uses Arrow (P2). |
| 17 | [When Is One Query per Template Enough? Adapting Learned Cost Models to New Hardware](https://arxiv.org/abs/2611.05892) | Verified | Medium | Most slowdown variance is between query templates. One measured query per template plus separate serial/parallel fits gets close to retraining. |
| 18 | [SWatcher+](https://arxiv.org/abs/2610.28270) (Oct 20) | Verified | Low | Corrects learned cardinality estimates only where they would change the plan. IRIS owns its optimizer, so this applies only if Mborobak ever chooses between strategies itself. |
| 19 | DataFusion byte-aware spill batching (Nov 8) | Verified | Low | [PR #25853](https://github.com/apache/datafusion/pull/25853), merged 2026-11-08: spill batches sized by bytes as well as rows. |
| 20 | [JetStream](https://arxiv.org/pdf/2610.20527) (Oct 14) | Verified | Medium | Generated accelerators and engine adapters both build against one fixed, engine-agnostic interface; the DBMS keeps storage and transactions. Two arXiv versions exist with different titles. |
| 21 | [KathDB-FAO](https://arxiv.org/abs/2609.28761v1) | Verified | Medium | Splits queries into atomic actions with input/output contracts, then groups them before generating code. A revised version (Nov 1) has a different title and figures. |
| 22 | [CATune](https://arxiv.org/pdf/2610.09276) | Verified | Low | Extracts dependencies between configuration knobs to avoid invalid combinations. |
| 23 | [Same Pattern, Different Answer](https://arxiv.org/pdf/2609.23032) (GQL/SQL-PGQ) | Verified | Medium (method) | A reference semantics plus a "divergence map" of where engines disagree. The topic doesn't apply, but the method does: it's a model for the PySpark compatibility matrix. |
| 24 | [Apache Spark 4.3.0](https://databricks.com/blog/introducing-apache-spark-43-now-available-databricks-runtime-19) (Oct 30) | Verified | **High** | Quarterly releases from now on. [Versioning policy](https://spark.apache.org/versioning-policy.md): 4.5.0 will be the 4.x LTS. |
| 25 | [Attacking Diophantus: Special Cases of Bag Containment](https://arxiv.org/abs/2609.30956) (Sep 25) | Verified | **High** | Bag (duplicate-preserving) containment is still undecidable in general. Rewrites that touch duplicate counts need explicit care. |
| 26 | [How Can We Shrink the Family of Test Databases?](https://arxiv.org/pdf/2609.16218) | Verified | **High** | Small sets of test databases for queries with NULLs and comparisons; a comparison is never true for NULL. |
| 27 | [Provenance of HAVING Queries in Semirings with Monus](https://arxiv.org/abs/2609.31246) (Sep 25) | Verified | Medium | Formal HAVING semantics. The useful part for Mborobak is the list of edge cases: empty groups, NULLs, duplicates. |
| 28 | [Benchmarking DuckDB v2.0-dev on NVIDIA Vera](https://duckdb.org/2026/10/16/nvidia-vera-benchmark) (Oct 16) | Verified | **High** | Pinned commit, `numactl` socket pinning, one warm-up, median of 3 runs. Caveats: done with NVIDIA, the full methodology post is still pending, and the table shows 2 runs while the text says 3. |
| 29 | [io_uring in Oracle Database](https://arxiv.org/abs/2609.22781) (Sep 19) | Verified | Medium (lesson) | Writes: 29% less CPU, 34% more throughput. Synchronous reads: no gain, and more CPU for large I/O. One change helps some paths and hurts others. |
| 30 | [FastPair: GPU-Optimized String Decoding](https://arxiv.org/pdf/2609.15034) | Verified | Low | Throughput depends on memory accesses per decoded byte. The only Mborobak lesson is to measure copying and materialization separately. |

## 3. Findings by theme

### 3.1 Correctness and translation testing (highest value)

The MVP acceptance criterion "results match native SQL for the reference suite" is only as strong as the test data. Several sources make the same point: hand-written datasets miss silent translation errors.

- **Targeted test data (#5, #26).** LASSO generates small databases designed to tell two queries apart, one attack per error class. #26 shows that queries with NULLs and comparisons need only a small, well-chosen family of test databases. *For Mborobak:* for each compiler rule, generate small adversarial datasets aimed at the errors that rule could cause, instead of relying only on large synthetic datasets. Evaluate whether LASSO itself can compare Mborobak's generated SQL with a hand-written reference query.
- **Duplicate counts (#25).** Rewrites that change duplicate counts are the classic silent bug: `union` vs. `union all`, deduplicated join keys, `distinct` placement, and pre-aggregating before a join. Containment under duplicate-preserving semantics is undecidable in general, so these rewrites must be shown correct one by one, not checked by a general tool.
- **HAVING edge cases (#27).** `groupBy().agg().filter()` compiles to HAVING. Test empty input, groups whose aggregate is NULL, all-NULL groups, duplicate rows, and boundary comparisons.
- **Spark-matching pitfalls (#2).** Comet's 1.2.0 fixes are a ready-made checklist for the compatibility matrix: time zones, −0.0 vs. 0.0, NaN ordering and equality, collations, decimal AVG precision, join output order.
- **Floating-point aggregates (#10).** Join duplication increases floating-point error. Parallel execution on IRIS can also change the order of a SUM, so float results can differ slightly between runs. The criterion "results match native SQL" needs a written tolerance policy: exact for integers and decimals, a relative tolerance for floating point.
- **Data distribution (#12).** Skew and cardinality expose bugs (and performance cliffs) that per-value tests miss. The benchmark data generator should take skew and cardinality as parameters.

### 3.2 Compiler architecture

- **Correctness firewall (#13).** Split the compiler's rewrites into two classes: (a) **required translation**, which defines semantics and must always be correct, and (b) **optional optimization** (subquery flattening, predicate placement, pre-aggregation), which may only change performance. Every class (b) rule must be able to be switched off, and the test suite runs with it on and off and compares results.
- **Rule registry with conditions (#8).** Following EquiTrace, every rewrite rule records its preconditions (for example "valid only if the join key is unique", or "valid only under set semantics"). `explain()` can list which rules fired. The registry also keeps "tested" separate from "proven": Mborobak tests its rules and should not claim they are proven.
- **Typed operator contracts (#21).** KathDB-FAO's contracts on operator inputs and outputs fit Mborobak's analyzer: each IR node declares its input schema, output schema, nullability and duplicate behavior. That makes mismatches compile-time errors.
- **One fixed backend boundary (#20).** JetStream builds everything against a single fixed interface. Mborobak's equivalent is a narrow "SQL dialect + execution adapter" interface. It keeps the scope's promise of possible future backends without building a multi-backend layer now.
- **Shadow execution for new paths (#4).** Uber's two phases fit any future optimized path in Mborobak (flattening rules, pre-aggregation, P2 client-side fallbacks): first run it next to the reference SQL and compare results, then enable it by default with a blocklist of known-bad query shapes. A `mborobak.shadow` diagnostic mode can do this in tests and staging.

### 3.3 Push work to IRIS, return compact results

- **Push down, don't pull (#1).** DuckDB 2.0's remote pushdown sends SQL to the remote database instead of pulling tables. It confirms Mborobak's central design (scope §1, goal 4).
- **Join → groupBy → agg (#6, #14).** Tessera and DuckDB's dimension-table post both target large intermediate results. Mborobak can't change the IRIS executor, but it can produce SQL that pre-aggregates before a join, or groups by integer keys and joins descriptions back afterwards. Both are class (b) rewrites (§3.2), are valid only under conditions (§3.1, duplicate counts), and must be measured on IRIS first. Proposed priority: P1 research, off by default.

### 3.4 Small-query overhead

- **Plan and preparation cost (#7).** For point queries, planning and preparation can cost more than execution. *For Mborobak:* (1) cache compiled SQL per plan shape, so repeating a pipeline skips the analyzer and compiler; (2) keep literals as parameters (scope §6.4) so IRIS can reuse its cached query; (3) report Python-side time (plan building, compilation, schema lookups) separately from IRIS time. This is the realistic way to protect the 10% overhead target for small queries.

### 3.5 Benchmark methodology

- **Recording the environment (#28).** Record the IRIS version and build, the Mborobak commit, Python and driver versions, CPU model, core pinning, IRIS parallel-query settings, warm-up runs and the number of measured runs. Publish raw per-run data, not just medians. The Vera post's own inconsistency (2 vs. 3 runs) shows why the raw data matters.
- **Report per scenario, not just overall (#29, #17).** Oracle's io_uring results improve writes and hurt large reads. The cost-model paper shows most variance is between query families. Report Mborobak's overhead per scenario family (filter, join, aggregation, chain) and never as one global number.
- **Separate the cost stages (#30, #16).** Measure separately: IRIS execution, fetching over the network, converting to Python objects, and building a pandas DataFrame. Arrow-based transfer (#16) is a P2 option once those numbers exist.

### 3.6 PySpark reference version (#24)

Spark now releases every quarter, and 4.5.0 will be the 4.x LTS. Spark 4 enables ANSI SQL mode by default (confirmed through Comet's docs, #2), so overflow, invalid casts and division by zero raise errors instead of returning NULL. The compatibility matrix needs one pinned reference: a specific PySpark version and ANSI mode on or off. IRIS's own behavior in these cases must then be tested and documented against that reference.

### 3.7 Low relevance for the MVP

Sources #3, #9, #11, #15, #18, #19 and #22 are about optimizer internals, engine memory management or configuration tuning. In Mborobak those jobs belong to IRIS. They become relevant in P2: client-side execution, UDAFs (#11 for mergeable statistic states, #9 for compact per-group state, #19 for memory limits measured in bytes) or if Mborobak ever chooses between execution strategies itself (#3, #18, #22).

## 4. Corrections to the reading list

- **#1:** "Aggregate states" over the remote link is not in the DuckDB 2.0 sources found; remote pushdown is.
- **#3:** Authorship by Microsoft Research is likely but unconfirmed. The paper explores the cost-model parameter space, not "native SQL vs. parallel vs. UDAF vs. fallback".
- **#5:** LASSO is described by its authors as LLM-agent-synthesized.
- **#9, #15:** No blog posts found; the work exists as DataFusion GitHub issues and PRs.
- **#10:** No source on corr/skewness/kurtosis after joins was found; the closest is a paper on linear algebra over joins.
- **#11:** CALQA is about screening forecast-ensemble configurations; "update/merge/finalize" is an analogy, not its API.
- **#28:** The DuckDB benchmark was done with NVIDIA, and its detailed methodology post is not yet published.

## 5. Proposed scope changes (for v1.2, not applied)

| # | Section | Change | Source |
|---|---------|--------|--------|
| C1 | §6 | Add rule 10: rewrites are either *required translation* or *optional optimization*. Optional ones can be switched off and are tested both on and off. | #13 |
| C2 | §6 | Add rule 11: rule registry with preconditions per rewrite; `explain()` lists the rules that fired. | #8, #21 |
| C3 | §6 | Add rule 12: rewrites that may change duplicate counts (union, join-key deduplication, distinct, pre-aggregation) each need a written justification and a targeted test. | #25 |
| C4 | §8.2 | Add targeted adversarial test data per rule (evaluate LASSO); a NULL/comparison test family; HAVING edge cases; Comet's pitfall list. | #2, #5, #26, #27 |
| C5 | §8.2 / §10 | Add a numeric tolerance policy: exact for integers and decimals, relative tolerance for floating point. | #10 |
| C6 | §8.3 | Data generator takes skew and cardinality as parameters. | #12 |
| C7 | §8.3 | Environment record and raw-data publication; per-family results; separate timing of execution, fetch, conversion and pandas. | #16, #17, #28, #29, #30 |
| C8 | §5 / §7 | Cache compiled SQL per plan shape; report Python-side planning and compilation time. | #7 |
| C9 | §15 | New D9: pinned PySpark reference version and ANSI mode setting. | #24, #2 |
| C10 | §13 | Release 0.2: shadow mode for optimized paths; pre-aggregation and integer-key grouping rewrites as experiments, off by default. | #4, #6, #14 |

### Decision (2026-11-18)

Applied to [mborobak-scope.md](mborobak-scope.md) v1.2, in reduced form:

- **Adopted:** C4 (LASSO as an optional evaluation, not a requirement), C5, C6, C7 and C9.
- **Merged:** C1, C2 and C3 became one lighter rule (§6.10): rewrites are labeled required or optional, optional ones can be switched off and are tested both on and off, and any rewrite that can change duplicate counts gets a dedicated test. No formal rule registry for now.
- **Adjusted:** C8 kept the compiled-SQL cache and the separate Python-side timing. The expected gain from IRIS cached-query reuse was dropped, because IRIS already turns literals into parameters in its cached queries.
- **Deferred:** C10, to the release 0.2 planning.
- **Added:** an IRIS behavior study in Phase 1. None of the 30 sources is about IRIS, and the main remaining risks depend on IRIS behavior.

### Limits of this review

- Findings come from abstracts, release notes and search snippets, not full papers.
- Claims about IRIS made in this review (parallel execution changing floating-point sums, handling of −0.0, NaN and time zones) were not verified; the IRIS behavior study will settle them.
- The High/Medium/Low relevance ratings are the reviewer's judgment.

## 6. Method

Each item was searched on the web on 2026-11-18, using standard search and then a more thorough search for items not found the first time. Most findings come from abstracts, release notes and search snippets; full papers were not read. Performance figures are the authors' own claims.
