# Benchmarks

Mborobak against hand-written IRIS SQL on the same IRIS instance, through the same DB-API driver ([scope](../docs/mborobak-scope.md) §8.3).

```bash
set -a; . path/to/.env; set +a        # IRIS_HOST, IRIS_PORT, IRIS_NAMESPACE, IRIS_USERNAME, IRIS_PASSWORD
.venv/bin/python benchmarks/run.py --sizes 10000,100000,1000000 --runs 5 --warmup 2
```

| File | Contents |
|------|----------|
| [`generate.py`](generate.py) | Seeded data: `Orders` (n rows) and `Customers` (n / 10). `--skew` sets how unevenly orders spread over customers (0 = uniform). Strings are mixed case on purpose. Indexes on `Orders.status`, `Orders.customer_id`, `Customers.country`; tables tuned after loading |
| [`scenarios.py`](scenarios.py) | 9 scenarios in 6 families (filter, projection, aggregation, join, sort, chain); each has a DataFrame pipeline and two hand-written SQL versions |
| [`run.py`](run.py) | Loads each size, runs 4 variants per scenario, checks that results match, writes the results |

**Variants.** `native` is the SQL a developer would write for IRIS. `native_exact` is the same with `%EXACT` wherever Mborobak adds it for PySpark's case-sensitive strings (D10). `mborobak_iris` runs Mborobak with `stringSemantics="iris"`, `mborobak` with the default. So:

- `mborobak_iris` vs `native` is Mborobak's own overhead;
- `mborobak` vs `native_exact` is the same overhead under PySpark semantics (the scope's acceptance target);
- `native_exact` vs `native` is what `%EXACT` costs (compiler spec O4).

**Output** (in `results/<date>/`): `environment.json` (IRIS build and settings, commit, Python, driver, CPU, seed, runs), `raw.json` (every measured run, with execute, fetch and Python-side times, plus a cold-compile record per scenario and a result check), `plans.json` (IRIS `EXPLAIN` of every variant at the largest size) and `summary.md`.

**Timing.** Total time is measured on the client around the whole call. For Mborobak it includes plan analysis and compilation (cached after the first run), execution, fetch and conversion of every value to its schema type; native SQL fetches raw driver values. Warm-up runs fill IRIS's cached queries and buffers.
