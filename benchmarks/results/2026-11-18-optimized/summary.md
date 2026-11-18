# Benchmark summary

2026-11-18T16:26:21 · IRIS: IRIS for UNIX (Ubuntu Server LTS for ARM64 Containers) 2026.2 (Build 221U) Fri Jun 26 2026 09:59:12 EDT · Mborobak 580ae0c · Python 3.11.16 · driver 5.4.1 · Apple M4 (10 cores) · local container (loopback network) · seed 42, skew 1.1 · 2 warm-up + 9 measured runs · medians of total client time

Columns: **native** = hand-written IRIS SQL (IRIS string rules); **native_exact** = the same with `%EXACT` where Mborobak adds it; **mb_iris** = Mborobak with `stringSemantics="iris"`; **mb** = Mborobak default (PySpark rules). **Overhead** compares Mborobak with native SQL of the same semantics; **%EXACT cost** = native_exact vs native. ✓ = results identical.

## 10,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 0.4 | 1.0 | 0.6 | 1.1 | +33% | +8% | +134% | 0.03 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.2 | 0.2 | 0.3 | 0.3 | +87% | +67% | -4% | 0.01 | 1 | ✓ |
| filter_fetch_numeric ¹ | filter | 2.9 | 3.3 | 3.4 | 3.8 | +17% | +16% | +13% | 0.04 | 1010 | ✓ |
| projection_fetch ¹ | projection | 11.5 | 9.8 | 13.9 | 14.0 | +21% | +43% | -15% | 0.02 | 10000 | ✓ |
| agg_by_int ¹ | aggregation | 6.1 | 4.7 | 5.8 | 6.2 | -5% | +32% | -23% | 0.04 | 818 | ✓ |
| agg_by_string ¹ | aggregation | 3.8 | 3.2 | 4.3 | 4.2 | +15% | +30% | -15% | 0.03 | 3 | ✓ |
| join_agg_sort ¹ | join | 3.6 | 3.5 | 3.7 | 3.9 | +3% | +11% | -3% | 0.04 | 30 | ✓ |
| sort_limit_double ¹ | sort | 2.0 | 2.0 | 3.9 | 3.7 | +89% | +84% | -2% | 0.03 | 100 | ✓ |
| chain_scope_example ¹ | chain | 1.8 | 1.8 | 2.0 | 2.0 | +12% | +13% | -1% | 0.06 | 20 | ✓ |

## 100,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 1.2 | 8.4 | 1.3 | 8.4 | +6% | +0% | +575% | 0.03 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.4 | 0.6 | 0.5 | 0.5 | +30% | -3% | +56% | 0.01 | 1 | ✓ |
| filter_fetch_numeric ¹ | filter | 16.6 | 15.8 | 19.2 | 18.6 | +16% | +18% | -5% | 0.02 | 9958 | ✓ |
| projection_fetch ¹ | projection | 87.5 | 89.7 | 146.4 | 145.0 | +67% | +62% | +3% | 0.03 | 100000 | ✓ |
| agg_by_int ¹ | aggregation | 26.6 | 26.5 | 35.1 | 30.2 | +32% | +14% | -0% | 0.03 | 7418 | ✓ |
| agg_by_string ¹ | aggregation | 7.1 | 5.8 | 6.8 | 5.9 | -3% | +2% | -18% | 0.02 | 3 | ✓ |
| join_agg_sort ¹ | join | 24.1 | 23.8 | 24.3 | 23.8 | +1% | -0% | -1% | 0.06 | 30 | ✓ |
| sort_limit_double ¹ | sort | 14.9 | 15.0 | 5.8 | 6.2 | -61% | -58% | +0% | 0.04 | 100 | ✓ |
| chain_scope_example ¹ | chain | 15.9 | 15.9 | 16.0 | 17.2 | +1% | +8% | +0% | 0.03 | 20 | ✓ |

## 1,000,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 30.4 | 83.1 | 29.4 | 82.9 | -3% | -0% | +174% | 0.03 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.6 | 1.3 | 0.7 | 1.4 | +3% | +7% | +102% | 0.02 | 1 | ✓ |
| filter_fetch_numeric | filter | 143.0 | 141.8 | 179.6 | 177.9 | +26% | +25% | -1% | 0.03 | 99538 | ✓ |
| projection_fetch | projection | 903.3 | 850.2 | 1560.4 | 1724.9 | +73% | +103% | -6% | 0.03 | 1000000 | ✓ |
| agg_by_int | aggregation | 523.6 | 502.8 | 514.4 | 197.2 | -2% | -61% | -4% | 0.03 | 64511 | ✓ |
| agg_by_string ¹ | aggregation | 49.8 | 43.7 | 49.6 | 43.0 | -0% | -2% | -12% | 0.04 | 3 | ✓ |
| join_agg_sort | join | 273.4 | 275.5 | 276.0 | 268.2 | +1% | -3% | +1% | 0.05 | 30 | ✓ |
| sort_limit_double | sort | 139.8 | 139.2 | 41.9 | 41.2 | -70% | -70% | -0% | 0.04 | 100 | ✓ |
| chain_scope_example | chain | 164.4 | 155.2 | 154.4 | 154.3 | -6% | -1% | -6% | 0.05 | 20 | ✓ |

¹ Below the 100 ms "substantial query" threshold (D5): absolute overhead in ms matters more than the percentage.

**Median overhead on substantial queries (Mborobak default vs native_exact): -1.6%** over 6 scenario × size pairs (target: at most +10%, scope §8.3).
