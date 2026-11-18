# Benchmark summary

2026-11-18T16:09:38 · IRIS: IRIS for UNIX (Ubuntu Server LTS for ARM64 Containers) 2026.2 (Build 221U) Fri Jun 26 2026 09:59:12 EDT · Mborobak 8bf271d (with local changes) · Python 3.11.16 · driver 5.4.1 · Apple M4 (10 cores) · local container (loopback network) · seed 42, skew 1.1 · 2 warm-up + 5 measured runs · medians of total client time

Columns: **native** = hand-written IRIS SQL (IRIS string rules); **native_exact** = the same with `%EXACT` where Mborobak adds it; **mb_iris** = Mborobak with `stringSemantics="iris"`; **mb** = Mborobak default (PySpark rules). **Overhead** compares Mborobak with native SQL of the same semantics; **%EXACT cost** = native_exact vs native. ✓ = results identical.

## 10,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 0.5 | 1.0 | 0.5 | 1.2 | +1% | +16% | +95% | 0.03 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.2 | 0.1 | 0.2 | 0.2 | -18% | +60% | -42% | 0.01 | 1 | ✓ |
| filter_fetch_numeric ¹ | filter | 2.7 | 2.9 | 4.5 | 4.3 | +67% | +45% | +9% | 0.03 | 1010 | ✓ |
| projection_fetch ¹ | projection | 12.4 | 10.8 | 18.1 | 17.8 | +46% | +64% | -13% | 0.02 | 10000 | ✓ |
| agg_by_int ¹ | aggregation | 8.9 | 7.9 | 8.4 | 8.4 | -5% | +7% | -11% | 0.02 | 818 | ✓ |
| agg_by_string ¹ | aggregation | 1.9 | 1.1 | 1.4 | 1.2 | -28% | +11% | -42% | 0.03 | 3 | ✓ |
| join_agg_sort ¹ | join | 3.7 | 3.4 | 4.1 | 4.3 | +11% | +26% | -7% | 0.06 | 30 | ✓ |
| sort_limit_double ¹ | sort | 2.3 | 2.2 | 2.9 | 2.1 | +28% | -7% | -3% | 0.03 | 100 | ✓ |
| chain_scope_example ¹ | chain | 1.8 | 1.9 | 2.0 | 2.3 | +12% | +20% | +9% | 0.04 | 20 | ✓ |

## 100,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 1.3 | 9.2 | 1.6 | 9.4 | +24% | +3% | +629% | 0.03 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.4 | 0.4 | 0.4 | 0.5 | -9% | +27% | -4% | 0.01 | 1 | ✓ |
| filter_fetch_numeric ¹ | filter | 17.8 | 16.8 | 22.6 | 22.6 | +27% | +34% | -6% | 0.02 | 9958 | ✓ |
| projection_fetch ¹ | projection | 92.5 | 90.6 | 189.0 | 198.2 | +104% | +119% | -2% | 0.03 | 100000 | ✓ |
| agg_by_int ¹ | aggregation | 42.5 | 41.4 | 50.9 | 51.1 | +20% | +23% | -2% | 0.03 | 7418 | ✓ |
| agg_by_string ¹ | aggregation | 26.1 | 23.1 | 70.3 | 23.5 | +169% | +2% | -12% | 0.02 | 3 | ✓ |
| join_agg_sort ¹ | join | 62.1 | 59.6 | 63.0 | 60.2 | +1% | +1% | -4% | 0.05 | 30 | ✓ |
| sort_limit_double ¹ | sort | 16.1 | 15.3 | 21.7 | 21.7 | +34% | +41% | -5% | 0.06 | 100 | ✓ |
| chain_scope_example ¹ | chain | 16.6 | 18.6 | 15.7 | 15.8 | -5% | -15% | +12% | 0.05 | 20 | ✓ |

## 1,000,000 orders

| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|
| filter_count_string ¹ | filter | 25.7 | 83.9 | 29.2 | 87.0 | +14% | +4% | +227% | 0.04 | 1 | ✓ |
| lookup_count_indexed_string ¹ | filter | 0.8 | 1.4 | 0.7 | 1.5 | -11% | +4% | +76% | 0.02 | 1 | ✓ |
| filter_fetch_numeric | filter | 142.2 | 142.2 | 215.1 | 217.9 | +51% | +53% | +0% | 0.03 | 99538 | ✓ |
| projection_fetch | projection | 860.6 | 850.0 | 2059.7 | 2136.4 | +139% | +151% | -1% | 0.03 | 1000000 | ✓ |
| agg_by_int | aggregation | 175.7 | 177.6 | 226.7 | 242.3 | +29% | +36% | +1% | 0.03 | 64511 | ✓ |
| agg_by_string ¹ | aggregation | 52.1 | 42.1 | 51.2 | 44.5 | -2% | +6% | -19% | 0.04 | 3 | ✓ |
| join_agg_sort | join | 527.1 | 299.8 | 295.7 | 310.0 | -44% | +3% | -43% | 0.05 | 30 | ✓ |
| sort_limit_double | sort | 142.9 | 141.6 | 43.0 | 43.9 | -70% | -69% | -1% | 0.05 | 100 | ✓ |
| chain_scope_example | chain | 193.7 | 169.3 | 161.8 | 162.8 | -16% | -4% | -13% | 0.05 | 20 | ✓ |

¹ Below the 100 ms "substantial query" threshold (D5): absolute overhead in ms matters more than the percentage.

**Median overhead on substantial queries (Mborobak default vs native_exact): +19.9%** over 6 scenario × size pairs (target: at most +10%, scope §8.3).
