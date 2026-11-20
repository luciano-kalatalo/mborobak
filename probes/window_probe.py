"""Probe of IRIS window functions for release 0.2, 2026-11-20.

function_probe.py showed that the common window functions exist. This probe settles how Spark's
window semantics map onto them: default frames (Spark's default with ORDER BY is RANGE … CURRENT ROW,
so ties share a running total), ROWS and RANGE frames with offsets, NULL placement in window ORDER BY,
NaN and -1E19 in window sort keys, collation of string partition and order keys, parameters inside
OVER, and where window functions may appear (over aggregates, in ORDER BY, in WHERE).

Usage:
    python probes/window_probe.py > probes/results-2026-11-20-windows-iris-2026.2.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

T = f"{base.SCHEMA}.W2"
NAN, INF = float("nan"), float("inf")
# g: SQLUPPER string; k: order key with ties and NULL; d: double with specials; x: values with NULLs
ROWS = [
    [1, "a", 1, 1.0, 10],
    [2, "a", 2, 2.0, None],
    [3, "a", 2, NAN, 30],
    [4, "a", None, -1e19, 40],
    [5, "A", 1, None, 50],
    [6, "b", 3, -INF, 60],
    [7, "b", 3, INF, None],
    [8, "10", 4, 0.0, 80],
    [9, "9", 5, -0.0, 90],
]

C = {
    "default_frame_with_order": f"SELECT id, SUM(x) OVER (PARTITION BY %EXACT(g) ORDER BY k) FROM {T} ORDER BY id",
    "explicit_range_frame": f"SELECT id, SUM(x) OVER (PARTITION BY %EXACT(g) ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM {T} ORDER BY id",
    "rows_frame": f"SELECT id, SUM(x) OVER (PARTITION BY %EXACT(g) ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM {T} ORDER BY id",
    "rows_offsets": f"SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM {T} ORDER BY id",
    "rows_following_only": f"SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM {T} ORDER BY id",
    "rows_unbounded_following": f"SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM {T} ORDER BY id",
    "rows_params": f"SELECT id, SUM(x) OVER (ORDER BY id ROWS BETWEEN ? PRECEDING AND CURRENT ROW) FROM {T} ORDER BY id",
    "range_offsets": f"SELECT id, SUM(x) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {T} ORDER BY id",
    "range_unbounded_both": f"SELECT id, SUM(x) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM {T} ORDER BY id",
    "no_order_whole_partition": f"SELECT id, SUM(x) OVER (PARTITION BY %EXACT(g)), COUNT(*) OVER (), COUNT(x) OVER () FROM {T} ORDER BY id",
    "partition_collation_default": f"SELECT id, COUNT(*) OVER (PARTITION BY g) FROM {T} ORDER BY id",
    "partition_collation_exact": f"SELECT id, COUNT(*) OVER (PARTITION BY %EXACT(g)) FROM {T} ORDER BY id",
    "order_null_placement": f"SELECT id, ROW_NUMBER() OVER (ORDER BY k, id), ROW_NUMBER() OVER (ORDER BY k DESC, id) FROM {T} ORDER BY id",
    "order_nulls_last_keyword": f"SELECT id, ROW_NUMBER() OVER (ORDER BY k NULLS LAST, id) FROM {T} ORDER BY id",
    "order_case_null_flag": f"SELECT id, ROW_NUMBER() OVER (ORDER BY CASE WHEN k IS NULL THEN 1 ELSE 0 END, k, id) FROM {T} ORDER BY id",
    "order_double_plain": f"SELECT id, d, ROW_NUMBER() OVER (ORDER BY d, id) FROM {T} ORDER BY id",
    "order_double_bucket": f"SELECT id, d, ROW_NUMBER() OVER (ORDER BY CASE WHEN d IS NULL THEN 0 WHEN d < -1.7976931348623157E308 THEN 1 "
                           f"WHEN d <> d THEN 4 WHEN d > 1.7976931348623157E308 THEN 3 ELSE 2 END, d + 0, id) FROM {T} ORDER BY id",
    "order_string_textorder": f"SELECT id, g, ROW_NUMBER() OVER (ORDER BY %EXACT(' ' || g), id) FROM {T} ORDER BY id",
    "order_string_default": f"SELECT id, g, ROW_NUMBER() OVER (ORDER BY g, id) FROM {T} ORDER BY id",
    "rank_dense_rank_ties": f"SELECT id, RANK() OVER (ORDER BY k), DENSE_RANK() OVER (ORDER BY k) FROM {T} ORDER BY id",
    "percent_rank_cume_dist": f"SELECT id, PERCENT_RANK() OVER (ORDER BY k), CUME_DIST() OVER (ORDER BY k) FROM {T} ORDER BY id",
    "ntile": f"SELECT id, NTILE(4) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "ntile_param": f"SELECT id, NTILE(?) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "lag_lead_default": f"SELECT id, LAG(x, 2, -1) OVER (ORDER BY id), LEAD(x, 1, 0) OVER (ORDER BY id), LAG(x) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "lag_params": f"SELECT id, LAG(x, ?, ?) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "lag_string": f"SELECT id, LAG(g) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "lag_negative": f"SELECT id, LAG(x, -1) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "first_last_value": f"SELECT id, FIRST_VALUE(x) OVER (ORDER BY id), LAST_VALUE(x) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "first_value_rows_frame": f"SELECT id, FIRST_VALUE(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM {T} ORDER BY id",
    "last_value_whole": f"SELECT id, LAST_VALUE(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM {T} ORDER BY id",
    "ignore_nulls": f"SELECT id, FIRST_VALUE(x IGNORE NULLS) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "ignore_nulls_after": f"SELECT id, FIRST_VALUE(x) IGNORE NULLS OVER (ORDER BY id) FROM {T} ORDER BY id",
    "nth_value": f"SELECT id, NTH_VALUE(x, 2) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "min_max_avg_over": f"SELECT id, MIN(x) OVER (PARTITION BY %EXACT(g)), MAX(x) OVER (PARTITION BY %EXACT(g)), AVG(x) OVER (PARTITION BY %EXACT(g)) FROM {T} ORDER BY id",
    "min_string_over": f"SELECT id, MIN(%EXACT(' ' || g)) OVER () FROM {T} ORDER BY id",
    "avg_int_over_type": f"SELECT id, AVG(k) OVER () FROM {T} ORDER BY id",
    "sum_double_specials_over": f"SELECT id, SUM(d) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM {T} WHERE id < 6 ORDER BY id",
    "count_distinct_over": f"SELECT id, COUNT(DISTINCT x) OVER () FROM {T} ORDER BY id",
    "window_over_aggregate": f"SELECT %EXACT(g), SUM(x), RANK() OVER (ORDER BY SUM(x) DESC) FROM {T} GROUP BY %EXACT(g)",
    "window_in_order_by": f"SELECT id FROM {T} ORDER BY ROW_NUMBER() OVER (ORDER BY id DESC)",
    "window_in_where": f"SELECT id FROM {T} WHERE ROW_NUMBER() OVER (ORDER BY id) < 3",
    "window_with_top": f"SELECT TOP 3 id, ROW_NUMBER() OVER (ORDER BY id DESC) FROM {T}",
    "window_with_distinct": f"SELECT DISTINCT %EXACT(g), COUNT(*) OVER () FROM {T}",
    "window_in_expression": f"SELECT id, x - LAG(x) OVER (ORDER BY id), CAST(ROW_NUMBER() OVER (ORDER BY id) AS DOUBLE) / 2 FROM {T} ORDER BY id",
    "window_in_case": f"SELECT id, CASE WHEN ROW_NUMBER() OVER (ORDER BY id) = 1 THEN 'first' ELSE 'other' END FROM {T} ORDER BY id",
    "window_param_in_partition_expr": f"SELECT id, SUM(x) OVER (PARTITION BY MOD(id, ?)) FROM {T} ORDER BY id",
    "window_filter_subquery": f"SELECT * FROM (SELECT id, g, ROW_NUMBER() OVER (PARTITION BY %EXACT(g) ORDER BY id) AS rn FROM {T}) AS s WHERE rn = 1 ORDER BY id",
    "window_types": f"SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, RANK() OVER (ORDER BY id) AS rk, COUNT(*) OVER () AS c, "
                    f"PERCENT_RANK() OVER (ORDER BY id) AS pr, NTILE(2) OVER (ORDER BY id) AS nt, SUM(x) OVER () AS sx, AVG(x) OVER () AS ax FROM {T}",
    "rows_frame_rank": f"SELECT id, RANK() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM {T} ORDER BY id",
    "two_windows_same_select": f"SELECT id, ROW_NUMBER() OVER (PARTITION BY %EXACT(g) ORDER BY id), SUM(x) OVER (ORDER BY id) FROM {T} ORDER BY id",
}
PARAMS = {"rows_params": [1], "ntile_param": [3], "lag_params": [1, -5], "window_param_in_partition_expr": [2]}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    try:
        cur.execute(f"CREATE TABLE {T} (id INT, g VARCHAR(10), k INT, d DOUBLE, x INT)")
        cur.executemany(f"INSERT INTO {T} VALUES (?,?,?,?,?)", ROWS)
        for key, sql in C.items():
            base.probe(cur, key, sql, PARAMS.get(key))
        cur.execute(C["window_types"])
        base.results["window_types.description"] = {"result": [(d[0], d[1]) for d in cur.description]}
    finally:
        base.run(cur, f"DROP TABLE {T}", fetch=False)
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
