"""IRIS behavior probe for the Mborobak Phase 1 study.

Runs small SQL experiments against a live IRIS instance and prints a JSON-ish
report. Everything is created in the MborobakProbe schema (plus one unqualified
table to observe the default schema) and dropped at the end.

Connection settings come from environment variables:
    IRIS_HOST, IRIS_PORT, IRIS_NAMESPACE, IRIS_USERNAME, IRIS_PASSWORD

Usage:
    python probes/iris_behavior_probe.py [--rows 500000]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import time
from typing import Any

import iris

SCHEMA = "MborobakProbe"
results: dict[str, Any] = {}


def connect():
    return iris.connect(
        os.environ["IRIS_HOST"],
        int(os.environ["IRIS_PORT"]),
        os.environ["IRIS_NAMESPACE"],
        os.environ["IRIS_USERNAME"],
        os.environ["IRIS_PASSWORD"],
    )


def run(cur, sql: str, params: list | None = None, fetch: bool = True):
    """Execute and return ("ok", rows) or ("error", message)."""
    try:
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        if fetch:
            try:
                return ("ok", [list(r) for r in cur.fetchall()])
            except Exception:
                return ("ok", None)
        return ("ok", None)
    except Exception as exc:  # noqa: BLE001 - probing errors is the point
        return ("error", str(exc).splitlines()[0][:300])


def record(key: str, sql: str, outcome) -> None:
    status, payload = outcome
    results[key] = {"sql": sql, "status": status, "result": payload}


def probe(cur, key: str, sql: str, params: list | None = None) -> None:
    record(key, sql if params is None else f"{sql}  -- params={params!r}", run(cur, sql, params))


def drop_all(cur) -> None:
    for t in ("T", "Nums", "Big", "Words"):
        run(cur, f"DROP TABLE {SCHEMA}.{t}", fetch=False)
    run(cur, "DROP TABLE FxProbeUnqualified", fetch=False)


def setup_small(cur) -> None:
    run(cur, f"CREATE TABLE {SCHEMA}.T (id INT, grp VARCHAR(10), s VARCHAR(20), "
             "amount NUMERIC(10,2), d DOUBLE, i INT)", fetch=False)
    rows = [
        (1, "a", "x", 10.10, 1.5, 7),
        (2, "a", "", 20.20, -0.0, 2),
        (3, "b", None, 30.30, 2.5, None),
        (4, "b", "y", None, None, 3),
        (5, None, "z", 50.50, 3.5, 4),
    ]
    for r in rows:
        cur.execute(f"INSERT INTO {SCHEMA}.T (id, grp, s, amount, d, i) VALUES (?,?,?,?,?,?)", list(r))


def q1_ordering(cur) -> None:
    t = f"{SCHEMA}.T"
    probe(cur, "q1.order_by_in_subquery", f"SELECT id FROM (SELECT id FROM {t} ORDER BY id DESC)")
    probe(cur, "q1.order_by_in_subquery_with_top",
          f"SELECT id FROM (SELECT TOP 3 id FROM {t} ORDER BY id DESC)")
    probe(cur, "q1.order_by_in_subquery_with_top_all",
          f"SELECT id FROM (SELECT TOP ALL id FROM {t} ORDER BY id DESC)")
    probe(cur, "q1.order_by_in_subquery_then_filter",
          f"SELECT id FROM (SELECT TOP ALL id FROM {t} ORDER BY id DESC) WHERE id > 1")
    probe(cur, "q1.order_by_in_cte",
          f"WITH c AS (SELECT id FROM {t} ORDER BY id DESC) SELECT id FROM c")
    probe(cur, "q1.order_by_inside_union_arm",
          f"SELECT id FROM {t} WHERE id < 3 ORDER BY id DESC UNION ALL SELECT id FROM {t} WHERE id >= 3")
    probe(cur, "q1.order_by_after_union",
          f"SELECT id FROM {t} WHERE id < 3 UNION ALL SELECT id FROM {t} WHERE id >= 3 ORDER BY id DESC")
    probe(cur, "q1.union_distinct_vs_all",
          f"SELECT COUNT(*) FROM (SELECT grp FROM {t} UNION SELECT grp FROM {t})")


def q2_limits(cur) -> None:
    t = f"{SCHEMA}.T"
    probe(cur, "q2.top_n", f"SELECT TOP 2 id FROM {t} ORDER BY id")
    probe(cur, "q2.top_param", f"SELECT TOP ? id FROM {t} ORDER BY id", [2])
    probe(cur, "q2.limit", f"SELECT id FROM {t} ORDER BY id LIMIT 2")
    probe(cur, "q2.limit_offset", f"SELECT id FROM {t} ORDER BY id LIMIT 2 OFFSET 1")
    probe(cur, "q2.limit_param", f"SELECT id FROM {t} ORDER BY id LIMIT ?", [2])
    probe(cur, "q2.offset_fetch",
          f"SELECT id FROM {t} ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")
    probe(cur, "q2.limit_in_subquery",
          f"SELECT id FROM (SELECT id FROM {t} ORDER BY id LIMIT 2) ORDER BY id DESC")


def q3_parameters(cur) -> None:
    t = f"{SCHEMA}.T"
    probe(cur, "q3.param_in_where", f"SELECT id FROM {t} WHERE i > ? ORDER BY id", [3])
    probe(cur, "q3.param_in_select_and_group_by",
          f"SELECT i + ? AS k, COUNT(*) FROM {t} GROUP BY i + ?", [1, 1])
    probe(cur, "q3.literal_in_select_and_group_by",
          f"SELECT i + 1 AS k, COUNT(*) FROM {t} GROUP BY i + 1")
    probe(cur, "q3.group_by_alias", f"SELECT i + 1 AS k, COUNT(*) FROM {t} GROUP BY k")
    probe(cur, "q3.group_by_ordinal", f"SELECT i + 1 AS k, COUNT(*) FROM {t} GROUP BY 1")
    probe(cur, "q3.param_in_case_select_group",
          f"SELECT CASE WHEN i > ? THEN 'hi' ELSE 'lo' END AS b, COUNT(*) FROM {t} "
          f"GROUP BY CASE WHEN i > ? THEN 'hi' ELSE 'lo' END", [3, 3])
    probe(cur, "q3.param_in_in_list", f"SELECT id FROM {t} WHERE id IN (?, ?, ?) ORDER BY id", [1, 3, 5])
    probe(cur, "q3.param_in_like", f"SELECT id FROM {t} WHERE s LIKE ? ORDER BY id", ["%y%"])
    probe(cur, "q3.param_in_having",
          f"SELECT grp, COUNT(*) FROM {t} GROUP BY grp HAVING COUNT(*) > ?", [1])
    probe(cur, "q3.param_typed_select", "SELECT ? AS v", [1.5])
    probe(cur, "q3.is_null_on_expression_in_select", f"SELECT (i + 1) IS NULL FROM {t}")
    probe(cur, "q3.case_is_null_in_select",
          f"SELECT CASE WHEN i + 1 IS NULL THEN 1 ELSE 0 END FROM {t} ORDER BY id")


def q4_identifiers(cur) -> None:
    probe(cur, "q4.lowercase_table_and_columns",
          f"SELECT ID, AMOUNT FROM {SCHEMA.lower()}.t WHERE id = 1")
    run(cur, "CREATE TABLE FxProbeUnqualified (a INT)", fetch=False)
    probe(cur, "q4.default_schema_for_unqualified_create",
          "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
          "WHERE TABLE_NAME = 'FxProbeUnqualified'")
    probe(cur, "q4.quoted_reserved_word_alias", f'SELECT id AS "Order" FROM {SCHEMA}.T WHERE id = 1')
    probe(cur, "q4.unquoted_reserved_word_alias", f"SELECT id AS Order FROM {SCHEMA}.T WHERE id = 1")
    probe(cur, "q4.quoted_mixed_case_column_name",
          f'SELECT "Id" FROM {SCHEMA}.T WHERE id = 1')
    probe(cur, "q4.result_column_name_case",
          f"SELECT id, Amount AS RevenueTotal FROM {SCHEMA}.T WHERE id = 1")
    cur.execute(f"SELECT id, Amount AS RevenueTotal, s AS lower_alias FROM {SCHEMA}.T WHERE id = 1")
    results["q4.cursor_description_names"] = {"result": [d[0] for d in cur.description]}
    cur.fetchall()
    probe(cur, "q4.columns_metadata",
          "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
          "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
          "ORDER BY ORDINAL_POSITION", [SCHEMA, "T"])


def q5_semantics(cur) -> None:
    t = f"{SCHEMA}.T"
    probe(cur, "q5.empty_string_round_trip", f"SELECT id, s FROM {t} WHERE id IN (2, 3) ORDER BY id")
    probe(cur, "q5.empty_string_is_null", f"SELECT COUNT(*) FROM {t} WHERE s IS NULL")
    probe(cur, "q5.empty_string_equals_literal", f"SELECT COUNT(*) FROM {t} WHERE s = ''")
    probe(cur, "q5.empty_string_equals_param", f"SELECT COUNT(*) FROM {t} WHERE s = ?", [""])
    probe(cur, "q5.length_of_empty", f"SELECT LENGTH(s), CHAR_LENGTH(s) FROM {t} WHERE id = 2")
    probe(cur, "q5.empty_literal_in_select", "SELECT '' AS e, LENGTH('') AS n")
    probe(cur, "q5.concat_with_null", f"SELECT s || 'q' FROM {t} WHERE id IN (2, 3) ORDER BY id")
    probe(cur, "q5.integer_division", "SELECT 7/2, -7/2, 7\\2, -7\\2, 7 # 2, -7 # 2")
    probe(cur, "q5.integer_column_division", f"SELECT i / 2 FROM {t} WHERE id = 1")
    probe(cur, "q5.division_by_zero", "SELECT 1/0")
    probe(cur, "q5.division_by_zero_column", f"SELECT i / (i - i) FROM {t} WHERE id = 1")
    probe(cur, "q5.avg_of_integers", f"SELECT AVG(i) FROM {t} WHERE id IN (1, 2)")
    probe(cur, "q5.avg_of_numeric", f"SELECT AVG(amount), SUM(amount) FROM {t}")
    probe(cur, "q5.numeric_divide_scale", "SELECT CAST(1 AS NUMERIC(10,2)) / 3")
    probe(cur, "q5.numeric_overflow_cast", "SELECT CAST(123456.789 AS NUMERIC(5,2))")
    probe(cur, "q5.invalid_cast_string_to_int", "SELECT CAST('abc' AS INTEGER)")
    probe(cur, "q5.int_overflow", "SELECT CAST(2147483647 AS INTEGER) + 1")
    probe(cur, "q5.null_order_asc", f"SELECT s FROM {t} ORDER BY s ASC")
    probe(cur, "q5.null_order_desc", f"SELECT s FROM {t} ORDER BY s DESC")
    probe(cur, "q5.nulls_last_syntax", f"SELECT s FROM {t} ORDER BY s ASC NULLS LAST")
    probe(cur, "q5.nulls_first_syntax", f"SELECT s FROM {t} ORDER BY s DESC NULLS FIRST")
    probe(cur, "q5.group_by_null_key", f"SELECT grp, COUNT(*) FROM {t} GROUP BY grp ORDER BY grp")
    probe(cur, "q5.count_star_vs_column", f"SELECT COUNT(*), COUNT(i), COUNT(s), SUM(i) FROM {t}")
    probe(cur, "q5.aggregates_on_empty_input",
          f"SELECT COUNT(*), SUM(i), AVG(i), MAX(i) FROM {t} WHERE 1 = 0")
    probe(cur, "q5.group_by_on_empty_input",
          f"SELECT grp, COUNT(*) FROM {t} WHERE 1 = 0 GROUP BY grp")
    probe(cur, "q5.having_without_group_by_empty",
          f"SELECT COUNT(*) FROM {t} WHERE 1 = 0 HAVING COUNT(*) = 0")
    probe(cur, "q5.string_comparison_case", "SELECT CASE WHEN 'a' = 'A' THEN 1 ELSE 0 END")
    probe(cur, "q5.string_column_case_compare", f"SELECT COUNT(*) FROM {t} WHERE s = 'X'")
    probe(cur, "q5.string_order_case", "SELECT TOP ALL w FROM (SELECT 'b' AS w UNION ALL SELECT 'B' "
                                       "UNION ALL SELECT 'a' UNION ALL SELECT 'A') ORDER BY w")
    probe(cur, "q5.trailing_space_compare", "SELECT CASE WHEN 'a' = 'a ' THEN 1 ELSE 0 END")
    probe(cur, "q5.distinct_case", "SELECT COUNT(DISTINCT w) FROM (SELECT 'b' AS w UNION ALL "
                                   "SELECT 'B')")


def q6_special_values(cur) -> None:
    t = f"{SCHEMA}.T"
    probe(cur, "q6.negative_zero_stored", f"SELECT d FROM {t} WHERE id = 2")
    probe(cur, "q6.negative_zero_literal", "SELECT -0.0, CAST(-0.0 AS DOUBLE)")
    probe(cur, "q6.negative_zero_equals_zero", f"SELECT COUNT(*) FROM {t} WHERE d = 0")
    probe(cur, "q6.insert_nan_param", f"INSERT INTO {t} (id, d) VALUES (?, ?)", [90, float("nan")])
    probe(cur, "q6.read_nan_back", f"SELECT id, d FROM {t} WHERE id = 90")
    probe(cur, "q6.insert_inf_param", f"INSERT INTO {t} (id, d) VALUES (?, ?)", [91, float("inf")])
    probe(cur, "q6.read_inf_back", f"SELECT id, d FROM {t} WHERE id = 91")
    run(cur, f"DELETE FROM {t} WHERE id IN (90, 91)", fetch=False)
    probe(cur, "q6.double_literal_division_by_zero", "SELECT CAST(1 AS DOUBLE) / 0")
    probe(cur, "q6.current_timestamp_vs_utc", "SELECT CURRENT_TIMESTAMP, GETUTCDATE(), NOW()")
    probe(cur, "q6.timestamp_with_offset_literal",
          "SELECT CAST('2026-11-18 10:00:00' AS TIMESTAMP), CAST('2026-11-18T10:00:00+02:00' AS TIMESTAMP)")
    probe(cur, "q6.timestamp_python_type", "SELECT CAST('2026-11-18 10:00:00.123456' AS TIMESTAMP)")
    cur.execute("SELECT CAST('2026-11-18 10:00:00' AS TIMESTAMP), CAST('2026-11-18' AS DATE), "
                "CAST(1.25 AS NUMERIC(10,2)), CAST(1.5 AS DOUBLE), CAST(1 AS INTEGER), CAST(1 AS BIT)")
    row = cur.fetchall()[0]
    results["q6.python_types_returned"] = {"result": [type(v).__name__ for v in row]}


def q7_parallel(cur, n: int) -> None:
    big = f"{SCHEMA}.Big"
    run(cur, f"CREATE TABLE {big} (id INT, k INT, d DOUBLE, amount NUMERIC(14,2), s VARCHAR(30))", fetch=False)
    rnd = random.Random(42)
    batch = []
    t0 = time.perf_counter()
    for i in range(n):
        mag = 10 ** rnd.randint(-6, 9)
        batch.append([i, i % 1000, rnd.uniform(-1, 1) * mag, round(rnd.uniform(0, 1000), 2), f"name{i % 5000}"])
        if len(batch) == 10000:
            cur.executemany(f"INSERT INTO {big} (id, k, d, amount, s) VALUES (?,?,?,?,?)", batch)
            batch = []
    if batch:
        cur.executemany(f"INSERT INTO {big} (id, k, d, amount, s) VALUES (?,?,?,?,?)", batch)
    insert_s = time.perf_counter() - t0
    results["q7.insert_rows"] = {"result": {"rows": n, "seconds": round(insert_s, 2),
                                            "rows_per_s": round(n / insert_s)}}
    run(cur, f"TUNE TABLE {big}", fetch=False)

    def timed(key, sql, repeat=3):
        vals, times = [], []
        for _ in range(repeat):
            t1 = time.perf_counter()
            status, rows = run(cur, sql)
            times.append(round(time.perf_counter() - t1, 4))
            vals.append(rows[0] if status == "ok" and rows else rows)
        results[key] = {"sql": sql, "result": {"values": vals, "seconds": times}}

    timed("q7.sum_double_serial", f"SELECT SUM(d), AVG(d) FROM {big}")
    timed("q7.sum_double_parallel", f"SELECT SUM(d), AVG(d) FROM %PARALLEL {big}")
    timed("q7.sum_numeric_serial", f"SELECT SUM(amount) FROM {big}")
    timed("q7.sum_numeric_parallel", f"SELECT SUM(amount) FROM %PARALLEL {big}")
    timed("q7.group_sum_serial", f"SELECT k, SUM(d) FROM {big} GROUP BY k ORDER BY k")
    timed("q7.group_sum_parallel", f"SELECT k, SUM(d) FROM %PARALLEL {big} GROUP BY k ORDER BY k")
    for key in ("q7.group_sum_serial", "q7.group_sum_parallel"):
        v = results[key]["result"]["values"]
        results[key]["result"]["values"] = f"{len(v[0]) if isinstance(v[0], list) else v} groups (rows elided)"
    # Compare per-group results serial vs parallel exactly.
    s_ok, s_rows = run(cur, f"SELECT k, SUM(d) FROM {big} GROUP BY k ORDER BY k")
    p_ok, p_rows = run(cur, f"SELECT k, SUM(d) FROM %PARALLEL {big} GROUP BY k ORDER BY k")
    if s_ok == p_ok == "ok":
        diffs = [abs(a[1] - b[1]) / max(abs(a[1]), 1e-300) for a, b in zip(s_rows, p_rows) if a[1] != b[1]]
        results["q7.group_sum_serial_vs_parallel"] = {"result": {
            "groups": len(s_rows), "groups_differing": len(diffs),
            "max_relative_diff": max(diffs) if diffs else 0.0}}
    # Python reference for the global double sum.
    ok, rows = run(cur, f"SELECT d FROM {big}")
    if ok == "ok":
        exact = math.fsum(r[0] for r in rows)
        results["q7.python_fsum_reference"] = {"result": exact}


def q8_fetch(cur, n: int) -> None:
    big = f"{SCHEMA}.Big"
    out = {}
    for size in (10_000, 100_000, n):
        sql = f"SELECT TOP {size} id, k, d, amount, s FROM {big}"
        t0 = time.perf_counter()
        cur.execute(sql)
        t_exec = time.perf_counter() - t0
        rows = cur.fetchall()
        t_fetch = time.perf_counter() - t0 - t_exec
        entry = {"rows": len(rows), "execute_s": round(t_exec, 4), "fetch_s": round(t_fetch, 4),
                 "rows_per_s": round(len(rows) / max(t_fetch + t_exec, 1e-9))}
        try:
            import pandas as pd
            t2 = time.perf_counter()
            pd.DataFrame([list(r) for r in rows], columns=[d[0] for d in cur.description])
            entry["pandas_build_s"] = round(time.perf_counter() - t2, 4)
        except ImportError:
            entry["pandas_build_s"] = "pandas not installed"
        out[str(size)] = entry
    results["q8.fetch_timing"] = {"result": out}
    # Statement size: a long IN list of literals.
    for count in (1000, 5000):
        lits = ",".join(str(i) for i in range(count))
        record(f"q8.in_list_{count}_literals", f"SELECT COUNT(*) FROM {big} WHERE id IN (...{count} literals)",
               run(cur, f"SELECT COUNT(*) FROM {big} WHERE id IN ({lits})"))
    for count in (1000, 5000):
        marks = ",".join("?" for _ in range(count))
        record(f"q8.in_list_{count}_params", f"SELECT COUNT(*) FROM {big} WHERE id IN (...{count} params)",
               run(cur, f"SELECT COUNT(*) FROM {big} WHERE id IN ({marks})", list(range(count))))
    for arms in (50, 200, 500):
        sql = " UNION ALL ".join(f"SELECT {i} AS v FROM {SCHEMA}.T WHERE id = 1" for i in range(arms))
        record(f"q8.union_all_{arms}_arms ({len(sql)} chars)", "UNION ALL chain",
               run(cur, f"SELECT COUNT(*) FROM ({sql})"))


def q9_collation(cur) -> None:
    """Default string collation (SQLUPPER) on columns vs. literals and %EXACT."""
    w = f"{SCHEMA}.Words"
    run(cur, f"CREATE TABLE {w} (id INT, w VARCHAR(20), wx VARCHAR(20) COLLATE %EXACT)", fetch=False)
    for i, v in enumerate(["apple", "Apple", "APPLE", "banana", "apple ", " apple", "Banana"]):
        cur.execute(f"INSERT INTO {w} (id, w, wx) VALUES (?, ?, ?)", [i, v, v])
    probe(cur, "q9.group_by_default_collation", f"SELECT w, COUNT(*) FROM {w} GROUP BY w ORDER BY w")
    probe(cur, "q9.group_by_exact_function", f"SELECT %EXACT(w), COUNT(*) FROM {w} GROUP BY %EXACT(w) ORDER BY 1")
    probe(cur, "q9.group_by_exact_column", f"SELECT wx, COUNT(*) FROM {w} GROUP BY wx ORDER BY wx")
    probe(cur, "q9.distinct_default", f"SELECT DISTINCT w FROM {w}")
    probe(cur, "q9.distinct_exact_column", f"SELECT DISTINCT wx FROM {w}")
    probe(cur, "q9.count_distinct_default_vs_exact",
          f"SELECT COUNT(DISTINCT w), COUNT(DISTINCT %EXACT(w)), COUNT(DISTINCT wx) FROM {w}")
    probe(cur, "q9.equality_default", f"SELECT COUNT(*) FROM {w} WHERE w = 'APPLE'")
    probe(cur, "q9.equality_exact_column", f"SELECT COUNT(*) FROM {w} WHERE wx = 'APPLE'")
    probe(cur, "q9.equality_param_default", f"SELECT COUNT(*) FROM {w} WHERE w = ?", ["apple"])
    probe(cur, "q9.trailing_space_equality_default", f"SELECT id FROM {w} WHERE w = 'apple' ORDER BY id")
    probe(cur, "q9.order_by_default", f"SELECT w FROM {w} ORDER BY w, id")
    probe(cur, "q9.order_by_exact_column", f"SELECT wx FROM {w} ORDER BY wx")
    probe(cur, "q9.like_default", f"SELECT COUNT(*) FROM {w} WHERE w LIKE 'APP%'")
    probe(cur, "q9.in_default", f"SELECT COUNT(*) FROM {w} WHERE w IN ('APPLE')")
    probe(cur, "q9.self_join_default",
          f"SELECT COUNT(*) FROM {w} a JOIN {w} b ON a.w = b.w")
    probe(cur, "q9.self_join_exact",
          f"SELECT COUNT(*) FROM {w} a JOIN {w} b ON a.wx = b.wx")
    probe(cur, "q9.union_distinct_default",
          f"SELECT COUNT(*) FROM (SELECT w FROM {w} UNION SELECT w FROM {w})")
    probe(cur, "q9.max_default", f"SELECT MAX(w), MIN(w) FROM {w}")
    probe(cur, "q9.namespace_default_collation",
          "SELECT COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'Words' ORDER BY ORDINAL_POSITION", [SCHEMA])
    probe(cur, "q9.date_type_python", "SELECT CAST('2026-11-18' AS DATE), {d '2026-11-18'}")
    probe(cur, "q9.group_by_ordinal_check", f"SELECT w, COUNT(*) FROM {w} GROUP BY 1")


def q10_limits(cur) -> None:
    big = f"{SCHEMA}.Big"
    for count in (10, 50, 100, 200, 400, 800):
        lits = ",".join(str(i) for i in range(count))
        record(f"q10.in_list_{count}_literals", "IN list", run(cur, f"SELECT COUNT(*) FROM {big} WHERE id IN ({lits})"))
        marks = ",".join("?" for _ in range(count))
        record(f"q10.in_list_{count}_params", "IN list",
               run(cur, f"SELECT COUNT(*) FROM {big} WHERE id IN ({marks})", list(range(count))))
    for arms in (250, 300, 350, 400, 450):
        sql = " UNION ALL ".join(f"SELECT {i} AS v FROM {SCHEMA}.T WHERE id = 1" for i in range(arms))
        record(f"q10.union_all_{arms}_arms_{len(sql)}_chars", "UNION ALL chain",
               run(cur, f"SELECT COUNT(*) FROM ({sql})"))
    # Long statement without many arms: one wide CASE expression.
    for n in (500, 1000, 2000):
        case = "CASE " + " ".join(f"WHEN id = {i} THEN {i}" for i in range(n)) + " ELSE -1 END"
        record(f"q10.case_{n}_branches_{len(case)}_chars", "wide CASE",
               run(cur, f"SELECT COUNT(*) FROM (SELECT {case} AS c FROM {SCHEMA}.T)"))
    # Deep nesting of subqueries.
    for depth in (10, 30, 60):
        sql = f"SELECT id FROM {SCHEMA}.T"
        for _ in range(depth):
            sql = f"SELECT id FROM ({sql}) WHERE id > 0"
        record(f"q10.nested_subqueries_{depth}", "nesting", run(cur, f"SELECT COUNT(*) FROM ({sql})"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=500_000)
    args = ap.parse_args()

    conn = connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    results["meta.version"] = {"result": cur.fetchall()[0][0]}
    results["meta.driver"] = {"result": getattr(iris, "__version__", "unknown")}
    drop_all(cur)
    try:
        setup_small(cur)
        q1_ordering(cur)
        q2_limits(cur)
        q3_parameters(cur)
        q4_identifiers(cur)
        q5_semantics(cur)
        q6_special_values(cur)
        q9_collation(cur)
        q7_parallel(cur, args.rows)
        q8_fetch(cur, args.rows)
        q10_limits(cur)
    finally:
        drop_all(cur)
        conn.close()
    print(json.dumps(results, indent=1, default=str))


if __name__ == "__main__":
    main()
