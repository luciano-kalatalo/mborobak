"""Probe of IRIS comparisons with NaN, 2026-11-20 (compiler spec §7.14, §12.7).

Spark orders NaN above every double, +inf included, and treats NaN = NaN as true. This probe records what
IRIS answers for each operator, in a WHERE clause and in a CASE, for every pair of NaN, ±inf, 1, 0, -1E19
and 1E300 in two columns of one row; then NaN against a parameter, in IN lists and across a join.

Usage:
    python probes/nan_comparison_probe.py > probes/results-2026-11-20-nan-iris-2026.2.json
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

S = base.SCHEMA
NAN, INF = float("nan"), float("inf")
VALUES = [NAN, INF, -INF, 1.0, 0.0, -1e19, 1e300]
OPS = ["=", "<>", "<", "<=", ">", ">="]


def spark(op: str, x: float, y: float) -> bool:
    if math.isnan(x) or math.isnan(y):
        cmp = 0 if math.isnan(x) and math.isnan(y) else (1 if math.isnan(x) else -1)
    else:
        cmp = (x > y) - (x < y)
    return {"=": cmp == 0, "<>": cmp != 0, "<": cmp < 0, "<=": cmp <= 0, ">": cmp > 0, ">=": cmp >= 0}[op]


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    for t in ("Nc", "Na", "Nb"):
        base.run(cur, f"DROP TABLE {S}.{t}", fetch=False)
    try:
        cur.execute(f"CREATE TABLE {S}.Nc (id INT, a DOUBLE, b DOUBLE)")
        rows = [[i, x, y] for i, (x, y) in enumerate(((x, y) for x in VALUES for y in VALUES), 1)]
        cur.executemany(f"INSERT INTO {S}.Nc VALUES (?,?,?)", rows)
        for op in OPS:
            cur.execute(f"SELECT id, CASE WHEN a {op} b THEN 1 WHEN NOT (a {op} b) THEN 0 END FROM {S}.Nc")
            in_case = dict(cur.fetchall())
            cur.execute(f"SELECT id FROM {S}.Nc WHERE a {op} b")
            in_where = {r[0] for r in cur.fetchall()}
            differ = [{"a": x, "b": y, "spark": spark(op, x, y), "case": in_case[i], "where": i in in_where}
                      for i, x, y in rows if in_case[i] != int(spark(op, x, y)) or (i in in_where) != spark(op, x, y)]
            base.results[f"columns.{op}"] = {"status": "ok", "differ_from_spark": differ}
        base.probe(cur, "isnan_in_case", f"SELECT a, CASE WHEN a <> a THEN 1 ELSE 0 END FROM {S}.Nc WHERE b = 0")
        base.probe(cur, "isnan_case_in_where", f"SELECT COUNT(*) FROM {S}.Nc WHERE CASE WHEN a <> a THEN 1 ELSE 0 END = 1")
        base.probe(cur, "isnan_bare_in_where", f"SELECT COUNT(*) FROM {S}.Nc WHERE a <> a")

        cur.execute(f"CREATE TABLE {S}.Na (id INT, a DOUBLE)")
        cur.execute(f"CREATE TABLE {S}.Nb (id INT, b DOUBLE)")
        cur.executemany(f"INSERT INTO {S}.Na VALUES (?,?)", [[1, NAN], [2, 1.0], [3, None]])
        cur.executemany(f"INSERT INTO {S}.Nb VALUES (?,?)", [[10, NAN], [20, 1.0]])
        base.probe(cur, "join_eq", f"SELECT x.id, y.id FROM {S}.Na x JOIN {S}.Nb y ON x.a = y.b ORDER BY 1")
        base.probe(cur, "cross_where_eq", f"SELECT x.id, y.id FROM {S}.Na x, {S}.Nb y WHERE x.a = y.b ORDER BY 1")
        for op in OPS:
            base.probe(cur, f"param_nan.{op}", f"SELECT id FROM {S}.Na WHERE a {op} ? ORDER BY id", [NAN])
        base.probe(cur, "in_nan_where", f"SELECT id FROM {S}.Na WHERE a IN (?, ?) ORDER BY id", [NAN, 1.0])
        base.probe(cur, "in_nan_case", f"SELECT id, CASE WHEN a IN (?) THEN 1 ELSE 0 END FROM {S}.Na ORDER BY id",
                   [NAN])
        base.probe(cur, "fix_eq_or_both_nan",
                   f"SELECT x.id, y.id FROM {S}.Na x JOIN {S}.Nb y ON (x.a = y.b OR "
                   "CASE WHEN x.a <> x.a AND y.b <> y.b THEN 1 ELSE 0 END = 1) ORDER BY 1")
        base.probe(cur, "fix_gt_param", f"SELECT id FROM {S}.Na WHERE (a > ? OR CASE WHEN a <> a THEN 1 ELSE 0 END = 1) "
                   "ORDER BY id", [0])
    finally:
        for t in ("Nc", "Na", "Nb"):
            base.run(cur, f"DROP TABLE {S}.{t}", fetch=False)
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
