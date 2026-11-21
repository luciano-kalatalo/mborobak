"""Probe of grouping-set syntax in IRIS SQL, 2026-11-21 (compiler spec §7.21, §12.11).

Spark's rollup and cube compile to GROUP BY ROLLUP / CUBE / GROUPING SETS elsewhere. This records which of
those forms IRIS accepts, and whether SELECT without FROM can be combined with UNION ALL.

Usage:
    python probes/grouping_probe.py > probes/results-2026-11-21-grouping-iris-2026.2.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

S = "MborobakGrp"
T = f"{S}.R"


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    cur.execute(f"CREATE TABLE {T} (a VARCHAR(5), b INT, x INT)")
    cur.executemany(f"INSERT INTO {T} VALUES (?,?,?)", [["p", 1, 10], ["p", 2, 20], ["q", 1, 5], [None, 1, 7]])
    for key, sql in {
        "rollup_function": f"SELECT a, b, SUM(x) FROM {T} GROUP BY ROLLUP(a, b)",
        "rollup_bare": f"SELECT a, b, SUM(x) FROM {T} GROUP BY ROLLUP a, b",
        "with_rollup": f"SELECT a, b, SUM(x) FROM {T} GROUP BY a, b WITH ROLLUP",
        "cube": f"SELECT a, b, SUM(x) FROM {T} GROUP BY CUBE(a, b)",
        "grouping_sets": f"SELECT a, b, SUM(x) FROM {T} GROUP BY GROUPING SETS ((a, b), (a), ())",
        "grouping_function": f"SELECT a, SUM(x), GROUPING(a) FROM {T} GROUP BY ROLLUP(a)",
        "empty_group_by": f"SELECT SUM(x) FROM {T} GROUP BY ()",
        "union_without_from": "SELECT 1 AS k UNION ALL SELECT 2",
        "union_of_groupings": f"SELECT a, SUM(x) FROM {T} GROUP BY a UNION ALL SELECT NULL, SUM(x) FROM {T}",
    }.items():
        base.probe(cur, key, sql)
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
