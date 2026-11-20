"""Probe of IRIS GLOBAL TEMPORARY tables for createDataFrame (release 0.2.x), 2026-11-20 (compiler spec §9.8).

Two connections: which one sees the rows, which types round-trip through executemany, how fast a bulk
insert is, and what dropping the definition does.

Usage:
    python probes/temp_table_probe.py > probes/results-2026-11-20-temp-iris-2026.2.json
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

S = base.SCHEMA


def main() -> None:
    c1, c2 = base.connect(), base.connect()
    cur, cur2 = c1.cursor(), c2.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    for t in ("G1", "G2"):
        base.run(cur, f"DROP TABLE {S}.{t}", fetch=False)
    try:
        base.probe(cur, "create", f"CREATE GLOBAL TEMPORARY TABLE {S}.G1 (id INT, s VARCHAR(100) COLLATE EXACT, "
                   "d DOUBLE, n NUMERIC(10,2), b BIT, dt DATE, ts TIMESTAMP)")
        rows = [[1, "a", float("nan"), decimal.Decimal("1.25"), True, dt.date(2026, 1, 1),
                 dt.datetime(2026, 1, 1, 1, 2, 3, 456789)], [2, None, float("-inf"), None, False, None, None]]
        cur.executemany(f"INSERT INTO {S}.G1 VALUES (?,?,?,?,?,?,?)", rows)
        base.probe(cur, "read_same_connection", f"SELECT * FROM {S}.G1 ORDER BY id")
        base.probe(cur2, "read_other_connection", f"SELECT COUNT(*) FROM {S}.G1")
        base.probe(cur2, "catalog_type", "SELECT TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES "
                   f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'G1'")
        base.probe(cur, "self_join", f"SELECT COUNT(*) FROM {S}.G1 a JOIN {S}.G1 b ON a.id = b.id")
        cur.execute(f"CREATE GLOBAL TEMPORARY TABLE {S}.G2 (id INT, s VARCHAR(50), d DOUBLE)")
        big = [[i, f"s{i}", i * 0.5] for i in range(20000)]
        t0 = time.perf_counter()
        cur.executemany(f"INSERT INTO {S}.G2 VALUES (?,?,?)", big)
        base.results["executemany_20000_rows_seconds"] = {"result": round(time.perf_counter() - t0, 3)}
        base.probe(cur, "count_20000", f"SELECT COUNT(*) FROM {S}.G2")
        base.probe(cur, "drop_while_other_connection_open", f"DROP TABLE {S}.G1")
        c1.close()
        base.probe(cur2, "rows_after_owner_closed", f"SELECT COUNT(*) FROM {S}.G2")
    finally:
        for t in ("G1", "G2"):
            base.run(cur2, f"DROP TABLE {S}.{t}", fetch=False)
        c2.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
