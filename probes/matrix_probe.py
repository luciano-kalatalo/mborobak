"""Probe for compatibility-matrix rows NUM-08 and CAST-07 to CAST-14 (2026-11-18d).

Checks IRIS MOD sign rules, ISNUMERIC on NaN/Infinity text, numeric-to-integer
truncation, and the text produced by CAST(... AS VARCHAR).

Reuses the connection and helpers of iris_behavior_probe.py. The scratch table
lives in the MborobakProbe schema and is dropped at the end.

Usage:
    python probes/matrix_probe.py
"""

from __future__ import annotations

import datetime
import decimal
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

SCHEMA = base.SCHEMA
results = base.results
run, probe = base.run, base.probe
T = f"{SCHEMA}.M"


def setup(cur) -> None:
    run(cur, f"DROP TABLE {T}", fetch=False)
    run(cur, f"CREATE TABLE {T} (id INT, a INT, b INT, d DOUBLE, n NUMERIC(10,4), "
             "s VARCHAR(30), ts TIMESTAMP, dt DATE)", fetch=False)
    rows = [
        (1, -7, 3, 2.7, decimal.Decimal("1.5000"), "NaN",
         datetime.datetime(2026, 1, 2, 3, 4, 5, 123456), datetime.date(2026, 1, 2)),
        (2, 7, -3, -2.7, decimal.Decimal("-0.1000"), "Infinity", None, None),
        (3, -7, -3, 0.1, decimal.Decimal("100"), "-Infinity", None, None),
        (4, 7, 3, 1e20, None, "inf", None, None),
    ]
    for r in rows:
        cur.execute(f"INSERT INTO {T} VALUES (?,?,?,?,?,?,?,?)", list(r))


def checks(cur) -> None:
    probe(cur, "num08.mod_and_hash", f"SELECT a, b, MOD(a, b), a # b FROM {T} ORDER BY id")
    probe(cur, "cast07.isnumeric_special", f"SELECT s, ISNUMERIC(s), ISNUMERIC(TRIM(s)) FROM {T} ORDER BY id")
    probe(cur, "cast09.double_to_int", f"SELECT d, CAST(d AS INTEGER), CAST(d AS BIGINT) FROM {T} ORDER BY id")
    probe(cur, "cast09.numeric_to_int", f"SELECT n, CAST(n AS INTEGER) FROM {T} ORDER BY id")
    probe(cur, "cast12.double_to_varchar", f"SELECT d, CAST(d AS VARCHAR(40)) FROM {T} ORDER BY id")
    probe(cur, "cast11_13.other_to_varchar",
          f"SELECT CAST(n AS VARCHAR(40)), CAST(a AS VARCHAR(10)), CAST(dt AS VARCHAR(40)), "
          f"CAST(ts AS VARCHAR(40)) FROM {T} WHERE id = 1")
    probe(cur, "cast14.text_to_date_timestamp",
          "SELECT CAST('2026-01-02' AS DATE), CAST('2026-01-02 03:04:05.5' AS TIMESTAMP)")


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    results["meta.version"] = {"result": cur.fetchall()[0][0]}
    try:
        setup(cur)
        checks(cur)
    finally:
        run(cur, f"DROP TABLE {T}", fetch=False)
        conn.close()
    print(json.dumps(results, indent=1, default=str))


if __name__ == "__main__":
    main()
