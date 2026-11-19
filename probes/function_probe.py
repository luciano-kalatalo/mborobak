"""Probe of IRIS SQL functions for the POC-readiness plan (2026-11-19).

Checks window functions and the string, math and date functions Databricks users rely on, and
records where IRIS differs from Spark. Reuses iris_behavior_probe.py's connection and helpers;
the scratch table lives in MborobakProbe and is dropped at the end.

Usage:
    python probes/function_probe.py > probes/results-2026-11-19-functions-iris-2026.2.json
"""

from __future__ import annotations

import datetime
import decimal
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

T = f"{base.SCHEMA}.W"
D = decimal.Decimal
ROWS = [
    [1, "a", "Hello World ", D("2.5"), datetime.date(2026, 1, 31), datetime.datetime(2026, 1, 31, 13, 45, 10)],
    [2, "a", "abc-123", D("-2.5"), datetime.date(2026, 2, 28), datetime.datetime(2026, 2, 28, 0, 0, 0)],
    [3, "b", None, D("1.005"), None, None],
    [4, "b", "x", D("7"), datetime.date(2025, 12, 1), datetime.datetime(2025, 12, 1, 23, 59, 59)],
]

CHECKS = {
    "window.row_number": f"SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id DESC) FROM {T} ORDER BY id",
    "window.rank_dense_rank": f"SELECT id, RANK() OVER (ORDER BY g), DENSE_RANK() OVER (ORDER BY g) FROM {T} ORDER BY id",
    "window.lag_lead": f"SELECT id, LAG(id) OVER (ORDER BY id), LEAD(id, 1) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "window.running_sum": f"SELECT id, SUM(n) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
                          f"AND CURRENT ROW) FROM {T} ORDER BY id",
    "window.avg_partition": f"SELECT id, AVG(n) OVER (PARTITION BY g) FROM {T} ORDER BY id",
    "window.first_value": f"SELECT id, FIRST_VALUE(id) OVER (PARTITION BY g ORDER BY id) FROM {T} ORDER BY id",
    "window.ntile": f"SELECT id, NTILE(2) OVER (ORDER BY id) FROM {T} ORDER BY id",
    "window.filter_on_rank_in_subquery": f"SELECT * FROM (SELECT id, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) "
                                         f"AS rn FROM {T}) AS x WHERE rn = 1",
    "string.upper_lower_trim": f"SELECT UPPER(s), LOWER(s), TRIM(s), LTRIM(s), RTRIM(s) FROM {T} WHERE id = 1",
    "string.length_vs_char_length": f"SELECT LENGTH(s), CHAR_LENGTH(s), CHARACTER_LENGTH(s) FROM {T} WHERE id = 1",
    "string.substring": f"SELECT SUBSTRING(s, 1, 5), SUBSTRING(s, 7) FROM {T} WHERE id = 1",
    "string.concat": f"SELECT s || '-' || g, CONCAT(s, g) FROM {T} ORDER BY id",
    "string.replace": f"SELECT REPLACE(s, 'l', 'L') FROM {T} WHERE id = 1",
    "string.matches_pattern": f"SELECT id FROM {T} WHERE s %MATCHES '*[0-9]*'",
    "string.regex_match_function": f"SELECT id, $MATCH(s, '.*[0-9]+.*') FROM {T} ORDER BY id",
    "string.instr_position": f"SELECT INSTR(s, 'o'), POSITION('o' IN s) FROM {T} WHERE id = 1",
    "string.lpad": f"SELECT LPAD(s, 15, '*') FROM {T} WHERE id = 4",
    "null.coalesce_nullif": f"SELECT COALESCE(s, 'none'), NULLIF(g, 'a') FROM {T} ORDER BY id",
    "math.round": f"SELECT n, ROUND(n, 0), ROUND(n, 2) FROM {T} ORDER BY id",
    "math.abs_ceil_floor_power_sqrt": f"SELECT ABS(n), CEILING(n), FLOOR(n), POWER(n, 2), SQRT(ABS(n)) FROM {T} WHERE id = 2",
    "date.current": "SELECT CURRENT_DATE, CURRENT_TIMESTAMP",
    "date.parts": f"SELECT YEAR(d), MONTH(d), DAYOFMONTH(d), DAYOFWEEK(d), QUARTER(d) FROM {T} WHERE id = 1",
    "date.time_parts": f"SELECT HOUR(ts), MINUTE(ts), SECOND(ts) FROM {T} WHERE id = 1",
    "date.dateadd_datediff": f"SELECT DATEADD('dd', 5, d), DATEDIFF('dd', d, '2026-03-01'), DATEADD('mm', 1, d) "
                             f"FROM {T} WHERE id = 1",
    "date.last_day": f"SELECT LAST_DAY(d) FROM {T} WHERE id = 2",
    "date.trunc": f"SELECT TRUNC(ts, 'MM') FROM {T} WHERE id = 1",
    "date.to_date_to_char": f"SELECT TO_DATE('2026-03-15', 'YYYY-MM-DD'), TO_CHAR(d, 'YYYY/MM/DD') FROM {T} WHERE id = 1",
    "date.to_char_timestamp": f"SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI') FROM {T} WHERE id = 1",
}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    try:
        cur.execute(f"CREATE TABLE {T} (id INT, g VARCHAR(5), s VARCHAR(20), n NUMERIC(10,3), d DATE, ts TIMESTAMP)")
        cur.executemany(f"INSERT INTO {T} VALUES (?,?,?,?,?,?)", ROWS)
        for key, sql in CHECKS.items():
            base.probe(cur, key, sql)
    finally:
        base.run(cur, f"DROP TABLE {T}", fetch=False)
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
