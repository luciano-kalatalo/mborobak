"""Probe of IRIS date and time functions for release 0.2, 2026-11-20.

function_probe.py found that DATEADD returns a timestamp, DATEDIFF takes its dates in the opposite
order to Spark, there is no DATE_TRUNC, and TO_CHAR fails on a timestamp. This probe settles the rest:
month arithmetic at month ends, ISO weeks, fractional seconds, parsing with formats and invalid input,
and the building blocks for truncation and formatting without TO_CHAR.

Usage:
    python probes/date_probe.py > probes/results-2026-11-20-dates-iris-2026.2.json
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

T = f"{base.SCHEMA}.Dt"
ROWS = [
    [1, dt.date(2026, 1, 31), dt.datetime(2026, 1, 31, 13, 45, 10, 123456), "2026-01-31"],
    [2, dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 0, 0, 0), "2024-02-29 08:30:00"],
    [3, dt.date(2026, 12, 31), dt.datetime(2026, 12, 31, 23, 59, 59, 999999), "31/12/2026"],
    [4, dt.date(2027, 1, 1), dt.datetime(2027, 1, 1, 0, 0, 1), "not a date"],
    [5, dt.date(2026, 3, 1), dt.datetime(2026, 3, 1, 12, 0, 0), "2026-3-1"],
    [6, None, None, None],
]

C = {
    "add_months_end": f"SELECT id, DATEADD('mm', 1, d), DATEADD('mm', -1, d), DATEADD('mm', 12, d) FROM {T} ORDER BY id",
    "add_days_type": f"SELECT DATEADD('dd', 1, d), CAST(DATEADD('dd', 1, d) AS DATE), DATEADD('dd', -1, ts) FROM {T} WHERE id = 1",
    "add_days_param": f"SELECT CAST(DATEADD('dd', ?, d) AS DATE) FROM {T} WHERE id = 1",
    "add_days_column": f"SELECT CAST(DATEADD('dd', id, d) AS DATE) FROM {T} WHERE id = 2",
    "datediff_days": f"SELECT id, DATEDIFF('dd', d, '2026-03-01'), DATEDIFF('dd', ts, CAST('2026-02-01 00:00:00' AS TIMESTAMP)) FROM {T} ORDER BY id",
    "datediff_dates_of_ts": f"SELECT DATEDIFF('dd', CAST(ts AS DATE), CAST(? AS DATE)) FROM {T} WHERE id = 3",
    "last_day": f"SELECT id, LAST_DAY(d), LAST_DAY(ts) FROM {T} ORDER BY id",
    "parts": f"SELECT id, YEAR(d), MONTH(d), DAYOFMONTH(d), DAYOFWEEK(d), DAYOFYEAR(d), QUARTER(d), WEEK(d) FROM {T} ORDER BY id",
    "parts_ts": f"SELECT id, HOUR(ts), MINUTE(ts), SECOND(ts), DAYOFWEEK(ts), YEAR(ts) FROM {T} ORDER BY id",
    "parts_of_date_time": f"SELECT HOUR(d), MINUTE(d) FROM {T} WHERE id = 1",
    "datepart_isoweek": f"SELECT id, DATEPART('isoweek', d) FROM {T} ORDER BY id",
    "datepart_week": f"SELECT id, DATEPART('wk', d), DATEPART('week', d) FROM {T} ORDER BY id",
    "datepart_fraction": f"SELECT DATEPART('ms', ts), DATEPART('mcs', ts), DATEPART('ns', ts) FROM {T} WHERE id = 1",
    "datepart_ms_only": f"SELECT DATEPART('ms', ts) FROM {T} WHERE id = 1",
    "second_fraction": f"SELECT SECOND(ts), DATEPART('ss', ts), DATEPART('s', ts) FROM {T} WHERE id = 1",
    "cast_ts_to_varchar": f"SELECT CAST(ts AS VARCHAR(40)), CAST(d AS VARCHAR(20)) FROM {T} WHERE id = 1",
    "cast_date_to_ts": f"SELECT CAST(d AS TIMESTAMP), CAST(ts AS DATE) FROM {T} WHERE id = 1",
    "to_date_formats": "SELECT TO_DATE('2026-01-31', 'YYYY-MM-DD'), TO_DATE('31/12/2026', 'DD/MM/YYYY'), TO_DATE('2026-3-1', 'YYYY-MM-DD')",
    "to_date_mon": "SELECT TO_DATE('31 Jan 2026', 'DD MON YYYY'), TO_DATE('January 31, 2026', 'MONTH DD, YYYY')",
    "to_date_yyyymmdd": "SELECT TO_DATE('20260131', 'YYYYMMDD')",
    "to_date_invalid": "SELECT TO_DATE('not a date', 'YYYY-MM-DD')",
    "to_date_wrong_format": "SELECT TO_DATE('31/12/2026', 'YYYY-MM-DD')",
    "to_date_bad_day": "SELECT TO_DATE('2026-02-30', 'YYYY-MM-DD')",
    "to_date_column": f"SELECT id, TO_DATE(s, 'YYYY-MM-DD') FROM {T} WHERE id IN (1, 6) ORDER BY id",
    "to_timestamp_fmt": "SELECT TO_TIMESTAMP('2026-01-31 13:45:10', 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP('31/12/2026 07:05', 'DD/MM/YYYY HH24:MI')",
    "to_timestamp_invalid": "SELECT TO_TIMESTAMP('xx', 'YYYY-MM-DD HH24:MI:SS')",
    "to_timestamp_fraction": "SELECT TO_TIMESTAMP('2026-01-31 13:45:10.123', 'YYYY-MM-DD HH24:MI:SS.FF')",
    "cast_text_to_ts": "SELECT CAST('2024-02-29 08:30:00' AS TIMESTAMP), CAST('2026-01-31' AS TIMESTAMP)",
    "current": "SELECT CURRENT_DATE, CURRENT_TIMESTAMP, NOW(), GETDATE(), CURRENT_TIMESTAMP(6)",
    "current_types": "SELECT CURRENT_DATE AS cd, CURRENT_TIMESTAMP AS ct",
    "to_char_ts_variants": f"SELECT TO_CHAR(ts, 'YYYY-MM-DD'), TO_CHAR(d, 'YYYY-MM-DD HH24:MI') FROM {T} WHERE id = 1",
    "lpad_number": f"SELECT LPAD(MONTH(d), 2, '0'), LPAD(DATEPART('ms', ts), 3, '0') FROM {T} WHERE id = 1",
    "ts_from_parts": f"SELECT DATEADD('ss', HOUR(ts) * 3600 + MINUTE(ts) * 60 + SECOND(ts), CAST(CAST(ts AS DATE) AS TIMESTAMP)) FROM {T} WHERE id = 1",
    "first_of_month": f"SELECT id, CAST(DATEADD('dd', 1 - DAYOFMONTH(d), d) AS DATE), CAST(DATEADD('dd', 1 - DAYOFYEAR(d), d) AS DATE) FROM {T} ORDER BY id",
    "dateadd_hh_on_date": f"SELECT DATEADD('hh', 13, CAST(d AS TIMESTAMP)), DATEADD('mi', 5, ts) FROM {T} WHERE id = 1",
    "date_compare_ts": f"SELECT id FROM {T} WHERE d < ts ORDER BY id",
    "date_minus_date": f"SELECT d - CAST('2026-01-01' AS DATE) FROM {T} WHERE id = 1",
    "dayname_monthname": f"SELECT DAYNAME(d), MONTHNAME(d) FROM {T} WHERE id = 1",
    "datediff_months": f"SELECT DATEDIFF('mm', d, CAST('2026-03-15' AS DATE)) FROM {T} WHERE id = 1",
    "timestampadd": f"SELECT TIMESTAMPADD(SQL_TSI_MONTH, 1, d) FROM {T} WHERE id = 1",
    "unix_epoch": f"SELECT DATEDIFF('s', CAST('1970-01-01 00:00:00' AS TIMESTAMP), ts) FROM {T} WHERE id = 1",
}
PARAMS = {"add_days_param": [5], "datediff_dates_of_ts": ["2027-01-01"]}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    try:
        cur.execute(f"CREATE TABLE {T} (id INT, d DATE, ts TIMESTAMP, s VARCHAR(30))")
        cur.executemany(f"INSERT INTO {T} VALUES (?,?,?,?)", ROWS)
        for key, sql in C.items():
            base.probe(cur, key, sql, PARAMS.get(key))
        cur.execute(C["current_types"])
        base.results["current_types.description"] = {"result": [(d[0], d[1]) for d in cur.description]}
    finally:
        base.run(cur, f"DROP TABLE {T}", fetch=False)
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
