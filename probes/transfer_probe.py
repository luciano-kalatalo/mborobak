"""Probe of result transfer for the 0.3 columnar item, 2026-11-20 (scope §13, compiler spec §9.9, §12.9).

The plan was a columnar (Arrow-style) path for toPandas(). This probe measures, on 1M rows of the benchmark's
Orders table (6 columns: int, int, varchar, numeric, double, date), each candidate route against the driver:
the driver's fetch (whole and per column type), whether the driver offers anything columnar, how fast IRIS
itself iterates a query (ObjectScript dynamic SQL, embedded Python), and what Mborobak adds on top.

Usage (loads MborobakCol.Orders, about 30 s):
    python probes/transfer_probe.py > probes/results-2026-11-20-transfer-iris-2026.2.json
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT.parent / "benchmarks"))
import iris_behavior_probe as base  # noqa: E402

N = 1_000_000
S = "MborobakCol"
SQL = f'SELECT "id", "customer_id", "status", "amount", "score", "created" FROM "{S}"."Orders"'


def best(fn, runs: int = 3) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return round(min(times), 3)


def main() -> None:
    import generate
    import iris

    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    out = base.results
    out["meta.version"] = {"result": cur.fetchall()[0][0]}
    out["meta.rows"] = {"result": N}
    out["load"] = {"result": generate.load(cur, S, N, 42, 0.0)}

    # the driver
    import iris.dbapi as dbapi

    out["driver.cursor_methods"] = {"result": sorted(n for n in dir(dbapi.Cursor) if not n.startswith("_"))}

    def fetchall():
        cur.execute(SQL)
        cur.fetchall()

    out["driver.fetchall_s"] = {"result": best(fetchall)}

    def fetchmany(n):
        def f():
            cur.execute(SQL)
            while cur.fetchmany(n):
                pass
        return f

    out["driver.fetchmany_s"] = {"result": {n: best(fetchmany(n)) for n in (1000, 10000, 100000)}}
    for column in ("id", "status", "amount", "score", "created"):
        q = f'SELECT "{column}" FROM "{S}"."Orders"'

        def one(q=q):
            cur.execute(q)
            cur.fetchall()

        out[f"driver.one_column_s.{column}"] = {"result": best(one)}

    def from_records():
        import pandas as pd

        cur.execute(SQL)
        pd.DataFrame.from_records([tuple(r) for r in cur.fetchall()])

    out["driver_plus_pandas_s"] = {"result": best(from_records)}

    # Mborobak
    from mborobak import MborobakSession

    session = MborobakSession.builder.connection(conn).create()
    df = session.table(f"{S}.Orders")
    out["mborobak.toPandas_s"] = {"result": best(df.toPandas)}
    out["mborobak.collect_s"] = {"result": best(df.collect)}

    # IRIS iterating the query itself
    for name in ("MbIterate", "MbPyModules"):
        base.run(cur, f"DROP FUNCTION {S}.{name}", fetch=False)
    cur.execute(f"""CREATE FUNCTION {S}.MbIterate(n INT) RETURNS INT LANGUAGE OBJECTSCRIPT {{
 set st=##class(%SQL.Statement).%New()
 set sc=st.%Prepare("SELECT TOP "_n_" id, customer_id, status, amount, score, created FROM {S}.Orders")
 set rs=st.%Execute(), rows=0
 while rs.%Next() {{ set rows=rows+1 }}
 quit rows
}}""")
    t0 = time.perf_counter()
    cur.execute(f"SELECT {S}.MbIterate({N})")
    cur.fetchall()
    out["iris.objectscript_iterate_s"] = {"result": round(time.perf_counter() - t0, 3)}
    cur.execute(f"""CREATE FUNCTION {S}.MbPyModules() RETURNS VARCHAR(2000) LANGUAGE PYTHON {{
    import importlib, time, iris
    found = []
    for m in ('numpy', 'pyarrow', 'pandas'):
        try:
            found.append(m + ' ' + importlib.import_module(m).__version__)
        except Exception:
            found.append(m + ' missing')
    t0 = time.perf_counter()
    df = iris.sql.exec("SELECT id, customer_id, status, amount, score, created FROM {S}.Orders").dataframe()
    t1 = time.perf_counter()
    return '; '.join(found) + '; dataframe() %.3f s; amount dtype %s' % (t1 - t0, df['amount'].dtype)
}}""")
    cur.execute(f"SELECT {S}.MbPyModules()")
    out["iris.embedded_python"] = {"result": cur.fetchall()[0][0]}
    for name in ("MbIterate", "MbPyModules"):
        base.run(cur, f"DROP FUNCTION {S}.{name}", fetch=False)
    for table in ("Orders", "Customers"):
        base.run(cur, f"DROP TABLE {S}.{table}", fetch=False)
    conn.close()
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
