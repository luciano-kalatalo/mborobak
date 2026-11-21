"""Probe of random values and sampling in IRIS SQL, 2026-11-21 (compiler spec §7.20).

IRIS SQL has no RAND. Spark's rand(seed), sample and randomSplit need a value per row that is uniform,
independent between seeds, and the same every time the query runs (randomSplit runs one query per split, and
its splits must not overlap or lose rows). This probe records which random and hash functions IRIS SQL has,
and measures, on 1M rows of the benchmark's Orders table, candidate per-row values built from a hash of the
seed and the row's columns: their speed, their uniformity, how two seeds relate, and the cost of numbering
duplicate rows so identical rows get different values.

Usage (loads MborobakSmp.Orders, about 30 s):
    python probes/sampling_probe.py > probes/results-2026-11-21-sampling-iris-2026.2.json
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
S = "MborobakSmp"
T = f'"{S}"."Orders"'
COLS = ("id", "customer_id", "status", "amount", "score", "created")


def key(seed: int, extra: str = "") -> str:
    parts = " || CHAR(1) || ".join(f'COALESCE(CAST("{c}" AS VARCHAR(60)), CHAR(2))' for c in COLS)
    return f"'{seed}' || CHAR(1) || {parts}{extra}"


HASHES = {
    "crc32": lambda seed: f"CRC32({key(seed)})",
    "crc32_md5": lambda seed: f"CRC32(MD5({key(seed)}))",
}


def timed(cur, sql: str) -> tuple[float, list]:
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    return round(time.perf_counter() - t0, 3), [list(r) for r in rows]


def main() -> None:
    import generate

    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    out = base.results
    out["meta.version"] = {"result": cur.fetchall()[0][0]}
    for name, sql in {
        "rand": "SELECT RAND()", "rand_seed": "SELECT RAND(42)", "random": "SELECT RANDOM()",
        "crc32": "SELECT CRC32('a')", "crc32_null": "SELECT CRC32(NULL)", "crc32_utf8": "SELECT CRC32('é')",
        "md5": "SELECT MD5('a')", "md5_null": "SELECT MD5(NULL)", "sha1": "SELECT SHA1('a')",
        "sha2": "SELECT SHA2('a', 256)", "cast_double_text": "SELECT CAST(CAST(0.1 AS DOUBLE) AS VARCHAR(60))",
    }.items():
        base.probe(cur, f"function.{name}", sql)

    out["meta.rows"] = {"result": N}
    out["load"] = {"result": generate.load(cur, S, N, 42, 0.0)}
    out["baseline.count_s"] = {"result": timed(cur, f"SELECT COUNT(*) FROM {T} WHERE score < 0.2")[0]}
    for name, h in HASHES.items():
        u = f"({h(42)}) / 4294967296.0"
        sec, rows = timed(cur, f"SELECT COUNT(*) FROM {T} WHERE {u} < 0.2")
        again = timed(cur, f"SELECT COUNT(*) FROM {T} WHERE {u} < 0.2")[1]
        out[f"{name}.filter_0.2"] = {"result": {"seconds": sec, "rows": rows[0][0], "same_twice": rows == again}}
        sec, rows = timed(cur, f"SELECT FLOOR(({u}) * 10) AS b, COUNT(*) FROM {T} GROUP BY FLOOR(({u}) * 10) "
                               "ORDER BY 1")
        out[f"{name}.deciles"] = {"result": rows}
        u2 = f"({h(43)}) / 4294967296.0"
        rows = timed(cur, f"SELECT SUM(CASE WHEN {u} < 0.1 AND {u2} < 0.1 THEN 1 ELSE 0 END), "
                          f"SUM(CASE WHEN {u} < 0.5 AND {u2} < 0.5 THEN 1 ELSE 0 END) FROM {T}")[1]
        out[f"{name}.two_seeds"] = {"result": {"both_below_0.1 (expect 10000)": rows[0][0],
                                               "both_below_0.5 (expect 250000)": rows[0][1]}}
    # identical rows need different values: number them within their group
    dup = f"ROW_NUMBER() OVER (PARTITION BY {', '.join(chr(34) + c + chr(34) for c in COLS)})"
    inner = f"SELECT {', '.join(chr(34) + c + chr(34) for c in COLS)}, {dup} AS mbrn FROM {T}"
    u = f"CRC32(MD5({key(42, ' || CHAR(1) || mbrn')})) / 4294967296.0"
    sec, rows = timed(cur, f"SELECT COUNT(*) FROM ({inner}) WHERE {u} < 0.2")
    out["crc32_md5.with_row_number_s"] = {"result": {"seconds": sec, "rows": rows[0][0]}}
    for table in ("Orders", "Customers"):
        base.run(cur, f"DROP TABLE {S}.{table}", fetch=False)
    conn.close()
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
