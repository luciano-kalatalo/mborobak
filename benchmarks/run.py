"""Mborobak vs. hand-written IRIS SQL, through the same DB-API driver (scope §8.3).

Usage:
    python benchmarks/run.py [--sizes 10000,100000,1000000] [--runs 5] [--warmup 2]
                             [--seed 42] [--skew 1.1] [--out benchmarks/results/<date>]

Connection from IRIS_HOST, IRIS_PORT, IRIS_NAMESPACE, IRIS_USERNAME, IRIS_PASSWORD. Writes
``environment.json``, ``raw.json`` (every measured run) and ``summary.md`` to the output directory.
Tables live in the MborobakBench schema and are dropped at the end.
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from importlib import metadata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import generate  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

import iris  # noqa: E402

from mborobak import MborobakSession  # noqa: E402
from mborobak.sql import functions as F  # noqa: E402

SCHEMA = "MborobakBench"
VARIANTS = ("native", "native_exact", "mborobak_iris", "mborobak")
SUBSTANTIAL_MS = 100  # D5


def connect():
    return iris.connect(os.environ["IRIS_HOST"], int(os.environ["IRIS_PORT"]), os.environ["IRIS_NAMESPACE"],
                        os.environ["IRIS_USERNAME"], os.environ["IRIS_PASSWORD"])


def scalar(cur, sql: str):
    try:
        cur.execute(sql)
        return cur.fetchall()[0][0]
    except Exception as exc:  # noqa: BLE001 - environment details are best effort
        return f"unavailable ({str(exc).splitlines()[0][:60]})"


def environment(cur, args) -> dict:
    def sh(cmd: list[str]) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:  # noqa: BLE001
            return "unavailable"

    dirty = sh(["git", "status", "--porcelain", "--", "src", "benchmarks"])
    return {
        "date": datetime.datetime.now().isoformat(timespec="seconds"),
        "iris_version": scalar(cur, "SELECT $ZVERSION"),
        "iris_auto_parallel": scalar(cur, "SELECT %SYSTEM_SQL.Util_GetOption('AutoParallel')"),
        "iris_auto_parallel_threshold": scalar(cur, "SELECT %SYSTEM_SQL.Util_GetOption('AutoParallelThreshold')"),
        "mborobak_commit": sh(["git", "rev-parse", "--short", "HEAD"]) + (" (with local changes)" if dirty else ""),
        "python": platform.python_version(),
        "driver": metadata.version("intersystems-irispython"),
        "os": platform.platform(),
        "cpu": sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor(),
        "cpu_count": os.cpu_count(),
        "iris_host": "local container (loopback network)" if os.environ.get("IRIS_HOST") in ("localhost", "127.0.0.1")
        else "remote",
        "seed": args.seed, "skew": args.skew, "runs": args.runs, "warmup": args.warmup,
        "caches": "warm: warm-up runs fill IRIS's cached queries and buffers and Mborobak's compiled-SQL cache; "
                  "Mborobak's first (cold) compile time is recorded separately",
    }


# --------------------------------------------------------------------------- measuring


def run_native(cur, sql: str, params: list) -> tuple[dict, list]:
    t0 = time.perf_counter()
    cur.execute(sql, params)
    t1 = time.perf_counter()
    rows = cur.fetchall()
    t2 = time.perf_counter()
    return {"total_ms": (t2 - t0) * 1000, "execute_ms": (t1 - t0) * 1000, "fetch_ms": (t2 - t1) * 1000,
            "python_ms": 0.0, "rows": len(rows)}, rows


def run_mborobak(df, action: str, session) -> tuple[dict, list]:
    t0 = time.perf_counter()
    result = df.count() if action == "count" else df.collect()
    t1 = time.perf_counter()
    m = session.lastExecution
    rows = [(result,)] if action == "count" else [tuple(r) for r in result]
    return {"total_ms": (t1 - t0) * 1000, "execute_ms": m.server_ms, "fetch_ms": m.fetch_ms,
            "python_ms": m.python_ms, "rows": len(rows), "cache_hit": m.cache_hit}, rows


def _norm(v):
    if isinstance(v, str):
        # The driver sometimes returns numbers as text (for example SUM(...) in a query with ORDER BY);
        # Mborobak converts by schema, native results are compared by value.
        try:
            v = decimal.Decimal(v)
        except decimal.InvalidOperation:
            return ("str", v)
    if isinstance(v, (decimal.Decimal, float, int)) and not isinstance(v, bool):
        f = float(v)
        return ("num", float(f"{f:.9g}")) if math.isfinite(f) else ("num", repr(f))
    if isinstance(v, (datetime.date, datetime.datetime)):
        return ("str", v.isoformat())
    return ("str", str(v)) if v is not None else ("null",)


def same_rows(a: list, b: list, ordered: bool) -> bool:
    na = [tuple(_norm(v) for v in r) for r in a]
    nb = [tuple(_norm(v) for v in r) for r in b]
    if not ordered:
        na, nb = sorted(na, key=repr), sorted(nb, key=repr)
    return na == nb


def inline(sql: str, params: list) -> str:
    """EXPLAIN takes no parameters: put the benchmark's constants into the text (for plans only)."""
    out = []
    values = iter(params)
    for part in sql.split("?"):
        out.append(part)
        try:
            v = next(values)
        except StopIteration:
            continue
        out.append(repr(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else "'" +
                   str(v).replace("'", "''") + "'")
    return "".join(out)


def explain(cur, sql: str, params: list) -> str:
    try:
        cur.execute("EXPLAIN " + inline(sql, params))
        return str(cur.fetchall()[0][0]).replace("\r\n", "\n")
    except Exception as exc:  # noqa: BLE001
        return f"EXPLAIN failed: {str(exc).splitlines()[0][:200]}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="10000,100000,1000000")
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--skew", type=float, default=1.1)
    ap.add_argument("--out", default=f"benchmarks/results/{datetime.date.today().isoformat()}")
    ap.add_argument("--plans-only", action="store_true", help="load the largest size and record plans only")
    args = ap.parse_args()
    if args.plans_only:
        args.runs, args.warmup = 0, 0
    sizes = [int(s) for s in args.sizes.split(",")]
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    conn = connect()
    cur = conn.cursor()
    env = environment(cur, args)
    if not args.plans_only:
        (out / "environment.json").write_text(json.dumps(env, indent=1))
    print(json.dumps(env, indent=1), flush=True)

    raw: list[dict] = []
    plans: dict = {}
    try:
        for n in sizes:
            load = generate.load(cur, SCHEMA, n, args.seed, args.skew)
            print(f"\n== {n} orders: loaded {load}", flush=True)
            tables = {"o": f"{SCHEMA}.Orders", "c": f"{SCHEMA}.Customers"}
            sessions = {
                "mborobak": MborobakSession.builder.connection(conn).create(),
                "mborobak_iris": MborobakSession.builder.connection(conn)
                .config("mborobak.sql.stringSemantics", "iris").create(),
            }
            for sc in SCENARIOS:
                results: dict[str, list] = {}
                for variant in VARIANTS:
                    if variant.startswith("native"):
                        sql = (sc.native if variant == "native" else sc.native_exact).format(**tables)

                        def once():
                            return run_native(cur, sql, sc.params)
                    else:
                        s = sessions[variant]
                        s.catalog.refresh()
                        df = sc.build(F, s.table(tables["o"]), s.table(tables["c"]))
                        sql = df.toSQL()[0]
                        s._sql_cache.clear()  # the first warm-up run measures a cold compile

                        def once(df=df, s=s):
                            return run_mborobak(df, sc.action, s)
                    for i in range(args.warmup):
                        timing, rows = once()
                        if i == 0 and variant.startswith("mborobak"):
                            raw.append({"size": n, "scenario": sc.name, "family": sc.family, "variant": variant,
                                        "run": "cold_compile", "python_ms": timing["python_ms"]})
                    for run in range(args.runs):
                        timing, rows = once()
                        raw.append({"size": n, "scenario": sc.name, "family": sc.family, "variant": variant,
                                    "run": run, **timing})
                    if not args.plans_only:
                        results[variant] = rows
                    if n == sizes[-1]:
                        params = df.toSQL()[1] if variant.startswith("mborobak") else sc.params
                        plans[f"{sc.name}/{variant}"] = {"sql": sql, "plan": explain(cur, sql, params)}
                if args.plans_only:
                    print(f"{sc.name:30} plans recorded", flush=True)
                    continue
                checks = {
                    "mborobak_iris_vs_native": same_rows(results["mborobak_iris"], results["native"], sc.ordered),
                    "mborobak_vs_native_exact": same_rows(results["mborobak"], results["native_exact"], sc.ordered),
                }
                raw.append({"size": n, "scenario": sc.name, "family": sc.family, "variant": "check", **checks})
                med = {v: statistics.median(r["total_ms"] for r in raw if r.get("size") == n and
                                            r.get("scenario") == sc.name and r.get("variant") == v and
                                            isinstance(r.get("run"), int)) for v in VARIANTS}
                print(f"{sc.name:30} " + "  ".join(f"{v}={med[v]:8.1f}ms" for v in VARIANTS)
                      + ("" if all(checks.values()) else f"  RESULTS DIFFER {checks}"), flush=True)
            for s in sessions.values():
                s.stop()
    finally:
        for table in ("Orders", "Customers"):
            try:
                cur.execute(f"DROP TABLE {SCHEMA}.{table}")
            except Exception:  # noqa: BLE001
                pass
        conn.close()

    (out / "plans.json").write_text(json.dumps(plans, indent=1))
    if args.plans_only:
        print(f"\nwrote {out}/plans.json", flush=True)
        return
    (out / "raw.json").write_text(json.dumps(raw, indent=1, default=str))
    (out / "summary.md").write_text(summarize(raw, env, sizes))
    print(f"\nwrote {out}/summary.md", flush=True)


# --------------------------------------------------------------------------- summary


def summarize(raw: list[dict], env: dict, sizes: list[int]) -> str:
    def med(n, sc, v, key="total_ms"):
        vals = [r[key] for r in raw if r.get("size") == n and r.get("scenario") == sc and r.get("variant") == v
                and isinstance(r.get("run"), int)]
        return statistics.median(vals) if vals else float("nan")

    def pct(a, b):
        return f"{(a / b - 1) * 100:+.0f}%" if b else "n/a"

    lines = [
        "# Benchmark summary",
        "",
        f"{env['date']} · IRIS: {env['iris_version']} · Mborobak {env['mborobak_commit']} · "
        f"Python {env['python']} · driver {env['driver']} · {env['cpu']} ({env['cpu_count']} cores) · "
        f"{env['iris_host']} · seed {env['seed']}, skew {env['skew']} · {env['warmup']} warm-up + "
        f"{env['runs']} measured runs · medians of total client time",
        "",
        "Columns: **native** = hand-written IRIS SQL (IRIS string rules); **native_exact** = the same with "
        "`%EXACT` where Mborobak adds it; **mb_iris** = Mborobak with `stringSemantics=\"iris\"`; **mb** = "
        "Mborobak default (PySpark rules). **Overhead** compares Mborobak with native SQL of the same "
        "semantics; **%EXACT cost** = native_exact vs native. ✓ = results identical.",
        "",
    ]
    names = list(dict.fromkeys(r["scenario"] for r in raw))
    for n in sizes:
        lines += [f"## {n:,} orders", "",
                  "| Scenario | Family | native ms | native_exact ms | mb_iris ms | mb ms | Overhead (iris) | "
                  "Overhead (pyspark) | %EXACT cost | Python ms | Rows | Same results |",
                  "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:-:|"]
        for sc in names:
            fam = next(r["family"] for r in raw if r["scenario"] == sc)
            nat, nex, mbi, mb = (med(n, sc, v) for v in VARIANTS)
            py = med(n, sc, "mborobak", "python_ms")
            rows = med(n, sc, "native", "rows")
            check = next((r for r in raw if r.get("size") == n and r.get("scenario") == sc
                          and r.get("variant") == "check"), {})
            ok = "✓" if check.get("mborobak_iris_vs_native") and check.get("mborobak_vs_native_exact") else "✗"
            substantial = "" if nat >= SUBSTANTIAL_MS else " ¹"
            lines.append(f"| {sc}{substantial} | {fam} | {nat:.1f} | {nex:.1f} | {mbi:.1f} | {mb:.1f} | "
                         f"{pct(mbi, nat)} | {pct(mb, nex)} | {pct(nex, nat)} | {py:.2f} | {rows:.0f} | {ok} |")
        lines.append("")
    lines += ["¹ Below the 100 ms \"substantial query\" threshold (D5): absolute overhead in ms matters more "
              "than the percentage.", ""]
    subst = []
    for n in sizes:
        for sc in names:
            nat, nex, mbi, mb = (med(n, sc, v) for v in VARIANTS)
            if nex >= SUBSTANTIAL_MS:
                subst.append(mb / nex - 1)
    if subst:
        lines.append(f"**Median overhead on substantial queries (Mborobak default vs native_exact): "
                     f"{statistics.median(subst) * 100:+.1f}%** over {len(subst)} scenario × size pairs "
                     f"(target: at most +10%, scope §8.3).")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
