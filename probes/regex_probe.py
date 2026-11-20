"""Probe of regular expressions in IRIS SQL for release 0.3, 2026-11-20 (compiler spec §7.16, §12.8).

IRIS SQL has no regex functions. This probe records what does exist (%MATCHES, %PATTERN), whether
ObjectScript's ICU regex ($LOCATE, %Regex.Matcher) can be reached from SQL through CREATE FUNCTION ...
LANGUAGE OBJECTSCRIPT, how NULL and '' reach such a function, the ICU dialect's differences from Java's,
speed, and how IRIS evaluates NOT over a user function's NULL result.

Usage:
    python probes/regex_probe.py > probes/results-2026-11-20-regex-iris-2026.2.json
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

S = base.SCHEMA
T = f"{S}.Rx"
FUNCS = {
    "PArg": ("s VARCHAR(100)", "VARCHAR(100)", 'quit $select(s="":"<null>",s=$c(0):"<c0>",1:"len "_$length(s))'),
    "PFind": ("s VARCHAR(100), p VARCHAR(100)", "INT", 'quit:s="" "" quit $locate($select(s=$c(0):"",1:s),p)>0'),
    "PNew": ("s VARCHAR(100), p VARCHAR(100)", "INT",
             'set m=##class(%Regex.Matcher).%New(p,$select(s=$c(0):"",1:s)) quit m.Locate()'),
    "PCached": ("s VARCHAR(100), p VARCHAR(100)", "INT",
                'if $get(%pRx("p"))\'=p { set %pRx("m")=##class(%Regex.Matcher).%New(p), %pRx("p")=p }\n'
                ' set m=%pRx("m"), m.Text=$select(s=$c(0):"",1:s) quit m.Locate()'),
    "PRepl": ("s VARCHAR(100), p VARCHAR(100), r VARCHAR(100)", "VARCHAR(100)",
              'quit:s="" "" set m=##class(%Regex.Matcher).%New(p,$select(s=$c(0):"",1:s)) quit m.ReplaceAll(r)'),
}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}

    def cleanup():
        for name in FUNCS:
            base.run(cur, f"DROP FUNCTION {S}.{name}", fetch=False)
        base.run(cur, f"DROP TABLE {T}", fetch=False)
        base.run(cur, f"DROP TABLE {S}.RxBig", fetch=False)

    cleanup()
    try:
        cur.execute(f"CREATE TABLE {T} (id INT, s VARCHAR(50))")
        cur.executemany(f"INSERT INTO {T} VALUES (?,?)", [[1, "abc"], [2, "ABC"], [3, "a1c"], [4, None], [5, ""],
                                                           [6, "é1"]])
        base.probe(cur, "match_in_sql", "SELECT $MATCH('abc', 'a.c')")
        base.probe(cur, "locate_in_sql", "SELECT $LOCATE('abc', 'b')")
        base.probe(cur, "matches_star", f"SELECT id FROM {T} WHERE s %MATCHES 'a*' ORDER BY id")
        base.probe(cur, "matches_class", f"SELECT id FROM {T} WHERE s %MATCHES 'a[0-9]c' ORDER BY id")
        base.probe(cur, "matches_dot", f"SELECT id FROM {T} WHERE s %MATCHES 'a.c' ORDER BY id")
        for name, (params, returns, body) in FUNCS.items():
            base.probe(cur, f"create.{name}", f"CREATE FUNCTION {S}.{name}({params}) RETURNS {returns} "
                       f"LANGUAGE OBJECTSCRIPT {{\n {body}\n}}")
        base.probe(cur, "null_and_empty_arguments", f"SELECT id, {S}.PArg(s) FROM {T} ORDER BY id")
        base.probe(cur, "find", f"SELECT id, {S}.PFind(s, 'b') FROM {T} ORDER BY id")
        base.probe(cur, "find_case_flag", f"SELECT id FROM {T} WHERE {S}.PFind(s, '(?i)abc') = 1 ORDER BY id")
        base.probe(cur, "replace_groups", f"SELECT {S}.PRepl('2026-01-31', '(\\d+)-(\\d+)-(\\d+)', '$3/$2/$1')")
        base.probe(cur, "replace_empty_matches", f"SELECT {S}.PRepl('abc', 'x*', '-')")
        base.probe(cur, "replace_escaped_dollar", f"SELECT {S}.PRepl('abc', 'b', '\\$1')")
        base.probe(cur, "unicode_word_class", f"SELECT {S}.PRepl('é1', '\\w', '_'), {S}.PFind('é', '\\p{{Alpha}}')")
        base.probe(cur, "empty_pattern", f"SELECT {S}.PFind('abc', '')")
        base.probe(cur, "empty_pattern_noncapturing", f"SELECT {S}.PFind('abc', '(?:)')")
        base.probe(cur, "invalid_pattern", f"SELECT {S}.PFind('a', '(')")
        base.probe(cur, "not_over_null_result", f"SELECT id FROM {T} WHERE NOT ({S}.PFind(s, 'b') = 1) ORDER BY id")
        base.probe(cur, "not_over_null_result_in_case",
                   f"SELECT id FROM {T} WHERE NOT ((CASE WHEN 1 = 0 THEN NULL ELSE {S}.PFind(s, 'b') END) = 1) "
                   "ORDER BY id")
        cur.execute(f"CREATE TABLE {S}.RxBig (id INT, s VARCHAR(50))")
        cur.executemany(f"INSERT INTO {S}.RxBig VALUES (?,?)", [[i, f"item-{i}-x{i % 97}"] for i in range(100000)])
        for name in ("PNew", "PCached", "PFind"):
            t0 = time.perf_counter()
            cur.execute(f"SELECT COUNT(*) FROM {S}.RxBig WHERE {S}.{name}(s, '-9[0-9]*-') = 1")
            count = cur.fetchall()[0][0]
            base.results[f"speed_100000_rows.{name}"] = {"result": [count, round(time.perf_counter() - t0, 3)]}
        t0 = time.perf_counter()
        cur.execute(f"SELECT COUNT(*) FROM {S}.RxBig WHERE s LIKE '%-9%'")
        base.results["speed_100000_rows.LIKE"] = {"result": [cur.fetchall()[0][0], round(time.perf_counter() - t0, 3)]}
    finally:
        cleanup()
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
