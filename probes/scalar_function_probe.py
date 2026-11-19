"""Probe of IRIS scalar functions for release 0.2 (null, string and math functions), 2026-11-19.

Follows function_probe.py with the edge cases that decide how each Spark function compiles: empty
strings versus NULL in function results, collation inside functions on a default (SQLUPPER)
column, NaN and infinity in math functions, errors for out-of-domain input, and functions whose
arguments arrive as ``?`` parameters (Mborobak sends every literal as a parameter).

Usage:
    python probes/scalar_function_probe.py > probes/results-2026-11-19-scalar-functions-iris-2026.2.json
"""

from __future__ import annotations

import decimal
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

T = f"{base.SCHEMA}.F"
D = decimal.Decimal
NAN, INF = float("nan"), float("inf")
# s has IRIS's default collation (SQLUPPER); x is declared EXACT.
ROWS = [
    [1, "Hello World", "Hello World", 2.5, D("2.5"), 7],
    [2, "", "", -2.5, D("-2.5"), -7],
    [3, None, None, None, None, None],
    [4, "abc", "ABC", NAN, D("1.005"), 0],
    [5, "  pad  ", "\tx\t", INF, D("0.285"), 2147483647],
    [6, "hello world-foo bar", "aBc", -INF, D("99.5"), 3],
    [7, "ab", "b", 0.285, D("-0.5"), -1],
]

C = {
    # empty string versus NULL in results
    "empty.literal": "SELECT '' AS e, LENGTH('') AS l, CHAR_LENGTH('') AS cl, CASE WHEN '' IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.column": f"SELECT id, s, CHAR_LENGTH(s), CASE WHEN s IS NULL THEN 1 ELSE 0 END FROM {T} WHERE id IN (2, 3) ORDER BY id",
    "empty.substring_past_end": "SELECT SUBSTRING('abc', 5, 2) AS r, CASE WHEN SUBSTRING('abc', 5, 2) IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.substring_len0": "SELECT SUBSTRING('abc', 1, 0) AS r, CASE WHEN SUBSTRING('abc', 1, 0) IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.trim_spaces": "SELECT TRIM('   ') AS r, CASE WHEN TRIM('   ') IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.replace_all": "SELECT REPLACE('aa', 'a', '') AS r, CASE WHEN REPLACE('aa', 'a', '') IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.upper_of_empty_column": f"SELECT UPPER(s), CASE WHEN UPPER(s) IS NULL THEN 1 ELSE 0 END, LOWER(s), TRIM(s) FROM {T} WHERE id = 2",
    "empty.concat": f"SELECT s || 'x', CONCAT(s, 'x'), 'a' || '' FROM {T} WHERE id = 2",
    "empty.concat_null": f"SELECT s || 'x', CONCAT(s, 'x'), CONCAT('x', s) FROM {T} WHERE id = 3",
    "empty.case_else_empty": "SELECT CASE WHEN 1 = 0 THEN 'a' ELSE '' END AS r, CASE WHEN (CASE WHEN 1 = 0 THEN 'a' ELSE '' END) IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.param": "SELECT ? AS r, CASE WHEN ? IS NULL THEN 1 ELSE 0 END AS isnull",
    "empty.substring_param": "SELECT SUBSTRING(?, ?, ?) AS r, CASE WHEN SUBSTRING(?, ?, ?) IS NULL THEN 1 ELSE 0 END AS isnull",
    # case and trimming
    "trim.tab": f"SELECT id, '[' || TRIM(x) || ']', '[' || LTRIM(s) || ']', '[' || RTRIM(s) || ']' FROM {T} WHERE id = 5",
    "trim.both_keyword": "SELECT TRIM(BOTH ' ' FROM '  a  '), TRIM(LEADING 'x' FROM 'xxaxx')",
    "case.upper_lower_sqlupper_column": f"SELECT id, s, UPPER(s), LOWER(s) FROM {T} WHERE id IN (1, 6) ORDER BY id",
    "case.non_ascii": "SELECT UPPER('straße ção'), LOWER('ÇÃO')",
    # collation inside functions (s is SQLUPPER)
    "coll.instr": f"SELECT id, INSTR(s, 'h'), INSTR(s, 'H'), POSITION('h' IN s) FROM {T} WHERE id = 1",
    "coll.replace": f"SELECT REPLACE(s, 'h', 'J'), REPLACE(s, 'H', 'J') FROM {T} WHERE id = 1",
    "coll.nullif": f"SELECT id, NULLIF(s, 'HELLO WORLD'), NULLIF(s, 'Hello World') FROM {T} WHERE id = 1",
    "coll.greatest": f"SELECT GREATEST(s, 'ABC'), LEAST(s, 'ABC') FROM {T} WHERE id = 4",
    "coll.coalesce_value": f"SELECT COALESCE(s, 'z') FROM {T} WHERE id = 6",
    "coll.where_on_function": f"SELECT id FROM {T} WHERE LOWER(s) = 'HELLO WORLD'",
    "coll.where_on_function_exact": f"SELECT id FROM {T} WHERE %EXACT(LOWER(s)) = 'HELLO WORLD'",
    # positions and padding
    "pos.instr_start": "SELECT INSTR('abcabc', 'b', 3), INSTR('abc', 'z'), INSTR('abc', ''), INSTR('', 'a')",
    "pos.substring_negative": "SELECT SUBSTRING('Hello', -3, 2), SUBSTRING('Hello', 0, 2), SUBSTRING('Hello', 2)",
    "pad.lpad_truncate": "SELECT LPAD('abcdef', 3, '*'), LPAD('ab', 5, 'xy'), LPAD('ab', 5)",
    "pad.rpad": "SELECT RPAD('ab', 5, 'xy'), RPAD('abcdef', 3, '*')",
    "pad.params": "SELECT LPAD(?, ?, ?), RPAD(?, ?, ?)",
    "str.reverse": "SELECT REVERSE('abc')",
    "str.initcap_candidates": "SELECT INITCAP('hello wORLD')",
    "str.replace_empty_search": "SELECT REPLACE('abc', '', 'x')",
    "str.repeat": "SELECT REPEAT('ab', 3)",
    "str.concat_many": "SELECT CONCAT('a', 'b', 'c')",
    "str.char_length_unicode": "SELECT CHAR_LENGTH('ção'), CHAR_LENGTH('a b ')",
    "str.cast_int_to_varchar": "SELECT 'n' || CAST(-12 AS VARCHAR(20)), 'n' || -12",
    # null functions
    "null.nvl_ifnull": f"SELECT id, NVL(s, 'n'), IFNULL(s, 'n') FROM {T} WHERE id IN (2, 3) ORDER BY id",
    "null.ifnull3": f"SELECT id, IFNULL(s, 'was null', 'not null') FROM {T} WHERE id IN (1, 3) ORDER BY id",
    "null.greatest_with_null": "SELECT GREATEST(1, NULL, 3), LEAST(1, NULL, 3), GREATEST(NULL, NULL)",
    "null.greatest_types": f"SELECT GREATEST(i, 1.5), GREATEST(n, 2), GREATEST(d, 0) FROM {T} WHERE id = 2",
    "null.coalesce_mixed": f"SELECT COALESCE(i, n), COALESCE(d, 0) FROM {T} WHERE id = 3",
    "null.isnan": f"SELECT id, CASE WHEN d <> d THEN 1 ELSE 0 END FROM {T} ORDER BY id",
    "null.nullif_double_nan": f"SELECT NULLIF(d, d) FROM {T} WHERE id = 4",
    # rounding
    "round.decimal": f"SELECT id, n, ROUND(n, 0), ROUND(n, 2), ROUND(n, -1) FROM {T} ORDER BY id",
    "round.double": f"SELECT id, d, ROUND(d, 0), ROUND(d, 2) FROM {T} WHERE id IN (1, 2, 7) ORDER BY id",
    "round.double_nan_inf": f"SELECT id, ROUND(d, 0) FROM {T} WHERE id IN (4, 5, 6) ORDER BY id",
    "round.int": f"SELECT id, ROUND(i, 0), ROUND(i, -1) FROM {T} WHERE id IN (1, 2, 5) ORDER BY id",
    "round.param_scale": f"SELECT ROUND(n, ?) FROM {T} WHERE id = 1",
    "round.truncate": f"SELECT TRUNCATE(n, 0), TRUNCATE(n, 1) FROM {T} WHERE id = 6",
    # ceil, floor, abs
    "ceil.decimal": f"SELECT id, CEILING(n), FLOOR(n) FROM {T} WHERE id IN (1, 2, 4) ORDER BY id",
    "ceil.double": f"SELECT id, CEILING(d), FLOOR(d), ABS(d) FROM {T} WHERE id IN (1, 2, 4, 5, 6) ORDER BY id",
    "abs.int_min": "SELECT ABS(CAST(-2147483648 AS INTEGER))",
    # power, sqrt, exp, log
    "pow.basic": f"SELECT POWER(2, 10), POWER(2.0, 0.5), POWER(-8, 1.0/3), POWER(0, -1) FROM {T} WHERE id = 1",
    "pow.overflow": "SELECT POWER(CAST(10 AS DOUBLE), 400)",
    "pow.int_overflow": "SELECT POWER(10, 400)",
    "sqrt.negative": "SELECT SQRT(-1)",
    "sqrt.double_negative": "SELECT SQRT(CAST(-1 AS DOUBLE))",
    "sqrt.nan_inf": f"SELECT id, SQRT(d) FROM {T} WHERE id IN (4, 5) ORDER BY id",
    "exp.basic": "SELECT EXP(1), EXP(CAST(1 AS DOUBLE))",
    "exp.overflow": "SELECT EXP(CAST(1000 AS DOUBLE))",
    "exp.overflow_numeric": "SELECT EXP(1000)",
    "log.zero": "SELECT LOG(0)",
    "log.negative": "SELECT LOG(-1)",
    "log.basic": "SELECT LOG(10), LN(10), LOG10(1000), LOG2(8)",
    "log.two_args": "SELECT LOG(2, 8)",
    "log.double": f"SELECT id, LOG(d) FROM {T} WHERE id IN (1, 5) ORDER BY id",
    "sign": f"SELECT id, SIGN(d), SIGN(n) FROM {T} WHERE id IN (1, 2, 4) ORDER BY id",
    "mod.negative": "SELECT MOD(-7, 3), MOD(7, -3)",
    # second round: one statement per failing case
    "pow.int": "SELECT POWER(2, 10)",
    "pow.fraction": "SELECT POWER(2.0, 0.5)",
    "pow.negative_base_fraction": "SELECT POWER(-8, 0.5)",
    "pow.zero_negative": "SELECT POWER(0, -1)",
    "pow.double": f"SELECT id, POWER(d, 2) FROM {T} WHERE id IN (1, 2) ORDER BY id",
    "pow.double_nan": f"SELECT POWER(d, 2) FROM {T} WHERE id = 4",
    "pow.double_inf": f"SELECT POWER(d, 2) FROM {T} WHERE id = 5",
    "pow.double_cast": "SELECT POWER(CAST(2 AS DOUBLE), CAST(0.5 AS DOUBLE))",
    "pow.exp_ln_form": "SELECT EXP(CAST(0.5 AS DOUBLE) * LOG(CAST(2 AS DOUBLE)))",
    "log.ln": "SELECT LN(10)",
    "log.log10": "SELECT LOG10(1000)",
    "log.log2": "SELECT LOG2(8)",
    "log.nan": f"SELECT LOG(d) FROM {T} WHERE id = 4",
    "log.double_type": "SELECT LOG(CAST(10 AS DOUBLE)), LOG10(CAST(1000 AS DOUBLE))",
    "exp.nan_inf": f"SELECT id, EXP(d) FROM {T} WHERE id IN (4, 5, 6) ORDER BY id",
    "exp.near_limit": "SELECT EXP(CAST(709 AS DOUBLE))",
    "sqrt.zero": "SELECT SQRT(0), SQRT(CAST(0 AS DOUBLE))",
    "round.double_via_varchar": f"SELECT id, CAST(d AS VARCHAR(50)), ROUND(CAST(d AS VARCHAR(50)), 2) FROM {T} WHERE id IN (1, 7) ORDER BY id",
    "round.double_to_double": f"SELECT id, CAST(ROUND(d, 2) AS DOUBLE), CAST(ROUND(d, 0) AS DOUBLE) FROM {T} WHERE id IN (1, 2, 7) ORDER BY id",
    "round.double_big": "SELECT ROUND(CAST(1E300 AS DOUBLE), 2), ROUND(CAST(123456789012345678 AS DOUBLE), -2)",
    "round.half_even_numeric": "SELECT ROUND(2.5, 0), ROUND(3.5, 0), ROUND(0.125, 2)",
    "greatest.nan": f"SELECT GREATEST(d, 1), LEAST(d, 1) FROM {T} WHERE id = 4",
    "greatest.coalesce_trick": "SELECT GREATEST(COALESCE(NULL, 3, 1), COALESCE(3, NULL, 1), COALESCE(1, NULL, 3))",
    "greatest.strings_exact": f"SELECT GREATEST(%EXACT(s), 'ABC'), LEAST(%EXACT(s), 'ABC') FROM {T} WHERE id = 4",
    "greatest.cast_args": f"SELECT GREATEST(CAST(i AS DOUBLE), CAST(1.5 AS DOUBLE)) FROM {T} WHERE id = 2",
    "pad.empty_pad": "SELECT LPAD('ab', 5, ''), RPAD('ab', 5, '')",
    "pad.negative_len": "SELECT LPAD('ab', -1, 'x')",
    "initcap.separators": "SELECT INITCAP('hello-world foo_bar  baz	qux'), INITCAP('ÉCOLE élève')",
    "upper.eszett": "SELECT UPPER('ß')",
    "concat.three": "SELECT 'a' || NULL || 'c', 'a' || 'b' || 'c'",
    "nullif.exact": f"SELECT NULLIF(%EXACT(s), 'HELLO WORLD'), NULLIF(%EXACT(s), 'Hello World') FROM {T} WHERE id = 1",
    "sign.double_type": f"SELECT SIGN(d) FROM {T} WHERE id = 1",
    "mod.double": "SELECT MOD(CAST(-7.5 AS DOUBLE), 2)",
    "trunc.numeric_neg": "SELECT TRUNCATE(-2.567, 2), TRUNCATE(2.567, 0)",
    "types.of_results": f"SELECT ROUND(i, 0) AS ri, ABS(i) AS ai, CEILING(n) AS cn, FLOOR(d) AS fd, POWER(i, 2) AS pi, "
                        f"SQRT(i) AS si, CHAR_LENGTH(s) AS ls, INSTR(s, 'a') AS ins FROM {T} WHERE id = 1",
}

PARAMS = {
    "empty.param": ["", ""],
    "empty.substring_param": ["abc", 5, 2, "abc", 5, 2],
    "pad.params": ["ab", 5, "*", "ab", 5, "*"],
    "round.param_scale": [1],
}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}
    base.run(cur, f"DROP TABLE {T}", fetch=False)
    try:
        cur.execute(f"CREATE TABLE {T} (id INT, s VARCHAR(30), x VARCHAR(30) COLLATE EXACT, d DOUBLE, "
                    f"n NUMERIC(10,3), i INT)")
        cur.executemany(f"INSERT INTO {T} VALUES (?,?,?,?,?,?)", ROWS)
        for key, sql in C.items():
            base.probe(cur, key, sql, PARAMS.get(key))
        # result types as the driver describes them
        cur.execute(C["types.of_results"])
        base.results["types.description"] = {"result": [(d[0], d[1]) for d in cur.description]}
    finally:
        base.run(cur, f"DROP TABLE {T}", fetch=False)
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
