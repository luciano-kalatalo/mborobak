"""Follow-up probe for compiler-spec open questions O1, O2, O3 (and O5).

O1  Where a column's collation can be read.
O2  A runtime-only error trigger for guards, and whether the error says which guard fired.
O3  An IRIS SQL expression that accepts integer text and rejects decimals/exponents.
O5  How -0.0 and NaN group, compare and sort.

Reuses the connection and helpers of iris_behavior_probe.py. Everything lives in
the MborobakProbe schema and is dropped at the end.

Usage:
    python probes/compiler_followup_probe.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

SCHEMA = base.SCHEMA
results = base.results
run, probe, record = base.run, base.probe, base.record

TABLES = ("Coll", "G", "P", "F")


def drop_all(cur) -> None:
    for t in TABLES:
        run(cur, f"DROP TABLE {SCHEMA}.{t}", fetch=False)


def column_names(cur, sql: str) -> list[str] | str:
    try:
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        cur.fetchall()
        return names
    except Exception as exc:  # noqa: BLE001
        return f"error: {str(exc).splitlines()[0][:200]}"


# --------------------------------------------------------------------- O1
def o1_collation(cur) -> None:
    ddl = {
        "default": "s_default VARCHAR(20)",
        "exact_kw": "s_exact VARCHAR(20) %EXACT",
        "collate_exact": "s_cexact VARCHAR(20) COLLATE %EXACT",
        "sqlstring": "s_sqlstring VARCHAR(20) COLLATE SQLSTRING",
        "truncate": "s_trunc VARCHAR(20) COLLATE %TRUNCATE(5)",
    }
    # Create one table per variant first, to see which DDL forms are accepted.
    for key, col in ddl.items():
        record(f"o1.ddl.{key}", col,
               run(cur, f"CREATE TABLE {SCHEMA}.Coll (id INT, {col})", fetch=False))
        run(cur, f"DROP TABLE {SCHEMA}.Coll", fetch=False)

    accepted = [c for k, c in ddl.items() if results[f"o1.ddl.{k}"]["status"] == "ok"]
    run(cur, f"CREATE TABLE {SCHEMA}.Coll (id INT, {', '.join(accepted)})", fetch=False)
    results["o1.table_columns"] = {"result": accepted}

    probe(cur, "o1.info_columns.collation_name",
          "SELECT COLUMN_NAME, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS "
          "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'Coll' ORDER BY ORDINAL_POSITION", [SCHEMA])
    results["o1.info_columns.all_column_names"] = {"result": column_names(
        cur, "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE 1 = 0")}
    results["o1.info_tables.all_column_names"] = {"result": column_names(
        cur, "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 1 = 0")}
    probe(cur, "o1.info_tables.classname",
          "SELECT CLASSNAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'Coll'",
          [SCHEMA])

    cls = results["o1.info_tables.classname"]
    classname = cls["result"][0][0] if cls["status"] == "ok" and cls["result"] else None
    results["o1.classname"] = {"result": classname}
    if not classname:
        return

    results["o1.compiled_property.all_column_names"] = {"result": column_names(
        cur, "SELECT * FROM %Dictionary.CompiledProperty WHERE 1 = 0")}
    probe(cur, "o1.compiled_property.collation",
          "SELECT Name, Collation, SqlFieldName FROM %Dictionary.CompiledProperty "
          "WHERE parent = ? ORDER BY Name", [classname])
    probe(cur, "o1.compiled_property.parameters_table",
          "SELECT parent, Name, Parameters FROM %Dictionary.CompiledProperty_Parameters "
          "WHERE parent %STARTSWITH ? ORDER BY parent", [classname])
    probe(cur, "o1.compiled_property.parameters_child",
          "SELECT p.Name, x.element_key, x.Parameters "
          "FROM %Dictionary.CompiledProperty p "
          "JOIN %Dictionary.CompiledProperty_Parameters x ON x.CompiledProperty = p.ID "
          "WHERE p.parent = ? ORDER BY p.Name", [classname])
    probe(cur, "o1.property_definition.collation",
          "SELECT Name, Collation FROM %Dictionary.PropertyDefinition WHERE parent = ? ORDER BY Name",
          [classname])


# --------------------------------------------------------------------- O2
def o2_error_trigger(cur) -> None:
    run(cur, f"CREATE TABLE {SCHEMA}.G (id INT, v INT, s VARCHAR(20))", fetch=False)
    for r in [(1, 5, "1"), (2, 0, "x"), (3, 2, None)]:
        cur.execute(f"INSERT INTO {SCHEMA}.G (id, v, s) VALUES (?,?,?)", list(r))
    g = f"{SCHEMA}.G"

    triggers = {
        "div_literal": "1/0",
        "div_expr": "1/(v-v)",
        "div_param": "1/?",
        "log_zero": "LOG(0)",
        "sqrt_neg": "SQRT(-1)",
        "to_date_tagged": "TO_DATE('mborobak_guard_7', 'YYYY-MM-DD')",
        "cast_date_tagged": "CAST('mborobak_guard_7' AS DATE)",
        "to_number_tagged": "TO_NUMBER('mborobak_guard_7')",
        "char_neg": "CHAR(-1)",
        "repeat_neg": "REPEAT('x', -1)",
    }
    for key, expr in triggers.items():
        params = [0] if "?" in expr else None
        # Never taken: must not fail at prepare time or for rows that skip the branch.
        never = f"SELECT id, CASE WHEN v = 99 THEN {expr} ELSE v END FROM {g} ORDER BY id"
        probe(cur, f"o2.{key}.branch_not_taken", never, params)
        # Taken for row id=2 only.
        taken = f"SELECT id, CASE WHEN v = 0 THEN {expr} ELSE v END FROM {g} ORDER BY id"
        probe(cur, f"o2.{key}.branch_taken", taken, params)
        # Same, inside a WHERE, to check the guard also works in predicates.
        where = f"SELECT id FROM {g} WHERE (CASE WHEN v = 0 THEN {expr} ELSE v END) > 0 ORDER BY id"
        probe(cur, f"o2.{key}.in_where", where, params)

    # Full guard as the spec renders it.
    probe(cur, "o2.division_guard_full",
          f"SELECT id, CASE WHEN v = 0 THEN 1/0 ELSE 10 / v END FROM {g} WHERE id <> 2 ORDER BY id")


# --------------------------------------------------------------------- O3
INT_SAMPLES = ["1", "-2", "+3", "007", "1.5", "1e3", " 1", "1 ", "", "abc", "1-", "-",
               "+", "12345678901234567890", "1.", ".5", "0x1F", "１"]


def o3_integer_pattern(cur) -> None:
    run(cur, f"CREATE TABLE {SCHEMA}.P (id INT, s VARCHAR(40))", fetch=False)
    for i, s in enumerate(INT_SAMPLES):
        cur.execute(f"INSERT INTO {SCHEMA}.P (id, s) VALUES (?, ?)", [i, s])
    p = f"{SCHEMA}.P"

    candidates = {
        "pattern_signed": "TRIM(s) %PATTERN '0.1(1\"-\",1\"+\")1.N'",
        "pattern_unsigned": "TRIM(s) %PATTERN '1.N'",
        "isnumeric_trim": "ISNUMERIC(TRIM(s)) = 1",
        "like_no_dot_e": "TRIM(s) NOT LIKE '%.%' AND UPPER(TRIM(s)) NOT LIKE '%E%'",
    }
    for key, cond in candidates.items():
        probe(cur, f"o3.{key}",
              f"SELECT s, CASE WHEN {cond} THEN 1 ELSE 0 END FROM {p} ORDER BY id")

    # The full cast rendering from spec §7.7, with a branch that returns -1 instead of failing.
    probe(cur, "o3.full_cast_rendering",
          "SELECT s, CASE WHEN ISNUMERIC(TRIM(s)) = 1 AND TRIM(s) %PATTERN '0.1(1\"-\",1\"+\")1.N' "
          "THEN CAST(TRIM(s) AS INTEGER) WHEN s IS NULL THEN NULL ELSE -1 END "
          f"FROM {p} ORDER BY id")
    probe(cur, "o3.cast_bigint_overflow_text",
          "SELECT CAST('12345678901234567890' AS BIGINT), CAST('99999999999' AS INTEGER)")
    probe(cur, "o3.pattern_with_param",
          f"SELECT s FROM {p} WHERE TRIM(s) %PATTERN ? ORDER BY id", ["0.1(1\"-\",1\"+\")1.N"])


# --------------------------------------------------------------------- O5
def o5_float_special(cur) -> None:
    run(cur, f"CREATE TABLE {SCHEMA}.F (id INT, d DOUBLE)", fetch=False)
    vals = [0.0, -0.0, float("nan"), float("nan"), 1.0, float("inf"), float("-inf"), None]
    for i, v in enumerate(vals):
        record(f"o5.insert_{i}", repr(v),
               run(cur, f"INSERT INTO {SCHEMA}.F (id, d) VALUES (?, ?)", [i, v], fetch=False))
    f = f"{SCHEMA}.F"
    probe(cur, "o5.raw", f"SELECT id, d FROM {f} ORDER BY id")
    probe(cur, "o5.group_by", f"SELECT d, COUNT(*) FROM {f} GROUP BY d")
    probe(cur, "o5.distinct", f"SELECT DISTINCT d FROM {f}")
    probe(cur, "o5.order_asc", f"SELECT id, d FROM {f} ORDER BY d, id")
    probe(cur, "o5.order_desc", f"SELECT id, d FROM {f} ORDER BY d DESC, id")
    probe(cur, "o5.zero_eq_negzero", f"SELECT id FROM {f} WHERE d = 0 ORDER BY id")
    probe(cur, "o5.nan_self_join",
          f"SELECT a.id, b.id FROM {f} a JOIN {f} b ON a.d = b.d WHERE a.id IN (2, 3) ORDER BY a.id, b.id")
    probe(cur, "o5.gt_inf", f"SELECT id FROM {f} WHERE d > 1e300 ORDER BY id")
    probe(cur, "o5.min_max_sum", f"SELECT MIN(d), MAX(d), COUNT(d) FROM {f}")


DOUBLE_MAX = "1.7976931348623157E308"
DOUBLE_SORT_BUCKET = (
    f"CASE WHEN d IS NULL THEN 0 WHEN d < -{DOUBLE_MAX} THEN 1 WHEN d <> d THEN 4 "
    f"WHEN d > {DOUBLE_MAX} THEN 3 ELSE 2 END"
)


def o5b_double_sentinel(cur) -> None:
    """DOUBLE -1E19 behaves as NULL in sorts/grouping; check the sort-key workaround."""
    f = f"{SCHEMA}.F"
    run(cur, f"DROP TABLE {f}", fetch=False)
    run(cur, f"CREATE TABLE {f} (id INT, d DOUBLE)", fetch=False)
    vals = [None, -1e19, -1e300, -1e20, -1.0, 0.0, -0.0, 2.5, 1e19, 1e300,
            float("inf"), float("-inf"), float("nan"), None]
    for i, v in enumerate(vals):
        cur.execute(f"INSERT INTO {f} (id, d) VALUES (?, ?)", [i, v])
    probe(cur, "o5b.order_by_plain", f"SELECT id, d FROM {f} ORDER BY d, id")
    probe(cur, "o5b.order_by_plus_zero", f"SELECT id, d FROM {f} ORDER BY d + 0, id")
    probe(cur, "o5b.order_by_bucket_asc",
          f"SELECT id, d FROM {f} ORDER BY {DOUBLE_SORT_BUCKET}, d + 0, id")
    probe(cur, "o5b.order_by_bucket_desc",
          f"SELECT id, d FROM {f} ORDER BY {DOUBLE_SORT_BUCKET} DESC, d + 0 DESC, id")
    probe(cur, "o5b.group_by", f"SELECT d, COUNT(*) FROM {f} GROUP BY d")
    probe(cur, "o5b.group_by_plus_zero", f"SELECT d + 0, COUNT(*) FROM {f} GROUP BY d + 0")
    probe(cur, "o5b.distinct", f"SELECT DISTINCT d FROM {f}")
    probe(cur, "o5b.count_distinct", f"SELECT COUNT(DISTINCT d) FROM {f}")
    probe(cur, "o5b.where_eq", f"SELECT id FROM {f} WHERE d = -1E19")
    probe(cur, "o5b.nan_where_plain", f"SELECT id FROM {f} WHERE d <> d")
    probe(cur, "o5b.nan_where_case", f"SELECT id FROM {f} WHERE CASE WHEN d <> d THEN 1 ELSE 0 END = 1")


def o2_error_detail(cur) -> None:
    """Full driver message for a runtime division by zero (the first line alone hides %msg)."""
    g = f"{SCHEMA}.G"
    try:
        cur.execute(f"SELECT 10 / v FROM {g}")
        cur.fetchall()
        results["o2.error_detail"] = {"status": "ok", "result": None}
    except Exception as exc:  # noqa: BLE001
        results["o2.error_detail"] = {"status": "error", "result": str(exc)}
    probe(cur, "o2.connection_usable_after_error", "SELECT 1")
    probe(cur, "o2.null_divided_by_zero", f"SELECT NULL / v FROM {g} WHERE v = 0")
    probe(cur, "o2.length_trigger_not_taken",
          f"SELECT id, CASE WHEN s = 'zz' THEN 1/(LENGTH(s)-LENGTH(s)) ELSE 1 END FROM {g} ORDER BY id")
    probe(cur, "o2.length_trigger_taken",
          f"SELECT id, CASE WHEN s = 'x' THEN 1/(LENGTH(s)-LENGTH(s)) ELSE 1 END FROM {g} ORDER BY id")


def o1_sql_collation(cur, classname: str | None) -> None:
    if classname:
        probe(cur, "o1.compiled_property.sqlcollation",
              "SELECT SqlFieldName, SqlCollation, Type FROM %Dictionary.CompiledProperty "
              "WHERE parent = ? ORDER BY SqlFieldName", [classname])
        # Parameters is a flat "KEY,value,KEY,value" string; COLLATION is empty for the default.
        status, rows = run(cur, "SELECT SqlFieldName, Parameters FROM %Dictionary.CompiledProperty "
                                "WHERE parent = ? ORDER BY SqlFieldName", [classname])
        parsed = None
        if status == "ok":
            parsed = {}
            for name, params in rows:
                items = (params or "").split(",")
                pairs = dict(zip(items[0::2], items[1::2]))
                if "COLLATION" in pairs:
                    parsed[name] = pairs["COLLATION"] or "<default>"
        results["o1.compiled_property.parameters_collation"] = {"status": status, "result": parsed}


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    results["meta.version"] = {"result": cur.fetchall()[0][0]}
    drop_all(cur)
    try:
        o1_collation(cur)
        o1_sql_collation(cur, results["o1.classname"]["result"])
        o2_error_trigger(cur)
        o2_error_detail(cur)
        o3_integer_pattern(cur)
        o5_float_special(cur)
        o5b_double_sentinel(cur)
    finally:
        drop_all(cur)
        conn.close()
    print(json.dumps(results, indent=1, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
