"""Probe of IRIS DDL and INSERT … SELECT for writing tables (release 0.2.x), 2026-11-20.

``df.write.saveAsTable`` compiles to CREATE TABLE plus INSERT … SELECT, so the data never leaves IRIS.
This probe settles what that needs: column types for Spark's types (string length, collation), which
column names IRIS accepts, which SELECT forms INSERT accepts (ORDER BY, TOP, WITH, parameters), what a
failing INSERT leaves behind, whether DDL and DML roll back, and whether a table can be renamed.

The steps run in order and share state, so each key records one statement.

Usage:
    python probes/write_probe.py > probes/results-2026-11-20-write-iris-2026.2.json
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import iris_behavior_probe as base  # noqa: E402

S = base.SCHEMA
TABLES = ("Src", "Old", "Old2", "Ddl", "W10", "W11", "W1", "W2", "W3", "W4", "W5", "W6", "W7", "W8", "W9", "Stage", "Renamed", "Tx", "Tx2")
SRC_ROWS = [
    [1, "apple", 1.5, decimal.Decimal("1.25"), dt.date(2026, 1, 31), dt.datetime(2026, 1, 31, 13, 45, 10, 123456)],
    [2, "Apple", float("nan"), decimal.Decimal("-2.50"), None, None],
    [3, "  x ", float("inf"), None, dt.date(2024, 2, 29), dt.datetime(2024, 2, 29, 0, 0, 0, 1)],
    [4, None, -0.0, decimal.Decimal("99999999.99"), dt.date(1999, 12, 31), dt.datetime(1999, 12, 31, 23, 59, 59)],
    [5, "x" * 5000, None, decimal.Decimal("0.01"), dt.date(2026, 3, 1), dt.datetime(2026, 3, 1)],
]
SRC = f"{S}.Src"

STEPS: list[tuple[str, str, list | None]] = [
    # ---- string columns: length and collation
    ("varchar_no_length", f"CREATE TABLE {S}.W1 (s VARCHAR)", None),
    ("varchar_no_length.insert_long", f"INSERT INTO {S}.W1 (s) SELECT s FROM {SRC} WHERE id = 5", None),
    ("varchar_no_length.insert_short", f"INSERT INTO {S}.W1 (s) SELECT s FROM {SRC} WHERE id = 1", None),
    ("varchar_no_length.maxlen",
     f"SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{S}' "
     "AND TABLE_NAME = 'W1' AND COLUMN_NAME = 's'", None),
    ("varchar_huge", f"CREATE TABLE {S}.W2 (a VARCHAR(3641144), b VARCHAR(32767) COLLATE EXACT, "
                     "c VARCHAR(32767) COLLATE %EXACT, d VARCHAR(32767) %EXACT, e VARCHAR(32767))", None),
    ("varchar_huge.insert_long", f"INSERT INTO {S}.W2 (a, b, c, e) SELECT s, s, s, s FROM {SRC} WHERE id = 5", None),
    ("varchar_huge.read_long", f"SELECT LENGTH(a), LENGTH(b), LENGTH(e) FROM {S}.W2", None),
    ("varchar_huge.collations",
     "SELECT p.SqlFieldName, p.Parameters FROM %Dictionary.CompiledProperty p "
     f"WHERE p.parent = '{S}.W2' ORDER BY p.SqlFieldName", None),
    ("varchar_huge.collations_by_classname",
     "SELECT c.COLUMN_NAME, p.Parameters, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS c "
     "JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME "
     "LEFT JOIN %Dictionary.CompiledProperty p ON p.parent = t.CLASSNAME AND p.SqlFieldName = c.COLUMN_NAME "
     f"WHERE c.TABLE_SCHEMA = '{S}' AND c.TABLE_NAME = 'W2' ORDER BY c.ORDINAL_POSITION", None),
    ("varchar_beyond_max", f"CREATE TABLE {S}.W9 (a VARCHAR(3641145))", None),
    ("exact_compare",
     f"INSERT INTO {S}.W2 (b, e) SELECT s, s FROM {SRC} WHERE id IN (1, 2, 3)", None),
    ("exact_compare.eq_b", f"SELECT COUNT(*) FROM {S}.W2 WHERE b = 'apple'", None),
    ("exact_compare.eq_e", f"SELECT COUNT(*) FROM {S}.W2 WHERE e = 'apple'", None),
    ("exact_compare.trailing_b", f"SELECT COUNT(*) FROM {S}.W2 WHERE b = '  x'", None),
    ("exact_compare.distinct_b", f"SELECT COUNT(DISTINCT b), COUNT(DISTINCT e) FROM {S}.W2", None),

    # ---- other types: round trip of special values through INSERT … SELECT
    ("types.timestamp_precision", f"CREATE TABLE {S}.W11 (t TIMESTAMP(6))", None),
    # NUMERIC keeps at most 19 digits before the point: the catalog reports precision min(p, 19 + s)
    ("types.decimal_precisions", f"CREATE TABLE {S}.W10 (a NUMERIC(19,0), b NUMERIC(20,2), c NUMERIC(21,0), "
                                 "d NUMERIC(28,2), e NUMERIC(38,10), f NUMERIC(38,18), g NUMERIC(38,38))", None),
    ("types.decimal_precisions_catalog", "SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE FROM "
     f"INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'W10' ORDER BY ORDINAL_POSITION", None),
    ("types.decimal_20_digits", f"INSERT INTO {S}.W10 (c) VALUES (?)", [decimal.Decimal("12345678901234567890")]),
    ("types.decimal_29_digits", f"INSERT INTO {S}.W10 (e) VALUES (?)",
     [decimal.Decimal("1234567890123456789.0123456789")]),
    ("types.decimal_29_digits_read", f"SELECT e FROM {S}.W10 WHERE e IS NOT NULL", None),
    ("types", f"CREATE TABLE {S}.W3 (i INTEGER, l BIGINT, d DOUBLE, n NUMERIC(10,2), n38 NUMERIC(38,10), "
              "b BIT, dt DATE, ts TIMESTAMP)", None),
    ("types.insert", f"INSERT INTO {S}.W3 (i, l, d, n, n38, b, dt, ts) SELECT id, id * 10000000000, d, n, n, "
                     f"CASE WHEN d > 1 THEN 1 WHEN d <= 1 THEN 0 END, dt, ts FROM {SRC}", None),
    ("types.read", f"SELECT i, l, d, n, n38, b, dt, ts FROM {S}.W3 ORDER BY i", None),
    ("types.catalog",
     "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION FROM "
     f"INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'W3' ORDER BY ORDINAL_POSITION", None),
    ("types.nan_computed", f"INSERT INTO {S}.W3 (i, d) SELECT 9, CAST('NaN' AS DOUBLE) - 0 FROM {SRC} WHERE id = 1",
     None),
    ("types.nan_read", f"SELECT d FROM {S}.W3 WHERE i = 9", None),
    ("types.decimal_round", f"INSERT INTO {S}.W3 (i, n) SELECT 10, 1.005 FROM {SRC} WHERE id = 1", None),
    ("types.decimal_round_read", f"SELECT n FROM {S}.W3 WHERE i = 10", None),
    ("types.decimal_overflow", f"INSERT INTO {S}.W3 (i, n) SELECT 11, 123456789012.5 FROM {SRC} WHERE id = 1", None),
    ("types.int_overflow", f"INSERT INTO {S}.W3 (i) SELECT 12 * 1000000000 FROM {SRC} WHERE id = 1", None),
    ("types.bit_from_bool", f"INSERT INTO {S}.W3 (i, b) SELECT 13, CASE WHEN 1 = 1 THEN 1 ELSE 0 END "
                            f"FROM {SRC} WHERE id = 1", None),
    ("types.bit_from_2", f"INSERT INTO {S}.W3 (i, b) SELECT 14, 2 FROM {SRC} WHERE id = 1", None),
    ("types.text_into_int", f"INSERT INTO {S}.W3 (i) SELECT 'abc' FROM {SRC} WHERE id = 1", None),
    ("types.text_into_date", f"INSERT INTO {S}.W3 (i, dt) SELECT 15, 'not a date' FROM {SRC} WHERE id = 1", None),
    ("types.leftovers", f"SELECT i, d, n, b, dt FROM {S}.W3 WHERE i >= 9 ORDER BY i", None),

    # ---- column names
    ("names", f'CREATE TABLE {S}.W4 ("(i + 1)" INT, "count(1)" INT, "a.b" INT, "MixedCase" INT, "Order" INT, '
              f'"1st" INT, "with space" INT, "%pct" INT, "x""q" INT, "_u" INT, "ç" INT)', None),
    ("names.catalog", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                      f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'W4' ORDER BY ORDINAL_POSITION", None),
    ("names.insert", f'INSERT INTO {S}.W4 ("(i + 1)", "count(1)", "a.b", "MixedCase", "Order", "1st", '
                     f'"with space", "%pct", "x""q", "_u", "ç") SELECT 1,2,3,4,5,6,7,8,9,10,11 FROM {SRC} WHERE id = 1',
     None),
    ("names.read", f'SELECT "(i + 1)", "x""q", "ç" FROM {S}.W4', None),
    ("names.long_128", f'CREATE TABLE {S}.W5 ("{"c" * 128}" INT)', None),
    ("names.long_129", f'CREATE TABLE {S}.W6 ("{"c" * 129}" INT)', None),
    ("names.duplicate_case", f'CREATE TABLE {S}.W7 ("a" INT, "A" INT)', None),
    ("names.id_column", f'CREATE TABLE {S}.W8 ("ID" INT, "id2" INT)', None),
    ("names.id_column_catalog", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                                f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'W8' ORDER BY ORDINAL_POSITION", None),
    ("names.empty", f'CREATE TABLE {S}.W9 ("" INT)', None),

    # ---- INSERT … SELECT forms
    ("forms.setup", f"CREATE TABLE {S}.Stage (id INT, s VARCHAR(100))", None),
    ("forms.order_by", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {SRC} WHERE id < 5 ORDER BY id DESC", None),
    ("forms.top", f"INSERT INTO {S}.Stage (id, s) SELECT TOP 2 id, s FROM {SRC} ORDER BY id", None),
    ("forms.subquery", f"INSERT INTO {S}.Stage (id, s) SELECT q.id, q.s FROM (SELECT TOP 1 id, s FROM {SRC} "
                       "ORDER BY id DESC) AS q", None),
    ("forms.params", f"INSERT INTO {S}.Stage (id, s) SELECT id + ?, ? FROM {SRC} WHERE id = ?", [100, "p", 1]),
    ("forms.with_cte", f"INSERT INTO {S}.Stage (id, s) WITH q AS (SELECT id, s FROM {SRC}) SELECT id, s FROM q "
                       "WHERE id = 2", None),
    ("forms.cte_first", f"WITH q AS (SELECT id, s FROM {SRC}) INSERT INTO {S}.Stage (id, s) SELECT id, s FROM q "
                        "WHERE id = 2", None),
    ("forms.union", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {SRC} WHERE id = 1 UNION ALL "
                    f"SELECT id, s FROM {SRC} WHERE id = 2", None),
    ("forms.union_wrapped", f"INSERT INTO {S}.Stage (id, s) SELECT * FROM (SELECT id, s FROM {SRC} WHERE id = 1 "
                            f"UNION ALL SELECT id, s FROM {SRC} WHERE id = 2) AS u", None),
    ("forms.window", f"INSERT INTO {S}.Stage (id, s) SELECT ROW_NUMBER() OVER (ORDER BY id), s FROM {SRC}", None),
    ("forms.limit_offset", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {SRC} ORDER BY id LIMIT 2", None),
    ("forms.rowcount_probe", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {SRC}", None),
    ("forms.self_append", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {S}.Stage", None),
    ("forms.count", f"SELECT COUNT(*) FROM {S}.Stage", None),
    ("forms.zero_rows", f"INSERT INTO {S}.Stage (id, s) SELECT id, s FROM {SRC} WHERE 1 = 0", None),

    # ---- failure in the middle of INSERT … SELECT
    ("fail.setup", f"CREATE TABLE {S}.Tx (id INT, q INT)", None),
    ("fail.divide", f"INSERT INTO {S}.Tx (id, q) SELECT id, 10 / (id - 3) FROM {SRC} ORDER BY id", None),
    ("fail.left_behind", f"SELECT COUNT(*) FROM {S}.Tx", None),
    ("fail.not_null_setup", f"CREATE TABLE {S}.Tx2 (id INT NOT NULL, s VARCHAR(10))", None),
    ("fail.not_null", f"INSERT INTO {S}.Tx2 (id, s) SELECT CASE WHEN id = 3 THEN NULL ELSE id END, 'a' FROM {SRC}",
     None),
    ("fail.not_null_left", f"SELECT COUNT(*) FROM {S}.Tx2", None),
    ("fail.too_long", f"INSERT INTO {S}.Tx2 (id, s) SELECT id, s FROM {SRC} WHERE id = 5", None),

    # ---- transactions
    ("tx.start", "START TRANSACTION", None),
    ("tx.insert", f"INSERT INTO {S}.Tx (id, q) SELECT id, id FROM {SRC}", None),
    ("tx.count_inside", f"SELECT COUNT(*) FROM {S}.Tx", None),
    ("tx.rollback", "ROLLBACK", None),
    ("tx.count_after", f"SELECT COUNT(*) FROM {S}.Tx", None),
    ("tx.ddl_start", "START TRANSACTION", None),
    ("tx.ddl_create", f"CREATE TABLE {S}.Ddl (id INT)", None),
    ("tx.ddl_rollback", "ROLLBACK", None),
    ("tx.ddl_exists", f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{S}' "
                      "AND TABLE_NAME = 'Ddl'", None),
    ("tx.fail_in_tx_start", "START TRANSACTION", None),
    ("tx.fail_in_tx", f"INSERT INTO {S}.Tx (id, q) SELECT id, 10 / (id - 3) FROM {SRC} ORDER BY id", None),
    ("tx.fail_in_tx_rollback", "ROLLBACK", None),
    ("tx.fail_in_tx_left", f"SELECT COUNT(*) FROM {S}.Tx", None),

    # ---- overwrite: DROP + CREATE of the same name, then a failing INSERT, loses the old rows on ROLLBACK
    ("lost.setup", f"CREATE TABLE {S}.Old (id INT, s VARCHAR(10))", None),
    ("lost.fill", f"INSERT INTO {S}.Old (id, s) SELECT id, 'old' FROM {SRC}", None),
    ("lost.start", "START TRANSACTION", None),
    ("lost.drop", f"DROP TABLE {S}.Old", None),
    ("lost.recreate", f"CREATE TABLE {S}.Old (id INT, q DOUBLE)", None),
    ("lost.fail", f"INSERT INTO {S}.Old (id, q) SELECT id, 1 / (id - 3) FROM {SRC}", None),
    ("lost.rollback", "ROLLBACK", None),
    ("lost.columns_back", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                          f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'Old' ORDER BY ORDINAL_POSITION", None),
    ("lost.rows_back", f"SELECT COUNT(*) FROM {S}.Old", None),
    ("lost.no_failure_start", "START TRANSACTION", None),
    ("lost.no_failure_refill", f"INSERT INTO {S}.Old (id, s) SELECT id, 'old' FROM {SRC}", None),
    ("lost.no_failure_commit", "COMMIT", None),
    ("lost.no_failure_tx", "START TRANSACTION", None),
    ("lost.no_failure_drop", f"DROP TABLE {S}.Old", None),
    ("lost.no_failure_recreate", f"CREATE TABLE {S}.Old (id INT, q DOUBLE)", None),
    ("lost.no_failure_rollback", "ROLLBACK", None),
    ("lost.no_failure_rows_back", f"SELECT COUNT(*) FROM {S}.Old", None),
    ("staged.start", "START TRANSACTION", None),
    ("staged.create", f"CREATE TABLE {S}.Old2 (id INT, q DOUBLE)", None),
    ("staged.fail", f"INSERT INTO {S}.Old2 (id, q) SELECT id, 1 / (id - 3) FROM {SRC}", None),
    ("staged.rollback", "ROLLBACK", None),
    ("staged.rows_kept", f"SELECT COUNT(*) FROM {S}.Old", None),
    ("staged.stage_gone", "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                          f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'Old2'", None),
    ("staged.swap_start", "START TRANSACTION", None),
    ("staged.swap_create", f"CREATE TABLE {S}.Old2 (id INT, q DOUBLE)", None),
    ("staged.swap_insert", f"INSERT INTO {S}.Old2 (id, q) SELECT id, 1 FROM {SRC}", None),
    ("staged.swap_drop", f"DROP TABLE {S}.Old", None),
    ("staged.swap_rename", f"ALTER TABLE {S}.Old2 RENAME Old", None),
    ("staged.swap_commit", "COMMIT", None),
    ("staged.swap_columns", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                            f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'Old' ORDER BY ORDINAL_POSITION", None),
    ("staged.swap_rows", f"SELECT COUNT(*) FROM {S}.Old", None),

    # ---- rename, drop, existence
    ("rename", f"ALTER TABLE {S}.Stage RENAME Renamed", None),
    ("rename.read", f"SELECT COUNT(*) FROM {S}.Renamed", None),
    ("rename.old_gone", f"SELECT COUNT(*) FROM {S}.Stage", None),
    ("rename.onto_existing", f"ALTER TABLE {S}.Renamed RENAME Tx", None),
    ("rename.catalog", "SELECT TABLE_NAME, CLASSNAME FROM INFORMATION_SCHEMA.TABLES "
                       f"WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME IN ('Renamed', 'Stage')", None),
    ("rename.insert_after", f"INSERT INTO {S}.Renamed (id, s) VALUES (1, 'a')", None),
    ("rename.new_schema", f"ALTER TABLE {S}.Renamed RENAME MborobakProbe2.Renamed", None),
    ("exists.lower", "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                     "WHERE UPPER(TABLE_SCHEMA) = UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?)", [S.lower(), "renamed"]),
    ("create.existing", f"CREATE TABLE {S}.Renamed (id INT)", None),
    ("create.case_variant", f"CREATE TABLE {S}.RENAMED (id INT)", None),
    ("create.new_schema", "CREATE TABLE MborobakProbe3.Fresh (id INT)", None),
    ("create.new_schema_drop", "DROP TABLE MborobakProbe3.Fresh", None),
    ("create.default_schema", "CREATE TABLE MborobakProbeDefault (id INT)", None),
    ("create.default_schema_where", "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES "
                                    "WHERE TABLE_NAME = 'MborobakProbeDefault'", None),
    ("create.default_schema_drop", "DROP TABLE MborobakProbeDefault", None),
    ("create.as_select", f"CREATE TABLE {S}.W6 AS SELECT id, s, d, n, dt, ts FROM {SRC}", None),
    ("create.as_select_catalog",
     "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM "
     f"INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{S}' AND TABLE_NAME = 'W6' ORDER BY ORDINAL_POSITION", None),
    ("create.as_select_count", f"SELECT COUNT(*) FROM {S}.W6", None),
    ("drop.missing", f"DROP TABLE {S}.NoSuchTable", None),
    ("drop.if_exists", f"DROP TABLE IF EXISTS {S}.NoSuchTable", None),
]


def main() -> None:
    conn = base.connect()
    cur = conn.cursor()
    cur.execute("SELECT $ZVERSION")
    base.results["meta.version"] = {"result": cur.fetchall()[0][0]}

    def cleanup():
        for t in TABLES:
            base.run(cur, f"DROP TABLE {S}.{t}", fetch=False)
        base.run(cur, "DROP TABLE MborobakProbe2.Renamed", fetch=False)
        base.run(cur, "DROP TABLE MborobakProbe3.Fresh", fetch=False)
        base.run(cur, "DROP TABLE MborobakProbeDefault", fetch=False)

    cleanup()
    try:
        cur.execute(f"CREATE TABLE {SRC} (id INT, s VARCHAR(10000), d DOUBLE, n NUMERIC(10,2), dt DATE, ts TIMESTAMP)")
        cur.executemany(f"INSERT INTO {SRC} VALUES (?,?,?,?,?,?)", SRC_ROWS)
        for key, sql, params in STEPS:
            base.probe(cur, key, sql, params)
            if key in ("forms.rowcount_probe", "forms.zero_rows", "types.insert"):
                base.results[key]["rowcount"] = cur.rowcount
    finally:
        cleanup()
        conn.close()
    print(json.dumps(base.results, indent=1, default=str))


if __name__ == "__main__":
    main()
