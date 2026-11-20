"""df.write (writer.py) against a fake connection that records statements.

The statements' behavior in IRIS (transactional DDL, INSERT … SELECT forms, collation) is pinned by
probes/write_probe.py; results through IRIS and PySpark are compared in tests/compat/test_writes.py.
"""

from __future__ import annotations

import re

import pytest

from mborobak import MborobakSession
from mborobak.errors import (
    AnalysisError,
    MborobakArithmeticError,
    TableNotFoundError,
    UnsupportedOperationError,
)
from mborobak.sql import functions as F
from mborobak.writer import can_store, ddl_type
from mborobak.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    TimestampNTZType,
)

# (schema, table) -> (table type, [(column, IRIS type, precision, scale)])
TABLES = {
    ("App", "Src"): ("BASE TABLE", [("id", "integer", 10, 0), ("s", "varchar", None, None),
                                    ("d", "double", None, None), ("n", "numeric", 10, 2)]),
    ("App", "Target"): ("BASE TABLE", [("ID", "bigint", 19, 0), ("label", "varchar", None, None)]),
    ("App", "Shown"): ("VIEW", [("id", "integer", 10, 0)]),
    ("App", "Odd"): ("BASE TABLE", [("id", "integer", 10, 0), ("t", "time", None, None)]),
}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rows: list = []
        self.description = None
        self.rowcount = -1

    def execute(self, sql, params=()):
        params = list(params or [])
        if "INFORMATION_SCHEMA" in sql:
            key = next((k for k in TABLES if k[0].upper() == params[0].upper() and k[1].upper() == params[1].upper()),
                       None)
            if key is None:
                self.rows = []
            elif "TABLE_TYPE" in sql:
                self.rows = [(key[0], key[1], TABLES[key][0])]
            else:
                self.rows = [(key[0], key[1], c, t, p, s, "YES", None) for c, t, p, s in TABLES[key][1]]
            return
        self.conn.log.append((sql, params))
        if self.conn.fail_on and self.conn.fail_on in sql:
            raise RuntimeError("[SQLCODE: <-400>:<Fatal error occurred>] <DIVIDE>xx^%sqlcq")
        self.rowcount = 3 if sql.startswith(("INSERT", "WITH")) else -1

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.log: list[tuple[str, list]] = []
        self.fail_on: str | None = None

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass


@pytest.fixture()
def conn():
    return FakeConnection()


@pytest.fixture()
def session(conn):
    return MborobakSession.builder.connection(conn).create()


@pytest.fixture()
def src(session):
    return session.table("App.Src")


def statements(conn) -> list[str]:
    return [re.sub(r"\s+", " ", s).strip() for s, _ in conn.log]


# ---------------------------------------------------------------- creating a table


def test_create_runs_in_one_transaction(conn, session, src):
    src.select("id", "s", (F.col("id") > 1).alias("big"), F.col("n") * 2).write.saveAsTable("App.New")
    assert statements(conn) == [
        "START TRANSACTION",
        'CREATE TABLE "App"."New" ("id" INTEGER, "s" VARCHAR(3641144) COLLATE EXACT, "big" BIT, '
        '"(n * 2)" NUMERIC(12,2))',
        'INSERT INTO "App"."New" ("id", "s", "big", "(n * 2)") SELECT "id", "s", CASE WHEN "id" > ? THEN 1 '
        'WHEN NOT ("id" > ?) THEN 0 END AS "big", ("n" * ?) AS "(n * 2)" FROM "App"."Src"',
        "COMMIT",
    ]
    m = session.lastExecution
    assert (m.action, m.rows, m.data_queries) == ("saveAsTable", 3, 2)
    assert m.params == [1, 1, 2]


def test_table_names_default_to_sqluser(conn, src):
    src.select("id").write.saveAsTable("Plain")
    assert 'CREATE TABLE "SQLUser"."Plain" ("id" INTEGER)' in statements(conn)


@pytest.mark.parametrize("dtype, ddl", [
    (IntegerType(), "INTEGER"), (LongType(), "BIGINT"), (DoubleType(), "DOUBLE"),
    (DecimalType(38, 10), "NUMERIC(38,10)"), (StringType(), "VARCHAR(3641144) COLLATE EXACT"),
    (BooleanType(), "BIT"), (DateType(), "DATE"), (TimestampNTZType(), "TIMESTAMP"),
])
def test_column_types(dtype, ddl):
    assert ddl_type(dtype) == ddl


def test_string_settings(conn, session, src):
    session.conf.set("mborobak.write.stringLength", 200)
    session.conf.set("mborobak.sql.stringSemantics", "iris")
    src.select("s").write.saveAsTable("App.New")
    assert 'CREATE TABLE "App"."New" ("s" VARCHAR(200))' in statements(conn)
    with pytest.raises(ValueError):
        session.conf.set("mborobak.write.stringLength", 0)


@pytest.mark.parametrize("build, message", [
    (lambda df: df.select(F.col("id").alias("a.b")), "'.'"),
    (lambda df: df.select(F.col("id").alias("x,y")), "','"),
    (lambda df: df.select(F.col("id").alias(" ")), "non-empty"),
    (lambda df: df.select("id", F.col("s").alias("ID")), "case-insensitive"),
])
def test_invalid_columns(conn, src, build, message):
    with pytest.raises(AnalysisError, match=message):
        build(src).write.saveAsTable("App.New")
    assert statements(conn) == []


def test_sql_source_with_cte_puts_with_first(conn, session):
    conn_cursor_description = [("id", 4, None, None, 10, 0, 1)]
    session._describer = lambda sql, params: conn_cursor_description
    df = session.sql("WITH q AS (SELECT id FROM App.Src) SELECT id FROM q WHERE id > ?", [1])
    df.write.saveAsTable("App.New")
    insert = statements(conn)[2]
    assert insert.startswith('WITH q AS (SELECT id FROM App.Src) INSERT INTO "App"."New" ("id") SELECT')
    assert conn.log[2][1] == [1]


# ---------------------------------------------------------------- modes


def test_errorifexists_is_the_default(conn, src):
    for writer in (src.write, src.write.mode("error"), src.write.mode("ErrorIfExists")):
        with pytest.raises(AnalysisError, match="already exists"):
            writer.saveAsTable("app.target")
    assert statements(conn) == []


def test_ignore_does_nothing_when_the_table_exists(conn, session, src):
    src.write.mode("ignore").saveAsTable("App.Target")
    assert statements(conn) == []
    assert session.lastExecution.data_queries == 0
    src.select("id").write.saveAsTable("App.Other", mode="ignore")  # a new table is created
    assert statements(conn)[1].startswith('CREATE TABLE "App"."Other"')


def test_unknown_mode():
    with pytest.raises(ValueError, match="unknown save mode"):
        from mborobak.writer import _parse_mode

        _parse_mode("upsert")


def test_overwrite_writes_a_staging_table_and_swaps_it_in(conn, src):
    # Not DROP, CREATE, INSERT: a failing INSERT would lose the old rows on ROLLBACK (compiler spec §9.7)
    src.select("id").write.mode("overwrite").saveAsTable("App.Target")
    s = statements(conn)
    stage = re.search(r'"App"\."(Target_mborobak_[0-9a-f]{8})"', s[1]).group(1)
    assert s == [
        "START TRANSACTION",
        f'CREATE TABLE "App"."{stage}" ("id" INTEGER)',
        f'INSERT INTO "App"."{stage}" ("id") SELECT "id" FROM "App"."Src"',
        'DROP TABLE "App"."Target"',
        f'ALTER TABLE "App"."{stage}" RENAME "Target"',
        "COMMIT",
    ]


def test_overwrite_of_a_table_the_dataframe_reads(conn, session):
    df = session.table("App.Target").filter(F.col("ID") > 1)
    df.write.mode("overwrite").saveAsTable("app.TARGET")
    s = statements(conn)
    stage = re.search(r'"App"\."(Target_mborobak_[0-9a-f]{8})"', s[1]).group(1)
    assert s == [
        "START TRANSACTION",
        f'CREATE TABLE "App"."{stage}" ("ID" BIGINT, "label" VARCHAR(3641144) COLLATE EXACT)',
        f'INSERT INTO "App"."{stage}" ("ID", "label") SELECT "ID", "label" FROM "App"."Target" WHERE "ID" > ?',
        'DROP TABLE "App"."Target"',
        f'ALTER TABLE "App"."{stage}" RENAME "Target"',
        "COMMIT",
    ]


def test_append_matches_columns_by_name_and_casts(conn, src):
    df = src.select(F.col("s").alias("LABEL"), "id")  # other order, other case, int into a bigint column
    df.write.mode("append").saveAsTable("App.Target")
    assert statements(conn) == [
        "START TRANSACTION",
        'INSERT INTO "App"."Target" ("ID", "label") SELECT CAST("id" AS BIGINT) AS "ID", "s" AS "label" '
        'FROM "App"."Src"',
        "COMMIT",
    ]


def test_append_creates_a_missing_table(conn, src):
    src.select("id").write.mode("append").saveAsTable("App.New")
    assert statements(conn)[1] == 'CREATE TABLE "App"."New" ("id" INTEGER)'


@pytest.mark.parametrize("build, message", [
    (lambda df: df.select("id"), "has 1 columns and table App.Target has 2"),
    (lambda df: df.select("id", F.col("s").alias("name")), r"lacks \(it has \['name'\] instead\)"),
    (lambda df: df.select(F.col("s").alias("id"), F.col("s").alias("label")), "string.*into column 'ID' \\(bigint\\)"),
    (lambda df: df.select("id", F.col("s").alias("ID")), "two columns named"),
])
def test_append_errors(conn, src, build, message):
    with pytest.raises(AnalysisError, match=message):
        build(src).write.mode("append").saveAsTable("App.Target")
    assert statements(conn) == []


def test_views_are_not_written(src):
    with pytest.raises(AnalysisError, match="is a view"):
        src.select("id").write.mode("append").saveAsTable("App.Shown")
    with pytest.raises(AnalysisError, match="is a view"):
        src.select("id").write.insertInto("App.Shown")


def test_tables_with_unsupported_columns_are_not_appended_to(src):
    from mborobak.errors import UnsupportedTypeError

    with pytest.raises(UnsupportedTypeError, match=r"t \(time\)"):
        src.select("id", "s").write.mode("append").saveAsTable("App.Odd")


# ---------------------------------------------------------------- insertInto


def test_insert_into_matches_by_position(conn, src):
    src.select("id", "s").write.insertInto("App.Target")
    assert statements(conn)[1] == ('INSERT INTO "App"."Target" ("ID", "label") SELECT CAST("id" AS BIGINT) AS "ID", '
                                   '"s" AS "label" FROM "App"."Src"')


def test_insert_into_overwrite_deletes_first(conn, src):
    src.select("id", "s").write.insertInto("App.Target", overwrite=True)
    assert statements(conn)[:2] == ["START TRANSACTION", 'DELETE FROM "App"."Target"']
    conn.log.clear()
    src.select("id", "s").write.mode("overwrite").insertInto("App.Target")
    assert statements(conn)[1] == 'DELETE FROM "App"."Target"'


def test_insert_into_errors(session, src):
    with pytest.raises(TableNotFoundError, match="saveAsTable creates one"):
        src.select("id").write.insertInto("App.Missing")
    df = session.table("App.Target")
    with pytest.raises(AnalysisError, match="cannot read the table it overwrites"):
        df.write.insertInto("App.Target", overwrite=True)
    with pytest.raises(AnalysisError, match="cannot write column 's' \\(string\\) into column 'ID'"):
        src.select("s", "s").write.insertInto("App.Target")


# ---------------------------------------------------------------- failures and options


def test_a_failed_write_rolls_back(conn, session, src):
    conn.fail_on = "INSERT"
    with pytest.raises(MborobakArithmeticError, match="division by zero"):
        src.select("id").write.mode("overwrite").saveAsTable("App.Target")
    s = statements(conn)
    assert s[-1] == "ROLLBACK" and not any(x.startswith("DROP") for x in s)


def test_options(conn, src):
    w = src.select("id").write.format("delta").partitionBy("id").option("overwriteSchema", "true")
    w.mode("overwrite").saveAsTable("App.Target")
    assert statements(conn)[-2] == 'ALTER TABLE ' + statements(conn)[1].split(" (")[0][13:] + ' RENAME "Target"'
    with pytest.raises(UnsupportedOperationError, match="mergeSchema"):
        src.write.option("mergeSchema", "true").mode("append").saveAsTable("App.Target")
    with pytest.raises(UnsupportedOperationError, match="path option"):
        src.write.saveAsTable("App.X", path="/tmp/x")
    for name in ("save", "parquet", "csv", "json"):
        with pytest.raises(UnsupportedOperationError, match="writes files"):
            getattr(src.write, name)("/tmp/out")


def test_store_assignment_rules():
    num = [IntegerType(), LongType(), DoubleType(), DecimalType(10, 2)]
    assert all(can_store(a, b) for a in num for b in num)
    assert all(can_store(t, StringType()) for t in num + [BooleanType(), DateType(), TimestampNTZType()])
    assert can_store(DateType(), TimestampNTZType()) and can_store(TimestampNTZType(), DateType())
    assert can_store(NullType(), DateType())
    assert not can_store(StringType(), IntegerType())
    assert not can_store(BooleanType(), IntegerType())
    assert not can_store(DateType(), LongType())
