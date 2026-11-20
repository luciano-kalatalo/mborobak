"""Regular expressions without IRIS: SQL text, names, types and installing the IRIS functions (§7.16)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, MborobakExecutionError, UnsupportedOperationError
from mborobak.irisfuncs import FUNCTIONS, create_sql, ensure
from mborobak.sql import functions as F
from mborobak.sql.types import IntegerType, StringType


@pytest.fixture()
def df():
    s = MborobakSession._for_tests(StaticCatalog({"T": [("i", IntegerType()), ("s", StringType()),
                                                         ("p", StringType())]}))
    return s.table("T")


def test_names_and_types(df):
    out = df.select(F.col("s").rlike("b"), F.rlike("s", "p"), F.regexp("s", F.lit("x")), F.regexp_like("s", "p"),
                    F.regexp_replace("s", "[0-9]+", "#"), F.regexp_extract("s", "(a)", 1),
                    F.regexp_count("s", F.lit("a")), F.regexp_substr("s", F.lit("a")), F.regexp_instr("s", F.lit("a")))
    assert out.dtypes == [("RLIKE(s, b)", "boolean"), ("RLIKE(s, p)", "boolean"), ("REGEXP(s, x)", "boolean"),
                          ("REGEXP_LIKE(s, p)", "boolean"), ("regexp_replace(s, [0-9]+, #, 1)", "string"),
                          ("regexp_extract(s, (a), 1)", "string"), ("regexp_count(s, a)", "int"),
                          ("regexp_substr(s, a)", "string"), ("regexp_instr(s, a, 0)", "int")]
    assert df.selectExpr("s RLIKE 'b'", "s REGEXP 'b'", "regexp_extract(s, '(b)')").columns == \
        ["RLIKE(s, b)", "RLIKE(s, b)", "regexp_extract(s, (b), 1)"]


def test_sql_text(df):
    text, params = df.filter(~F.col("s").rlike("^a")).select(F.regexp_replace("s", "a", "b")).toSQL()
    call = 'CASE WHEN 1 = 0 THEN NULL ELSE "Mborobak"."RegexpLike1"("s", ?) END'
    assert f"WHERE NOT ({call} = 1)" in text
    assert 'CASE WHEN 1 = 0 THEN NULL ELSE "Mborobak"."RegexpReplace1"("s", ?, ?) END' in text
    assert params == ["a", "b", "^a"] or params == ["^a", "a", "b"]
    stmt = df._session._compile(df.select(F.regexp_instr("s", F.lit("x")))._plan)
    assert stmt.functions == frozenset({"RegexpInstr1"}) and stmt.params == ["x"]  # idx 0 is not sent


def test_type_errors(df):
    with pytest.raises(AnalysisError, match="string"):
        df.select(F.col("i").rlike("1"))
    with pytest.raises(TypeError):
        F.regexp_extract("s", "(a)", "1")
    with pytest.raises(UnsupportedOperationError, match="idx=0"):
        F.regexp_instr("s", F.lit("a"), 1)
    for text in ("split(s, 'a')", "regexp_extract_all(s, 'a')"):
        with pytest.raises(UnsupportedOperationError, match="array"):
            df.selectExpr(text)


class FakeCursor:
    rowcount = -1

    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=()):
        self.conn.log.append(sql)
        if sql.startswith("SELECT ROUTINE_NAME"):
            self.rows = [(n,) for n in sorted(self.conn.existing)]
        elif sql.startswith("CREATE FUNCTION"):
            if self.conn.deny:
                raise RuntimeError("[SQLCODE: <-99>:<Privilege violation>]")
            self.conn.existing.add(sql.split('"')[3])

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, existing=(), deny=False):
        self.existing, self.deny, self.log = set(existing), deny, []

    def cursor(self):
        return FakeCursor(self)


def test_functions_are_created_once_per_session():
    conn = FakeConnection(existing={"RegexpLike1"})
    s = MborobakSession.builder.connection(conn).create()
    ensure(s, {"RegexpLike1", "RegexpReplace1"}, "test")
    assert [x.split("(")[0] for x in conn.log if x.startswith("CREATE")] == ['CREATE FUNCTION "Mborobak"."RegexpReplace1"']
    conn.log.clear()
    ensure(s, {"RegexpLike1", "RegexpReplace1"}, "test")
    assert conn.log == []  # known for this session: no lookup
    assert s.installFunctions() == [f"Mborobak.{n}" for n in sorted(FUNCTIONS)]
    assert conn.existing == set(FUNCTIONS)


def test_missing_privilege_explains_what_to_do():
    s = MborobakSession.builder.connection(FakeConnection(deny=True)).create()
    with pytest.raises(MborobakExecutionError, match="installFunctions"):
        ensure(s, {"RegexpLike1"}, "test")


def test_function_definitions():
    sql = create_sql("RegexpExtract1")
    assert sql.startswith('CREATE FUNCTION "Mborobak"."RegexpExtract1"(s VARCHAR(3641144), p VARCHAR(32000), i INT) '
                          "RETURNS VARCHAR(3641144) LANGUAGE OBJECTSCRIPT {")
    assert 'ztrap "MBGROUP"' in sql and '%mborobakRx' in sql
