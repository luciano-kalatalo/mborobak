"""Compiler and analyzer tests without IRIS (StaticCatalog)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession, Row
from mborobak.catalog import StaticCatalog, parse_collation, parse_table_name
from mborobak.errors import (
    AnalysisError,
    ColumnNotFoundError,
    PlanTooLargeError,
    TableNotFoundError,
    UnsupportedOperationError,
)
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, DoubleType, IntegerType, StringType


@pytest.fixture()
def session():
    catalog = StaticCatalog({
        "Sales": [("id", IntegerType(), False), ("customer_id", IntegerType()), ("status", StringType()),
                  ("amount", DecimalType(10, 2)), ("score", DoubleType())],
        "Finance.Codes": [("code", StringType(), False, "EXACT"), ("label", StringType())],
    })
    return MborobakSession._for_tests(catalog)


def sql(df):
    return df.toSQL()


def test_scope_example(session):
    df = (session.table("Sales").filter(F.col("status") == "PAID").groupBy("customer_id")
          .agg(F.sum("amount").alias("revenue")).orderBy(F.col("revenue").desc()))
    text, params = sql(df)
    assert text == (
        'SELECT "customer_id", SUM("amount") AS "revenue"\n'
        'FROM "SQLUser"."Sales"\n'
        'WHERE %EXACT("status") = ?\n'
        'GROUP BY "customer_id"\n'
        'ORDER BY SUM("amount") DESC'
    )
    assert params == ["PAID"]


def test_exact_column_is_not_wrapped(session):
    text, _ = sql(session.table("Finance.Codes").filter(F.col("code") == "A").filter(F.col("label") == "b"))
    assert '"code" = ?' in text and '%EXACT("label") = ?' in text


def test_iris_semantics_has_no_exact(session):
    session.conf.set("mborobak.sql.stringSemantics", "iris")
    text, _ = sql(session.table("Sales").groupBy("status").count())
    assert "%EXACT" not in text


def test_group_key_is_exact_in_select_and_group_by(session):
    text, _ = sql(session.table("Sales").groupBy("status").agg(F.max("status").alias("m")))
    assert text.startswith('SELECT %EXACT("status") AS "status", MAX(%EXACT("status")) AS "m"')
    assert text.endswith('GROUP BY %EXACT("status")')


def test_never_group_by_position(session):
    text, _ = sql(session.table("Sales").groupBy((F.col("id") + 1).alias("k")).count())
    assert 'GROUP BY ("id" + ?)' in text


def test_order_by_inside_subquery_only_with_top(session):
    df = session.table("Sales").orderBy("id").filter(F.col("id") > 1).distinct()
    text, _ = sql(df)
    assert "ORDER BY" not in text  # distinct destroys order; no ORDER BY without TOP inside


def test_limit_then_filter_keeps_top_and_reapplies_order(session):
    text, params = sql(session.table("Sales").orderBy("id").limit(3).filter(F.col("id") > 1))
    inner, outer = text.split(") AS t1")
    assert "SELECT TOP ?" in inner and 'ORDER BY "id"' in inner
    assert 'ORDER BY t1."id"' in outer
    assert params == [3, 1]  # in text order: TOP, then WHERE


def test_double_sort_uses_bucket_key(session):
    text, _ = sql(session.table("Sales").orderBy(F.col("score").desc()))
    assert 'CASE WHEN "score" IS NULL THEN 0' in text
    assert 'END DESC, "score" + 0 DESC' in text


def test_explicit_nulls_last(session):
    text, _ = sql(session.table("Sales").orderBy(F.col("id").asc_nulls_last()))
    assert text.endswith('ORDER BY CASE WHEN "id" IS NULL THEN 1 ELSE 0 END, "id"')


def test_integer_division_casts_to_double(session):
    text, _ = sql(session.table("Sales").select((F.col("id") / F.col("customer_id")).alias("r")))
    assert '(CAST("id" AS DOUBLE) / "customer_id") AS "r"' in text


def test_modulo_uses_mod(session):
    text, _ = sql(session.table("Sales").select((F.col("id") % 3).alias("m")))
    assert 'MOD("id", ?) AS "m"' in text


def test_string_to_int_cast_has_guard(session):
    text, _ = sql(session.table("Sales").select(F.col("status").cast("int")))
    assert "%PATTERN" in text and 'LOG(LENGTH("status") - LENGTH("status"))' in text


def test_boolean_in_select_is_case(session):
    text, _ = sql(session.table("Sales").select(F.col("id").isNull().alias("n")))
    assert 'CASE WHEN "id" IS NULL THEN 1 WHEN NOT ("id" IS NULL) THEN 0 END AS "n"' in text


def test_literals_are_parameters_and_counted(session):
    df = session.table("Sales").filter(F.col("id").isin(list(range(901))))
    with pytest.raises(PlanTooLargeError):
        df.toSQL()


def test_count_wraps_plan(session):
    from mborobak import expressions as E
    from mborobak import plan as P
    from mborobak.sql.types import LongType

    df = session.table("Sales").orderBy("id")
    stmt = session._compile(P.CountRows(df._plan, E.Attribute(E.new_id(), "count", LongType(), False)))
    assert stmt.text.startswith('SELECT COUNT(*) AS "count"\nFROM (')
    assert "ORDER BY" not in stmt.text


def test_default_output_names(session):
    df = session.table("Sales").groupBy("customer_id").agg(
        F.sum("amount"), F.count("*"), F.countDistinct("status"), F.avg("score"))
    assert df.columns == ["customer_id", "sum(amount)", "count(1)", "count(DISTINCT status)", "avg(score)"]
    assert session.table("Sales").select(F.col("id") + F.col("customer_id")).columns == ["(id + customer_id)"]


def test_result_types_follow_spark(session):
    df = session.table("Sales").groupBy("status").agg(
        F.sum("id").alias("s_int"), F.sum("amount").alias("s_dec"), F.avg("id").alias("a_int"),
        F.avg("amount").alias("a_dec"), F.count("id").alias("c"))
    assert df.dtypes == [("status", "string"), ("s_int", "bigint"), ("s_dec", "decimal(20,2)"),
                         ("a_int", "double"), ("a_dec", "decimal(14,6)"), ("c", "bigint")]


def test_errors_raised_at_the_transformation(session):
    sales = session.table("Sales")
    with pytest.raises(ColumnNotFoundError, match="Did you mean: status"):
        sales.filter(F.col("statsu") == "x")
    with pytest.raises(AnalysisError, match="neither grouped nor aggregated"):
        sales.groupBy("status").agg(F.col("id"))
    with pytest.raises(AnalysisError, match="must be boolean"):
        sales.filter(F.col("id"))
    with pytest.raises(AnalysisError, match="cannot compare"):
        sales.filter(F.col("id") == "x")
    with pytest.raises(TableNotFoundError):
        session.table("Nope")


def test_unsupported_members_raise(session):
    sales = session.table("Sales")
    with pytest.raises(UnsupportedOperationError):
        sales.cache()
    with pytest.raises(UnsupportedOperationError):
        session.conf.set("spark.sql.ansi.enabled", True)
    with pytest.raises(ValueError, match="Cannot convert column into bool"):
        bool(F.col("id") > 1)


def test_column_access_and_case_insensitive_names(session):
    sales = session.table("Sales")
    assert sales.select(sales["STATUS"]).columns == ["status"]
    assert sales.select(F.col("Status")).columns == ["Status"]
    assert sales.withColumnRenamed("nope", "x") is sales


def test_explain_is_deterministic(session, capsys):
    df = session.table("Sales").filter(F.col("status") == "PAID")
    df.explain()
    first = capsys.readouterr().out
    df.explain()
    assert capsys.readouterr().out == first
    assert "== SQL ==" in first and "1: 'PAID'" in first


def test_parse_helpers():
    assert parse_table_name("Sales").schema == "SQLUser"
    assert str(parse_table_name("`My.Schema`.`Odd Table`")) == "My.Schema.Odd Table"
    assert parse_collation("CALCSELECTIVITY,1,COLLATION,,CONTENT,STRING") == "SQLUPPER"
    assert parse_collation("COLLATION,EXACT,MAXLEN,20") == "EXACT"
    assert parse_collation(None) is None


def test_row_behaves_like_pyspark():
    r = Row(a=1, b="x")
    assert r[0] == 1 and r["b"] == "x" and r.a == 1 and r.asDict() == {"a": 1, "b": "x"}
    assert Row("a", "b")(1, "x") == r
    assert repr(r) == "Row(a=1, b='x')"
