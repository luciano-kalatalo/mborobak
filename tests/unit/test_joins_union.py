"""Join and union compilation without IRIS."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AmbiguousColumnError, AnalysisError, UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, IntegerType, StringType


@pytest.fixture()
def session():
    return MborobakSession._for_tests(StaticCatalog({
        "Sales": [("id", IntegerType(), False), ("customer_id", IntegerType()), ("amount", DecimalType(10, 2))],
        "Customers": [("id", IntegerType(), False), ("name", StringType()), ("country", StringType())],
    }))


def test_join_of_two_tables_has_no_subqueries(session):
    s, c = session.table("Sales"), session.table("Customers")
    text, _ = s.join(c, s.customer_id == c.id, "left").toSQL()
    assert "(\n" not in text
    assert 'LEFT OUTER JOIN "SQLUser"."Customers" AS t2 ON t1."customer_id" = t2."id"' in text


def test_filtered_side_becomes_subquery(session):
    s, c = session.table("Sales"), session.table("Customers")
    text, params = s.join(c.filter(c.country == "BR"), s.customer_id == c.id, "left").toSQL()
    assert 'WHERE %EXACT("country") = ?' in text  # inside the right side, before the outer join
    assert params == ["BR"]


def test_string_join_condition_is_exact(session):
    c = session.table("Customers")
    a, b = c.alias("a"), c.alias("b")
    text, _ = a.join(b, F.col("a.name") == F.col("b.name")).toSQL()
    assert 'ON %EXACT(t1."name") = %EXACT(t2."name")' in text


def test_full_join_on_name_uses_coalesce(session):
    s = session.table("Sales").withColumnRenamed("customer_id", "cid")
    c = session.table("Customers").withColumnRenamed("id", "cid")
    df = s.join(c, "cid", "full")
    assert df.columns == ["cid", "id", "amount", "name", "country"]
    assert 'COALESCE(t1."customer_id", t2."id") AS "cid"' in df.toSQL()[0]


def test_join_output_nullability(session):
    s, c = session.table("Sales"), session.table("Customers")
    df = s.join(c, s.customer_id == c.id, "left")
    nullable = {f.name: f.nullable for f in df.schema if f.name in ("name",)}
    assert nullable["name"] is True
    assert [f.nullable for f in df.schema][0] is False  # Sales.id stays NOT NULL on the left side


def test_ambiguous_and_qualified_names(session):
    s, c = session.table("Sales"), session.table("Customers")
    joined = s.join(c, s.customer_id == c.id)
    with pytest.raises(AmbiguousColumnError):
        joined.select("id")
    assert joined.select(F.col("Sales.id"), F.col("Customers.id").alias("cid")).columns == ["id", "cid"]


def test_semi_and_anti_joins_use_exists(session):
    s, c = session.table("Sales"), session.table("Customers")
    for how in ("semi", "leftsemi", "left_semi", "SEMI"):
        df = s.join(c, s.customer_id == c.id, how)
        assert df.columns == ["id", "customer_id", "amount"]  # the left side only
        assert df.toSQL()[0] == ('SELECT t1."id", t1."customer_id", t1."amount"\nFROM "SQLUser"."Sales" AS t1\n'
                                 'WHERE EXISTS (SELECT 1 FROM "SQLUser"."Customers" AS t2 '
                                 'WHERE t1."customer_id" = t2."id")')
    for how in ("anti", "leftanti", "left_anti"):
        assert 'WHERE NOT EXISTS (SELECT 1 FROM "SQLUser"."Customers" AS t2 WHERE' in \
            s.join(c, s.customer_id == c.id, how).toSQL()[0]


def test_semi_join_on_names_keeps_the_left_columns(session):
    s = session.table("Sales")
    c = session.table("Customers").withColumnRenamed("id", "customer_id")
    df = s.join(c, "customer_id", "left_semi")
    assert df.columns == ["customer_id", "id", "amount"]  # keys first, as in Spark
    assert df.schema[1].nullable is False


def test_semi_join_with_filters_on_both_sides(session):
    s, c = session.table("Sales"), session.table("Customers")
    df = s.join(c.filter(c.country == "BR"), (s.customer_id == c.id) & (c.name != "x"), "semi").filter(s.id > 5)
    text, params = df.toSQL()
    assert "EXISTS (SELECT 1 FROM (" in text and 'WHERE %EXACT("country") = ?' in text
    assert text.endswith('AND t1."id" > ?') and params == ["BR", "x", 5]
    joined = df.join(c, s.customer_id == c.id)  # a later join wraps the semi join as a subquery
    assert joined.columns == ["id", "customer_id", "amount", "id", "name", "country"]
    assert "WHERE EXISTS" in joined.toSQL()[0]


def test_semi_join_condition_must_use_both_sides_resolvably(session):
    s, c = session.table("Sales"), session.table("Customers")
    with pytest.raises(AnalysisError):
        s.join(c, s.customer_id == c.id, "anti").select(c.name)  # the right side is not in the output
    assert s.join(c, how="semi").toSQL()[0].endswith('WHERE EXISTS (SELECT 1 FROM "SQLUser"."Customers" AS t2)')


def test_union_by_name(session):
    s, c = session.table("Sales"), session.table("Customers")
    left = s.select("id", F.col("amount").alias("value"))
    right = c.select(F.col("id").cast("decimal(10,2)").alias("VALUE"), "ID")  # other order, other case
    df = left.unionByName(right)
    assert df.columns == ["id", "value"]
    text, _ = df.toSQL()
    assert 'SELECT "id", CAST("id" AS NUMERIC(10,2)) AS "value"' in text  # reordered by name
    with pytest.raises(AnalysisError, match=r"\['country'\] are only in the right"):
        left.unionByName(c.select(F.col("name").alias("Value"), "id", "country"))


def test_union_by_name_with_missing_columns(session):
    s, c = session.table("Sales"), session.table("Customers")
    df = s.select("id", "amount").unionByName(c.select("name", "id"), allowMissingColumns=True)
    assert df.dtypes == [("id", "int"), ("amount", "decimal(10,2)"), ("name", "string")]
    text, _ = df.toSQL()
    assert 'CAST(NULL AS NUMERIC(10,2)) AS "amount"' in text and 'CAST(NULL AS VARCHAR(32767)) AS "name"' in text
    with pytest.raises(AnalysisError, match=r"\['amount'\] are only in the left"):
        s.select("id", "amount").unionByName(c.select("id"))
    with pytest.raises(AnalysisError, match="two columns named"):
        s.select("id", F.col("amount").alias("ID")).unionByName(c.select("id"))


def test_union_sql_and_widening(session):
    s, c = session.table("Sales"), session.table("Customers")
    df = s.select("id").union(c.select("id")).union(s.select("amount"))
    text, _ = df.toSQL()
    assert text.count("UNION ALL") == 2
    assert df.dtypes == [("id", "decimal(12,2)")]
    assert 'CAST("id" AS NUMERIC(12,2)) AS "id"' in text


def test_union_column_count_mismatch(session):
    s = session.table("Sales")
    with pytest.raises(AnalysisError, match="same number of columns"):
        s.select("id").union(s.select("id", "amount"))


def test_duplicate_names_are_renamed_inside_subqueries(session):
    s, c = session.table("Sales"), session.table("Customers")
    text, _ = s.join(c, s.customer_id == c.id).limit(5).filter(c.id > 1).toSQL()
    assert 'AS "id_2"' in text
    assert text.startswith('SELECT t3."id", t3."customer_id", t3."amount", t3."id_2" AS "id"')
    assert text.endswith('WHERE t3."id_2" > ?')
