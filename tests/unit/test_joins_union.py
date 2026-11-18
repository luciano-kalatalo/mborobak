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


def test_semi_join_is_later(session):
    s, c = session.table("Sales"), session.table("Customers")
    with pytest.raises(UnsupportedOperationError, match="P1"):
        s.join(c, s.customer_id == c.id, "left_semi")


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
