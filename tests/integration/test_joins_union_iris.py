"""Joins and union against a live IRIS (compiler spec §6.3, compatibility matrix JOIN-*, UNI-*)."""

from __future__ import annotations

import decimal

import pytest

from mborobak.errors import AmbiguousColumnError, AnalysisError
from mborobak.sql import functions as F

pytestmark = pytest.mark.iris

D = decimal.Decimal


@pytest.fixture()
def sales(session):
    return session.table("MborobakTest.Sales")


@pytest.fixture()
def customers(session):
    return session.table("MborobakTest.Customers")


def test_inner_join_then_aggregate(session, sales, customers):
    df = (sales.join(customers, sales["customer_id"] == customers["id"])
          .groupBy("country").agg(F.sum("amount").alias("revenue")).orderBy("country"))
    # 'us' and 'US' are different countries under PySpark semantics; 'US' has no sales.
    assert [tuple(r) for r in df.collect()] == [("BR", D("190.50")), ("us", D("305.25"))]
    assert session.lastExecution.data_queries == 1


def test_left_join_keeps_unmatched_rows(sales, customers):
    rows = (sales.join(customers, sales.customer_id == customers.id, "left")
            .select(sales.id, customers.name).orderBy(sales.id).collect())
    assert [(r.id, r.name) for r in rows] == [
        (1, "Ana"), (2, "Ana"), (3, "Bo"), (4, "Bo"), (5, "Cy"), (6, "Cy"), (7, None), (8, None), (9, None)]


def test_right_join(sales, customers):
    df = sales.join(customers, sales.customer_id == customers.id, "right")
    assert df.count() == 7
    unmatched = df.filter(sales.id.isNull()).select(customers.name).collect()
    assert [r.name for r in unmatched] == ["Di"]


def test_full_join_on_name_coalesces_key(sales, customers):
    left = sales.withColumnRenamed("customer_id", "cid")
    right = customers.withColumnRenamed("id", "cid")
    df = left.join(right, "cid", "full")
    assert df.columns[0] == "cid" and df.columns.count("cid") == 1
    keys = sorted((r.cid for r in df.collect()), key=lambda v: (v is not None, v))
    assert keys == [None, 10, 10, 20, 20, 30, 30, 40, 40, 50]


def test_join_on_name_keeps_key_once(sales, customers):
    df = sales.withColumnRenamed("customer_id", "cid").join(customers.withColumnRenamed("id", "cid"), ["cid"])
    assert df.columns == ["cid", "id", "status", "amount", "score", "day", "name", "country"]
    assert df.count() == 6


def test_self_join_with_aliases(customers):
    a, b = customers.alias("a"), customers.alias("b")
    df = a.join(b, F.col("a.country") == F.col("b.country")).select(
        F.col("a.name").alias("x"), F.col("b.name").alias("y"))
    pairs = sorted((r.x, r.y) for r in df.collect())
    assert pairs == [("Ana", "Ana"), ("Ana", "Cy"), ("Bo", "Bo"), ("Cy", "Ana"), ("Cy", "Cy"), ("Di", "Di")]


def test_string_join_is_exact(session):
    words = session.table("MborobakTest.Words").filter(F.col("id") <= 7)
    a, b = words.alias("a"), words.alias("b")
    assert a.join(b, F.col("a.w") == F.col("b.w")).count() == 7  # IRIS default collation gives 21


def test_cross_join(sales, customers):
    assert sales.crossJoin(customers).count() == 36
    assert sales.join(customers).count() == 36


def test_ambiguous_name_after_join(sales, customers):
    with pytest.raises(AmbiguousColumnError):
        sales.join(customers, sales.customer_id == customers.id).select("id")


def test_join_with_duplicate_names_through_a_subquery(sales, customers):
    df = sales.join(customers, sales.customer_id == customers.id).orderBy(sales.id).limit(100) \
        .filter(customers.id > 10)
    rows = df.collect()
    assert [r[0] for r in rows] == [3, 4, 5, 6]
    assert df.columns.count("id") == 2


def test_three_way_join(session, sales, customers):
    s2 = sales.alias("s2")
    df = (sales.join(customers, sales.customer_id == customers.id)
          .join(s2, F.col("s2.id") == customers.id)
          .select(sales.id, F.col("s2.id").alias("id2")))
    assert df.collect() == []  # customer ids (10, 20, 30, 50) never equal a sales id (1-9)
    assert "JOIN" in df.toSQL()[0]


def test_union_keeps_duplicates_by_position(sales, customers):
    df = sales.select("id").union(customers.select("id"))
    assert df.count() == 13
    top = df.filter(F.col("id") > 8).orderBy(F.col("id").desc()).collect()
    assert [r.id for r in top] == [50, 30, 20, 10, 9]


def test_union_widens_numeric_types(sales):
    df = sales.filter(F.col("id") == 1).select("id").union(sales.filter(F.col("id") == 1).select("amount"))
    assert df.dtypes == [("id", "decimal(12,2)")]
    assert sorted(r.id for r in df.collect()) == [D("1.00"), D("100.50")]


def test_union_then_distinct_and_names_from_first(sales, customers):
    df = sales.select(F.col("customer_id").alias("k")).unionAll(customers.select("id")).distinct()
    assert df.columns == ["k"]
    assert sorted((r.k for r in df.collect()), key=lambda v: (v is not None, v)) == [None, 10, 20, 30, 40, 50]


def test_union_of_strings_keeps_exact_values(session):
    words = session.table("MborobakTest.Words").filter(F.col("id") <= 3).select("w")
    assert sorted(r.w for r in words.union(words).distinct().collect()) == ["APPLE", "Apple", "apple"]


def test_union_rejects_mixed_types(sales):
    with pytest.raises(AnalysisError, match="UNI-03"):
        sales.select("id").union(sales.select("status"))


def test_union_arm_with_order_and_limit(sales, customers):
    top2 = sales.orderBy(F.col("amount").desc()).limit(2).select("id")
    df = top2.union(customers.select("id"))
    assert sorted(r.id for r in df.collect()) == [1, 3, 10, 20, 30, 50]


def test_semi_and_anti_joins(session):
    s, c = session.table("MborobakTest.Sales"), session.table("MborobakTest.Customers")
    semi = s.join(c.filter(c.country == "BR"), s.customer_id == c.id, "left_semi")
    assert sorted(r.id for r in semi.collect()) == [1, 2, 5, 6]  # customers 10 and 30
    anti = s.join(c, s.customer_id == c.id, "left_anti")
    assert sorted(r.id for r in anti.collect()) == [7, 8, 9]  # customer NULL and 40
    assert anti.count() == 3


def test_union_by_name(session):
    s, c = session.table("MborobakTest.Sales"), session.table("MborobakTest.Customers")
    df = s.select("id", "status").unionByName(c.select(F.col("name").alias("STATUS"), "id"))
    assert df.count() == 13 and df.columns == ["id", "status"]
    missing = s.select("id").unionByName(c.select("id", "name"), allowMissingColumns=True)
    assert sum(1 for r in missing.collect() if r.name is None) == 9
