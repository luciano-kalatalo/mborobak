"""session.sql() against a live IRIS."""

from __future__ import annotations

import datetime
import decimal

import pytest

from mborobak.errors import MborobakExecutionError, UnsupportedTypeError
from mborobak.sql import functions as F

pytestmark = pytest.mark.iris

D = decimal.Decimal


def test_sql_alone_keeps_its_order(session):
    df = session.sql("SELECT id, status FROM MborobakTest.Sales WHERE id > ? ORDER BY id DESC", [6])
    assert session.lastExecution is None or session.lastExecution.action != "collect"
    assert [r.id for r in df.collect()] == [9, 8, 7]
    m = session.lastExecution
    assert m.data_queries == 1 and m.schema_lookups == 0


def test_sql_schema_comes_from_iris(session):
    df = session.sql("SELECT id, amount, day, status, score FROM MborobakTest.Sales")
    assert df.dtypes == [("id", "int"), ("amount", "decimal(10,2)"), ("day", "date"), ("status", "string"),
                         ("score", "double")]
    row = df.filter(F.col("id") == 1).first()
    assert row.day == datetime.date(2026, 1, 1) and row.amount == D("100.50")


def test_transformations_over_sql(session):
    base = session.sql("SELECT id, customer_id, status, amount FROM MborobakTest.Sales WHERE id > ?", [0])
    df = base.filter(F.col("status") == "PAID").groupBy("customer_id") \
        .agg(F.sum("amount").alias("revenue")).orderBy(F.col("revenue").desc())
    assert [tuple(r) for r in df.collect()] == [(20, D("300.00")), (10, D("100.50")), (None, D("1.00")),
                                                (30, None)]


def test_sql_with_cte(session):
    df = session.sql("WITH big AS (SELECT id, amount FROM MborobakTest.Sales WHERE amount > ?) "
                     "SELECT id, amount FROM big", [50])
    assert [r.id for r in df.orderBy("id").collect()] == [1, 3, 5]
    assert df.filter(F.col("id") > 1).count() == 2


def test_sql_union_with_order_by(session):
    df = session.sql("SELECT id FROM MborobakTest.Sales UNION ALL SELECT id FROM MborobakTest.Customers "
                     "ORDER BY 1 DESC")
    assert [r.id for r in df.collect()][:3] == [50, 30, 20]
    assert df.count() == 13
    assert sorted(r.id for r in df.filter(F.col("id") > 8).collect()) == [9, 10, 20, 30, 50]


def test_sql_joined_with_a_table(session):
    orders = session.sql("SELECT id AS order_id, customer_id FROM MborobakTest.Sales")
    customers = session.table("MborobakTest.Customers")
    df = orders.join(customers, orders.customer_id == customers.id).select("order_id", "name")
    assert sorted((r.order_id, r.name) for r in df.collect())[:2] == [(1, "Ana"), (2, "Ana")]
    assert df.count() == 6


def test_sql_group_by_position_passes_through(session):
    # Known difference GRP-01: in IRIS SQL, GROUP BY 1 groups by the constant 1.
    rows = session.sql("SELECT status, COUNT(*) AS n FROM MborobakTest.Sales GROUP BY 1").collect()
    assert len(rows) == 1 and rows[0].n == 9


def test_sql_errors(session):
    with pytest.raises(MborobakExecutionError):
        session.sql("SELECT nope FROM MborobakTest.Sales")
    with pytest.raises(UnsupportedTypeError):
        session.sql("SELECT CAST('10:00:00' AS TIME) AS t FROM MborobakTest.Sales")


def test_named_parameters(session):
    df = session.sql("SELECT id, status FROM MborobakTest.Sales WHERE id > :low AND id < :high AND id <> :low + 1",
                     {"low": 2, "high": 7})
    assert sorted(r.id for r in df.collect()) == [4, 5, 6]
    assert session.lastExecution.params == [2, 7, 2]


def test_read_table(session):
    assert session.read.table("MborobakTest.Sales").count() == session.table("MborobakTest.Sales").count() == 9
