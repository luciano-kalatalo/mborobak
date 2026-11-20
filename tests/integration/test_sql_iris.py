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


def test_regex_functions_are_installed_on_first_use(session, raw_connection):
    from mborobak import MborobakSession

    cur = raw_connection.cursor()
    cur.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'Mborobak'")
    for (name,) in cur.fetchall():  # start without them
        cur.execute(f'DROP FUNCTION "Mborobak"."{name}"')
    df = session.table("MborobakTest.Words").filter(F.col("w").rlike("^[a-z]+$"))
    assert sorted(r.id for r in df.collect()) == [1, 4, 11]  # apple, banana, abc
    assert session._functions_ready == {"RegexpLike1"}
    fresh = MborobakSession.builder.connection(raw_connection).create()  # finds it, creates nothing
    assert fresh.table("MborobakTest.Words").filter(F.col("w").rlike("an")).count() == 2
    assert len(fresh.installFunctions()) == 6


def test_spark_dialect_against_iris_tables(session):
    session.conf.set("mborobak.sql.dialect", "spark")
    df = session.sql("SELECT c.country, count(*) AS orders, sum(s.amount) AS revenue "
                     "FROM MborobakTest.Sales s JOIN MborobakTest.Customers c ON s.customer_id = c.id "
                     "WHERE s.status = 'PAID' GROUP BY c.country ORDER BY revenue DESC")
    assert df.columns == ["country", "orders", "revenue"]
    assert [tuple(r) for r in df.collect()] == [("us", 1, D("300.00")), ("BR", 2, D("100.50"))]  # 'PAID' only, exact
    assert session.lastExecution.data_queries == 1
    top = session.sql("SELECT Sales.id FROM MborobakTest.Sales WHERE Sales.amount > :m ORDER BY 1 LIMIT 2", {"m": 50})
    assert [r.id for r in top.collect()] == [1, 3]
