"""Phase 2 prototype against a live IRIS: results must follow PySpark semantics."""

from __future__ import annotations

import datetime
import decimal
import math

import pytest

from mborobak.errors import MborobakArithmeticError, MborobakCastError
from mborobak.sql import functions as F

pytestmark = pytest.mark.iris

D = decimal.Decimal


def test_scope_pipeline_table_filter_groupby_agg_orderby_collect(session):
    sales = session.table("MborobakTest.Sales")
    result = (
        sales.filter(F.col("status") == "PAID")
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("revenue"))
        .orderBy(F.col("revenue").desc())
    )
    rows = result.collect()
    # Exact match only: 'paid' and 'PAID ' are other values in PySpark (STR-01, STR-02).
    # Descending order puts the NULL revenue last (NULL-03).
    assert [tuple(r) for r in rows] == [(20, D("300.00")), (10, D("100.50")), (None, D("1.00")), (30, None)]
    assert rows[0].revenue == D("300.00") and rows[0]["customer_id"] == 20
    assert result.dtypes == [("customer_id", "int"), ("revenue", "decimal(20,2)")]
    m = session.lastExecution
    assert m.data_queries == 1 and m.schema_lookups == 0 and m.rows == 4


def test_count_runs_one_query(session):
    n = session.table("MborobakTest.Sales").filter(F.col("status") == "PAID").count()
    assert n == 4
    assert session.lastExecution.data_queries == 1


def test_grouping_keeps_exact_string_values(session):
    words = session.table("MborobakTest.Words").filter(F.col("id") <= 7)
    rows = words.groupBy("w").count().orderBy("w").collect()
    assert [r.w for r in rows] == [" apple", "APPLE", "Apple", "Banana", "apple", "apple ", "banana"]
    assert all(r["count"] == 1 for r in rows)


def test_min_max_on_strings_are_binary(session):
    row = session.table("MborobakTest.Words").filter(F.col("id") <= 7).agg(
        F.min("w").alias("lo"), F.max("w").alias("hi")).collect()[0]
    assert (row.lo, row.hi) == (" apple", "banana")


def test_iris_string_semantics_setting(session):
    words = session.table("MborobakTest.Words")
    assert words.filter(F.col("w") == "APPLE").count() == 1
    session.conf.set("mborobak.sql.stringSemantics", "iris")
    assert words.filter(F.col("w") == "APPLE").count() == 4  # apple, Apple, APPLE, 'apple '


def test_double_sort_matches_spark(session):
    rows = session.table("MborobakTest.Sales").select("id", "score").orderBy("score").collect()
    assert [r.id for r in rows] == [2, 5, 3, 9, 7, 1, 6, 8, 4]
    by_id = {r.id: r.score for r in rows}
    assert by_id[3] == -1e19  # not turned into NULL (FP-09)
    assert math.isnan(by_id[4]) and by_id[5] == -math.inf and by_id[2] is None


def test_double_sort_descending(session):
    rows = session.table("MborobakTest.Sales").orderBy(F.col("score").desc()).collect()
    assert [r.id for r in rows] == [4, 8, 6, 1, 7, 9, 3, 5, 2]


def test_order_survives_limit_then_filter(session):
    sales = session.table("MborobakTest.Sales")
    rows = sales.orderBy(F.col("amount").desc()).limit(3).filter(F.col("id") > 1).collect()
    assert [r.id for r in rows] == [3, 5]


def test_string_to_int_cast(session):
    words = session.table("MborobakTest.Words")
    rows = words.filter(F.col("id").isin(8, 9, 12)).select("id", F.col("w").cast("int").alias("n")) \
        .orderBy("id").collect()
    assert [(r.id, r.n) for r in rows] == [(8, 12), (9, 7), (12, None)]


def test_invalid_cast_raises(session):
    words = session.table("MborobakTest.Words").filter(F.col("id") == 10)
    with pytest.raises(MborobakCastError) as info:
        words.select(F.col("w").cast("int")).collect()
    assert info.value.sqlcode == -400
    assert "test_prototype_iris.py" in info.value.operation


def test_division_by_zero_raises(session):
    sales = session.table("MborobakTest.Sales")
    with pytest.raises(MborobakArithmeticError, match="division by zero"):
        sales.select(F.col("id") / (F.col("id") - F.col("id"))).collect()


def test_integer_division_is_double(session):
    row = session.table("MborobakTest.Sales").filter(F.col("id") == 7).select(
        (F.col("id") / 2).alias("half")).first()
    assert row.half == 3.5 and isinstance(row.half, float)


def test_values_follow_schema_types(session):
    row = session.table("MborobakTest.Sales").filter(F.col("id") == 1).first()
    assert row.day == datetime.date(2026, 1, 1)
    assert row.amount == D("100.50") and isinstance(row.score, float)


def test_boolean_projection(session):
    rows = session.table("MborobakTest.Sales").select("id", F.col("status").isNull().alias("missing")) \
        .orderBy("id").collect()
    assert [r.missing for r in rows] == [False] * 8 + [True]


def test_with_column_and_rename(session):
    df = (session.table("MborobakTest.Sales")
          .withColumn("double_id", F.col("id") * 2)
          .withColumnRenamed("customer_id", "cust")
          .drop("score", "day")
          .filter(F.col("double_id") > 14))
    assert df.columns == ["id", "cust", "status", "amount", "double_id"]
    assert [r.double_id for r in df.orderBy("id").collect()] == [16, 18]


def test_take_first_and_show(session, capsys):
    sales = session.table("MborobakTest.Sales").orderBy("id")
    assert [r.id for r in sales.take(2)] == [1, 2]
    assert sales.first().id == 1
    sales.select("id", "status").show(2)
    out = capsys.readouterr().out
    assert "| id|status|" in out and "only showing top 2 rows" in out


def test_to_pandas_types(session):
    pdf = session.table("MborobakTest.Sales").select("id", "customer_id", "amount", "day").toPandas()
    assert str(pdf["id"].dtype) == "int32"
    assert str(pdf["customer_id"].dtype) == "float64"  # has a NULL, as PySpark without Arrow
    assert pdf["amount"].dtype == object and pdf["day"].dtype == object


def test_compiled_sql_is_cached(session):
    df = session.table("MborobakTest.Sales").filter(F.col("id") > 3)
    df.count()
    assert session.lastExecution.cache_hit is False
    df.count()
    assert session.lastExecution.cache_hit is True
