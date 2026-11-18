"""Regression tests for the code review of 2026-11-18 (one test per finding)."""

from __future__ import annotations

import math

import pytest

from mborobak import MborobakSession
from mborobak import expressions as E
from mborobak.catalog import StaticCatalog
from mborobak.errors import UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, DoubleType, IntegerType, StringType


@pytest.fixture()
def session():
    def describer(sql, params):
        return [("id", 4, None, None, 10, 0, 1), ("i", 4, None, None, 10, 0, 1)]

    return MborobakSession._for_tests(StaticCatalog({
        "A": [("id", IntegerType()), ("i", IntegerType()), ("d", DoubleType()), ("n", DecimalType(10, 2)),
              ("s", StringType())],
        "B": [("p", IntegerType()), ("q", IntegerType())],
    }), describer=describer)


def test_repeated_column_gets_its_own_id_and_union_keeps_both(session):
    a, b = session.table("A"), session.table("B")
    picked = a.select(F.col("i"), F.col("i").alias("j"))
    assert len({attr.id for attr in picked._plan.output}) == 2
    text, _ = picked.union(b.select("p", "q")).toSQL()
    assert 'SELECT "p" AS "i", "q" AS "j"' in text
    assert text.startswith('SELECT t1."i", t1."j"')


def test_same_cte_with_other_args_is_refused(session):
    q = "WITH c AS (SELECT id, i FROM t WHERE i > ?) SELECT id, i FROM c"
    x, y = session.sql(q, [5]), session.sql(q, [25])
    with pytest.raises(UnsupportedOperationError, match="same args"):
        x.join(y.withColumnRenamed("id", "id2").withColumnRenamed("i", "i2"), F.col("id") == F.col("id2")).toSQL()
    same = session.sql(q, [5])
    text, params = x.join(same.withColumnRenamed("id", "id2").withColumnRenamed("i", "i2"),
                          F.col("id") == F.col("id2")).toSQL()
    assert text.count("WITH c AS") == 1 and params == [5]  # one shared WITH, one parameter


def test_decimal_division_types_follow_spark(session):
    a = session.table("A")
    df = a.select(F.col("n") / 2, F.col("n") / F.col("i"), F.col("n") / F.col("d"), F.col("i") / 2)
    assert [t for _, t in df.dtypes] == ["decimal(14,6)", "decimal(21,13)", "double", "double"]
    text, _ = df.toSQL()
    assert '("n" / ?)' in text and '(CAST("n" AS DOUBLE) / "d")' in text


def test_boolean_to_string_is_true_false(session):
    text, _ = session.table("A").select((F.col("i") > 1).cast("string").alias("b")).toSQL()
    assert "THEN 'true' WHEN" in text and "THEN 'false' END" in text


def test_dropped_sort_key_is_carried_and_reapplied(session):
    a = session.table("A")
    df = a.orderBy(F.col("i").desc()).select("id").limit(3).filter(F.col("id") > 0)
    text, params = df.toSQL()
    inner, outer = text.split(") AS t1")
    assert 'SELECT TOP ? "id", "i"' in inner
    assert outer.strip().endswith('ORDER BY t1."i" DESC')
    assert df.columns == ["id"]


def test_agg_dict_names(session):
    df = session.table("A").groupBy("s").agg({"i": "mean", "*": "count"})
    assert df.columns == ["s", "avg(i)", "count(1)"]


def test_min_without_columns_uses_all_numeric_columns(session):
    assert session.table("A").groupBy("id").min().columns == ["id", "min(id)", "min(i)", "min(d)", "min(n)"]


def test_guarded_cast_operand_is_computed_once(session):
    df = session.table("A").select("id", (F.col("d") * 2).cast("string").cast("double").cast("int").alias("x"))
    text, params = df.toSQL()
    assert text.count('("d" * ?)') == 1
    assert len(params) == 4 and params[-1] == 2 and math.isnan(params[0])
    assert df.columns == ["id", "x"]
    assert "mborobak_h" not in text.splitlines()[0].split(" AS ")[-1]  # hidden columns stay hidden


def test_transform_keeps_unchanged_trees_identical():
    lit = E.Literal(float("nan"), DoubleType())
    expr = E.BinaryArithmetic("+", lit, lit)
    assert E.transform(expr, lambda e: e) is expr
