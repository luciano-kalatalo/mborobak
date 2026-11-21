"""Differential tests for pivot, unpivot, rollup and cube (compatibility matrix §2.22, rows SHAPE-xx)."""

from __future__ import annotations

import pytest

from mborobak.errors import AnalysisError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def same(pair, name, build, **kw):
    mb, sp = pair.both(name, build)
    assert_same(mb, sp, **kw)
    assert [f.nullable for f in mb.schema.fields] == [f.nullable for f in sp.schema.fields]


def test_SHAPE_01_pivot_with_values(pair):
    same(pair, "wins", lambda F, df: df.groupBy("k").pivot("g", ["a", "A", "b", "zz"]).sum("x"))
    same(pair, "wins", lambda F, df: df.groupBy("k").pivot("g", ["a", None]).count())
    same(pair, "wins", lambda F, df: df.groupBy("g").pivot("k", [1, 2, 3]).agg(F.sum("n"), F.count("x").alias("c")))
    same(pair, "wins", lambda F, df: df.groupBy("g").pivot("k", ["2", "5"]).agg(F.avg("x")))  # cast to the column


def test_SHAPE_02_pivot_without_values(pair):
    same(pair, "wins", lambda F, df: df.groupBy("k").pivot("g").count())  # null first, case-sensitive, '10' < '9'
    same(pair, "wins", lambda F, df: df.groupBy("g").pivot("k").agg(F.countDistinct("x"), F.min("n")))
    same(pair, "wins", lambda F, df: df.groupBy().pivot("g").agg(F.sum("x")))
    same(pair, "stats", lambda F, df: df.groupBy("g").pivot("i").agg(F.sum("x") + 1))


def test_SHAPE_03_rollup_and_cube(pair):
    same(pair, "wins", lambda F, df: df.rollup("g", "k").agg(F.sum("x"), F.count("*")))
    same(pair, "wins", lambda F, df: df.rollup("k").count())
    same(pair, "wins", lambda F, df: df.cube("g", "k").agg(F.avg("n").alias("a")))
    same(pair, "wins", lambda F, df: df.cube(F.col("k") % 2).agg(F.max("x")))
    same(pair, "stats", lambda F, df: df.rollup("g").agg(F.median("x"), F.stddev("i")))


def test_SHAPE_04_unpivot_and_melt(pair):
    same(pair, "wins", lambda F, df: df.unpivot("id", ["k", "x"], "var", "val"))
    same(pair, "wins", lambda F, df: df.unpivot(["id", "g"], ["x", "d"], "var", "val"))  # int and double: double
    same(pair, "wins", lambda F, df: df.select("id", "k", "x").unpivot("id", None, "c", "v"))
    same(pair, "wins", lambda F, df: df.melt(ids=["id"], values=["n"], variableColumnName="v", valueColumnName="w"))
    same(pair, "wins", lambda F, df: df.select("g", "id").unpivot([], ["g"], "v", "w"))


def test_SHAPE_05_errors(pair):
    for df, error in ((pair.mb("wins"), AnalysisError), (pair.sp("wins"), Exception)):
        with pytest.raises(error):
            df.unpivot("id", ["g", "k"], "v", "w").collect()  # string and int: no common type
        with pytest.raises(error):
            df.rollup("g").pivot("k").count().collect()
