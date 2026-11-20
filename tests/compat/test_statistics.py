"""Differential tests for statistics and describe()/summary() (compatibility matrix §2.20, rows STAT-xx)
and the small DataFrame API of the same release (rows DF-xx)."""

from __future__ import annotations

import pytest

from mborobak.errors import UnsupportedOperationError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def same(pair, build, dataset="stats", **kw):
    assert_same(*pair.both(dataset, build), **kw)


def test_STAT_01_stddev_and_variance(pair):
    same(pair, lambda F, df: df.groupBy("g").agg(F.stddev("x"), F.stddev_samp("x"), F.stddev_pop("x"), F.std("x"),
                                                 F.variance("x"), F.var_samp("x"), F.var_pop("x")))
    same(pair, lambda F, df: df.groupBy("g").agg(F.stddev("i"), F.variance("n"), F.var_pop("i")))
    same(pair, lambda F, df: df.agg(F.stddev("x"), F.var_pop("n")))


def test_STAT_02_correlation_and_covariance(pair):
    same(pair, lambda F, df: df.groupBy("g").agg(F.corr("x", "y"), F.covar_pop("x", "y"), F.covar_samp("x", "y")))
    same(pair, lambda F, df: df.agg(F.corr("i", "n"), F.covar_samp("i", "x")))


def test_STAT_03_skewness_and_kurtosis(pair):
    same(pair, lambda F, df: df.groupBy("g").agg(F.skewness("x"), F.kurtosis("x"), F.skewness("i"),
                                                 F.kurtosis("n")))


def test_STAT_04_median_and_percentile(pair):
    same(pair, lambda F, df: df.groupBy("g").agg(F.median("x"), F.median("i"), F.median("n"),
                                                 F.percentile("x", 0.25), F.percentile("i", 0.9), F.percentile("y", 0.0),
                                                 F.percentile("y", 1.0)))
    same(pair, lambda F, df: df.agg(F.median("x"), F.percentile("n", 0.75), F.count("*")))
    same(pair, lambda F, df: df.groupBy("g").agg((F.median("x") * 2 + F.max("i")).alias("m")))
    same(pair, lambda F, df: df.select(F.median("y")))


def same_text(mb, sp):
    """describe()/summary() return numbers as text: compare them as numbers, within the suite's tolerance
    (IRIS's and Spark's standard deviations can differ in the 17th digit), and everything else exactly."""
    import math

    assert mb.columns == sp.columns and mb.dtypes == sp.dtypes
    for m, s in zip(mb.collect(), sp.collect()):
        for a, b in zip(m, s):
            try:
                fa, fb = float(a), float(b)
            except (TypeError, ValueError):
                assert a == b, f"{m} vs {s}"
                continue
            assert (math.isnan(fa) and math.isnan(fb)) or math.isclose(fa, fb, rel_tol=1e-12), f"{m} vs {s}"
            assert ("." in a) == ("." in b) and ("E" in a) == ("E" in b), f"format: {a} vs {b}"


def test_STAT_05_describe_and_summary(pair):
    mb, sp = pair.mb("stats"), pair.sp("stats")
    same_text(mb.describe(), sp.describe())
    same_text(mb.describe("x", "s"), sp.describe("x", "s"))
    same_text(mb.summary(), sp.summary())
    same_text(mb.summary("count", "min", "33%", "max"), sp.summary("count", "min", "33%", "max"))


def test_STAT_06_unsupported_forms(pair):
    from mborobak.sql import functions as F
    from mborobak.sql.window import Window

    with pytest.raises(UnsupportedOperationError):
        F.percentile("x", [0.25, 0.75])
    with pytest.raises(UnsupportedOperationError):
        pair.mb("stats").select(F.median("x").over(Window.partitionBy("g"))).collect()


def test_DF_01_replace(pair):
    same(pair, lambda F, df: df.replace("a", "A"))
    same(pair, lambda F, df: df.replace(["a", "b"], ["x", "y"], subset=["g", "s"]))
    same(pair, lambda F, df: df.replace({1.0: 100.0, 2.0: 200.0}))
    same(pair, lambda F, df: df.na.replace(1, 9, subset=["i", "x"]))
    same(pair, lambda F, df: df.replace("a", None, "g"))


def test_DF_02_transform_todf_views(pair):
    def top(df, n):
        return df.orderBy("id").limit(n)

    same(pair, lambda F, df: df.transform(top, 3).toDF("a", "b", "c", "d", "e", "f", "h"), ordered=True)
    mb, sp = pair.mb("stats"), pair.sp("stats")
    mb.filter("i > 1").createOrReplaceTempView("stats_big")
    sp.filter("i > 1").createOrReplaceTempView("stats_big")
    query = "SELECT g, count(*) AS c, median(x) FROM stats_big GROUP BY g"
    assert_same(pair.mb_session.sql(query), pair.spark.sql(query))
    assert_same(pair.mb_session.table("stats_big"), pair.spark.table("stats_big"))
    pair.spark.catalog.dropTempView("stats_big")


def test_DF_03_is_empty_and_tail(pair):
    mb, sp = pair.mb("stats"), pair.sp("stats")
    assert mb.isEmpty() == sp.isEmpty() is False
    assert mb.filter("id > 100").isEmpty() == sp.filter("id > 100").isEmpty() is True
    assert [tuple(r) for r in mb.orderBy("id").tail(2)] == [tuple(r) for r in sp.orderBy("id").tail(2)]
