"""Differential tests for window functions and dropDuplicates (compatibility matrix §2.13, rows WIN-xx).

Every window that numbers rows (row_number, ntile, lag, first) orders by a unique key, so the result is
deterministic in both engines. Partition "b" holds +inf and -inf, so sums of ``d`` over it are avoided
(FP-12).
"""

from __future__ import annotations

import pytest

from mborobak.errors import AnalysisError, UnsupportedOperationError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def same(pair, build, **kw):
    assert_same(*pair.both("wins", build), **kw)


def W(F):
    """The Window class of the engine whose functions module is F."""
    if F.__name__.startswith("pyspark"):
        from pyspark.sql import Window
    else:
        from mborobak.sql import Window
    return Window


def test_WIN_01_ranking_functions(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("k")
        u = W(F).partitionBy("g").orderBy("k", "id")
        return df.select("id", F.row_number().over(u), F.rank().over(w), F.dense_rank().over(w),
                         F.percent_rank().over(w), F.cume_dist().over(w), F.ntile(2).over(u))
    same(pair, build)


def test_WIN_02_lag_lead(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("id")
        return df.select("id", F.lag("x").over(w), F.lag("x", 2, -1).over(w), F.lead("x").over(w),
                         F.lead("n", 1, 0).over(w), F.lag("x", -1).over(w), F.lag("g").over(W(F).orderBy("id")),
                         F.lag("x", 0).over(w))
    same(pair, build)


def test_WIN_03_running_aggregates_share_ties(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("k")
        # max(d) is left out: IRIS's MAX ignores NaN in windows too (FP-06)
        return df.select("id", F.sum("x").over(w), F.count("*").over(w), F.avg("n").over(w), F.max("x").over(w),
                         F.min("d").over(w))
    same(pair, build)


def test_WIN_04_whole_partition_aggregates(pair):
    def build(F, df):
        w = W(F).partitionBy("g")
        return df.select("id", F.sum("x").over(w), F.avg("n").over(w), F.count("*").over(w), F.count("x").over(w),
                         F.min("d").over(w), F.max("k").over(W(F).partitionBy()))
    same(pair, build)
    same(pair, lambda F, df: df.select("id", F.min("g").over(W(F).partitionBy("k")),
                                       F.max("g").over(W(F).partitionBy())))


def test_WIN_05_rows_frames(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("id")
        Wn = W(F)
        return df.select("id", F.sum("x").over(w.rowsBetween(-1, 1)),
                         F.sum("x").over(w.rowsBetween(Wn.unboundedPreceding, Wn.currentRow)),
                         F.sum("n").over(w.rowsBetween(1, Wn.unboundedFollowing)),
                         F.avg("x").over(w.rowsBetween(-2, 0)), F.count("x").over(w.rowsBetween(0, 0)),
                         F.min("k").over(w.rowsBetween(Wn.unboundedPreceding, Wn.unboundedFollowing)))
    same(pair, build)


def test_WIN_06_supported_range_frames(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("k")
        Wn = W(F)
        return df.select("id", F.sum("x").over(w.rangeBetween(Wn.unboundedPreceding, Wn.currentRow)),
                         F.sum("x").over(w.rangeBetween(Wn.unboundedPreceding, Wn.unboundedFollowing)),
                         F.sum("x").over(w.rangeBetween(Wn.currentRow, Wn.unboundedFollowing)),
                         F.count("*").over(W(F).orderBy(F.col("k").desc()).rangeBetween(Wn.currentRow,
                                                                                       Wn.unboundedFollowing)))
    same(pair, build)


def test_WIN_07_order_keys_nulls_doubles_strings(pair):
    def build(F, df):
        Wn = W(F)
        return df.select("id", F.row_number().over(Wn.orderBy("d", "id")),
                         F.row_number().over(Wn.orderBy(F.col("d").desc(), "id")),
                         F.row_number().over(Wn.orderBy(F.col("k").asc_nulls_last(), "id")),
                         F.row_number().over(Wn.orderBy(F.col("k").desc_nulls_first(), "id")),
                         F.row_number().over(Wn.orderBy("g", "id")),
                         F.rank().over(Wn.orderBy(F.col("g").desc())))
    same(pair, build)


def test_WIN_08_partitions_are_case_sensitive_and_group_nulls(pair):
    same(pair, lambda F, df: df.select("id", F.count("*").over(W(F).partitionBy("g")),
                                       F.count("*").over(W(F).partitionBy("k")),
                                       F.sum("x").over(W(F).partitionBy(F.upper("g")))))


def test_WIN_09_first_last(pair):
    def build(F, df):
        w = W(F).partitionBy("g").orderBy("id")
        Wn = W(F)
        return df.select("id", F.first("x").over(w), F.last("x").over(w),
                         F.last("x").over(w.rowsBetween(Wn.unboundedPreceding, Wn.unboundedFollowing)),
                         F.first("n").over(w.rowsBetween(-1, 1)),
                         F.first("x").over(w.rangeBetween(Wn.currentRow, Wn.unboundedFollowing)))
    same(pair, build)


def test_WIN_10_window_results_in_filters_and_expressions(pair):
    def top(F, df):  # top 2 per group, the most common window pattern
        w = W(F).partitionBy("g").orderBy(F.col("x").desc_nulls_last(), "id")
        return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 2).select("id", "g", "rn")
    same(pair, top)

    def exprs(F, df):
        w = W(F).partitionBy("g").orderBy("id")
        return df.select("id", (F.col("x") - F.lag("x").over(w)).alias("delta"),
                         F.when(F.row_number().over(w) == 1, "first").otherwise("other").alias("pos"),
                         (F.col("x") > F.avg("x").over(W(F).partitionBy("g"))).alias("above"),
                         (F.sum("x").over(w) * 100 / F.sum("x").over(W(F).partitionBy("g"))).alias("share"))
    same(pair, exprs)


def test_WIN_11_windows_after_limit_aggregate_and_sort(pair):
    same(pair, lambda F, df: df.orderBy("id").limit(5).withColumn("r", F.rank().over(W(F).orderBy(F.col("id").desc())))
         .select("id", "r"))
    same(pair, lambda F, df: df.groupBy("g").agg(F.sum("x").alias("s"))
         .withColumn("r", F.dense_rank().over(W(F).orderBy(F.col("s").desc()))))
    # Spark re-sorts rows for a window, so an earlier orderBy doesn't define the output order
    same(pair, lambda F, df: df.orderBy(F.col("id").desc())
         .withColumn("c", F.sum("x").over(W(F).orderBy("id"))).select("id", "c"))
    same(pair, lambda F, df: df.withColumn("rn", F.row_number().over(W(F).partitionBy("g").orderBy("id")))
         .groupBy("rn").count())


def test_WIN_12_analysis_errors(pair):
    from mborobak.sql import functions as MF
    from pyspark.sql import functions as SF

    def both_fail(build):
        with pytest.raises(AnalysisError):  # Mborobak raises at the call, as for other analysis errors
            build(MF, pair.mb("wins")).collect()
        with pytest.raises(Exception):  # noqa: B017
            build(SF, pair.sp("wins")).collect()

    both_fail(lambda F, df: df.select(F.row_number().over(W(F).partitionBy("g"))))
    both_fail(lambda F, df: df.select(F.lag("x").over(W(F).orderBy("id").rowsBetween(-1, 1))))
    both_fail(lambda F, df: df.select(F.rank().over(W(F).orderBy("id").rowsBetween(-1, 1))))
    both_fail(lambda F, df: df.filter(F.row_number().over(W(F).orderBy("id")) == 1))
    both_fail(lambda F, df: df.select(F.countDistinct("x").over(W(F).partitionBy("g"))))
    both_fail(lambda F, df: df.select(F.rank()))
    both_fail(lambda F, df: df.select(F.sum("x").over(W(F).orderBy("id").rowsBetween(1, -1))))


def test_WIN_13_range_frames_with_offsets_are_unsupported(pair):
    sp = pair.sp("wins")
    from pyspark.sql import Window as SW
    from pyspark.sql import functions as SF

    sp.select(SF.sum("x").over(SW.orderBy("k").rangeBetween(-1, 1))).collect()  # Spark computes it
    from mborobak.sql import Window as MW
    from mborobak.sql import functions as MF

    with pytest.raises(UnsupportedOperationError):
        pair.mb("wins").select(MF.sum("x").over(MW.orderBy("k").rangeBetween(-1, 1)))


def test_WIN_14_drop_duplicates(pair):
    same(pair, lambda F, df: df.dropDuplicates(["g"]).select("g"))
    same(pair, lambda F, df: df.dropDuplicates(["g", "k"]).select("g", "k"))
    same(pair, lambda F, df: df.select("g").dropDuplicates())
    same(pair, lambda F, df: df.select("k", "x").drop_duplicates(["k"]).groupBy("k").count())
    # id 4 (-1E19) is left out: as a key it merges with NULL in IRIS (FP-10)
    same(pair, lambda F, df: df.filter(F.col("id") != 4).dropDuplicates(["d"]).select("d"))
