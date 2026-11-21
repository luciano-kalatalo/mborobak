"""Differential tests for rand, randn and sampling (compatibility matrix §2.21, rows RAND-xx).

Spark's random values come from the seed and each row's position in its partition; Mborobak's from the seed
and the row's columns (compiler spec §7.20), so the values differ (RAND-02). What both must agree on is
compared here: names, types, nullability, the cases whose rows are fixed (fractions 0 and 1), and properties
such as every row landing in exactly one split.
"""

from __future__ import annotations

import collections
import math

import pytest

from mborobak.errors import UnsupportedOperationError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def rows(df) -> collections.Counter:
    def norm(v):
        return "NaN" if isinstance(v, float) and math.isnan(v) else v  # NaN never equals itself

    return collections.Counter(tuple(norm(v) for v in r) for r in df.collect())


def test_RAND_01_names_types_nullability(pair):
    mb, sp = pair.both("nums", lambda F, df: df.select("id", F.rand(42), F.randn(7), F.rand(-3).alias("r")))
    assert mb.columns == sp.columns
    assert mb.dtypes == sp.dtypes
    assert [f.nullable for f in mb.schema.fields] == [f.nullable for f in sp.schema.fields]
    mb, sp = pair.mb("nums").selectExpr("rand(5)", "randn(5) AS z"), pair.sp("nums").selectExpr("rand(5)", "randn(5) AS z")
    assert mb.columns == sp.columns and mb.dtypes == sp.dtypes
    from mborobak.sql import functions as MF
    from pyspark.sql import functions as SF

    for df, F in ((pair.mb("nums"), MF), (pair.sp("nums"), SF)):  # no seed: a random one, shown in the name
        name = df.select(F.rand()).columns[0]
        assert name.startswith("rand(") and name != df.select(F.rand()).columns[0]


def test_RAND_02_values_in_range_and_repeatable(pair):
    from mborobak.sql import functions as MF
    from pyspark.sql import functions as SF

    for df, F in ((pair.mb("nums"), MF), (pair.sp("nums"), SF)):
        out = df.select("id", F.rand(42).alias("r")).orderBy("id").collect()
        assert all(0.0 <= r.r < 1.0 for r in out)
        assert out == df.select("id", F.rand(42).alias("r")).orderBy("id").collect()
        assert len({r.r for r in out}) == len(out)


def test_RAND_03_sample(pair):
    for build in (lambda F, df: df.sample(fraction=1.0, seed=1), lambda F, df: df.sample(0.0, 3),
                  lambda F, df: df.sample(False, 1.0, 5), lambda F, df: df.sample(1.0)):
        assert_same(*pair.both("nums", build))
    for df in (pair.mb("left"), pair.sp("left")):
        s = df.sample(0.5, 42)
        assert s.columns == df.columns and s.dtypes == df.dtypes
        assert s.count() == df.sample(fraction=0.5, seed=42).count() <= df.count()
        assert not (rows(s) - rows(df))


def test_RAND_04_random_split(pair):
    for df in (pair.mb("nums"), pair.sp("nums")):
        parts = df.randomSplit([0.5, 0.3, 0.2], seed=11)
        assert len(parts) == 3 and all(p.columns == df.columns for p in parts)
        assert sum((rows(p) for p in parts), collections.Counter()) == rows(df)
        assert [p.count() for p in df.randomSplit([0.5, 0.3, 0.2], seed=11)] == [p.count() for p in parts]
        whole, empty = df.randomSplit([1.0, 0.0], seed=1)
        assert whole.count() == df.count() and empty.count() == 0


def test_RAND_05_sample_by(pair):
    def build(F, df):
        return df.sampleBy("k", {1: 1.0, 2: 0.0, 9: 1.0}, seed=3)

    assert_same(*pair.both("left", build))
    assert_same(*pair.both("left", lambda F, df: df.stat.sampleBy(F.col("name"), {"a": 1.0, "zz": 1.0}, 1)))
    assert_same(*pair.both("left", lambda F, df: df.sampleBy("k", {}, 1)))


def test_RAND_06_unsupported_and_invalid(pair):
    with pytest.raises(UnsupportedOperationError):
        pair.mb("nums").sample(True, 0.5, 1)
    for df in (pair.mb("nums"), pair.sp("nums")):
        with pytest.raises(Exception):  # noqa: B017 - PySpark raises IllegalArgumentException on collect
            df.sample(fraction=1.5, seed=1).collect()
        with pytest.raises(Exception):  # noqa: B017
            df.sampleBy("id", {1: 2.0}).collect()
        with pytest.raises(Exception):  # noqa: B017
            [p.collect() for p in df.randomSplit([-1.0, 2.0], 1)]
