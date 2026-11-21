"""rand, randn and sampling, compiled without IRIS (compiler spec §7.20)."""

from __future__ import annotations

import re

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import UnsupportedOperationError
from mborobak.randomness import distinct_rows
from mborobak.scan import scan_source
from mborobak.sql import functions as F
from mborobak.sql.types import DoubleType, IntegerType, StringType


@pytest.fixture()
def s():
    return MborobakSession._for_tests(StaticCatalog({
        "K": [("id", IntegerType(), False, None, True), ("g", StringType())],  # primary key id
        "U": [("a", IntegerType()), ("s", StringType()), ("x", DoubleType())],
    }))


def sql(df) -> str:
    return re.sub(r"mborobak_w\d+", "mborobak_w", df._session._compile(df._plan).text)


def test_names_and_types(s):
    df = s.table("U").select(F.rand(42), F.randn(-7), F.rand(1).alias("r"))
    assert df.dtypes == [("rand(42)", "double"), ("randn(-7)", "double"), ("r", "double")]
    assert [f.nullable for f in df.schema.fields] == [False, False, False]
    assert s.table("U").selectExpr("rand(3)", "randn(4) z").columns == ["rand(3)", "z"]
    a, b = F.rand(), F.rand()
    assert a._expr.args[0].value != b._expr.args[0].value  # a new random seed each time
    with pytest.raises(TypeError):
        F.rand("1")


def test_copies_are_numbered_unless_rows_are_distinct(s):
    u, k = s.table("U"), s.table("K")
    assert "ROW_NUMBER() OVER (PARTITION BY" in sql(u.withColumn("r", F.rand(1)))
    for distinct in (k, k.filter(F.col("id") > 1), k.withColumnRenamed("id", "key"), u.distinct(),
                     u.groupBy("s").count(), u.select("a").dropDuplicates(), k.join(k.alias("z").select(
                         F.col("id").alias("id2")), F.col("id") == F.col("id2"))):
        assert distinct_rows(distinct._plan), distinct
        assert "ROW_NUMBER" not in sql(distinct.withColumn("r", F.rand(1)))
    for repeated in (u, k.select("g"), k.select(F.col("id") + 1), u.union(u)):
        assert not distinct_rows(repeated._plan)


def test_each_use_hashes_the_row_once(s):
    text = sql(s.table("U").filter(F.rand(7) < 0.5).orderBy(F.rand(2)))
    assert text.count("MD5(") == 2  # no NaN or NULL guards repeat it
    assert "'7' || CHAR(1)" in text and "'2' || CHAR(1)" in text
    assert sql(s.table("U").withColumn("z", F.randn(3))).count("MD5(") == 2  # two uniforms


def test_hidden_row_number_is_dropped(s):
    for df in (s.table("U").filter(F.rand(1) < 0.5), s.table("U").orderBy(F.rand(1)),
               s.table("U").sample(0.3, 1), s.table("U").withColumn("r", F.rand(1))):
        assert "mborobak" not in " ".join(df.columns)
        assert df.columns[:3] == ["a", "s", "x"]


def test_sample_arguments(s):
    u = s.table("U")
    params = lambda df: df._session._compile(df._plan).params  # noqa: E731
    assert params(u.sample(0.25)) == [0.25]
    assert sql(u.sample(0.25, 42)) == sql(u.sample(fraction=0.25, seed=42)) == sql(u.sample(False, 0.25, 42))
    assert sql(u.sample(0.25, 42)) != sql(u.sample(0.25, 43))
    with pytest.raises(UnsupportedOperationError):
        u.sample(True, 0.5)
    with pytest.raises(ValueError):
        u.sample(fraction=1.5)
    with pytest.raises(TypeError):
        u.sample(fraction="0.5")


def test_random_split(s):
    parts = s.table("U").randomSplit([2, 1, 1], seed=5)
    bounds = [p._session._compile(p._plan).params for p in parts]
    assert bounds == [[0.0, 0.5], [0.5, 0.75], [0.75, 1.0]]
    assert len({sql(p).split("WHERE")[1].split(">=")[0] for p in parts}) == 1  # one seed, one row hash
    with pytest.raises(ValueError):
        s.table("U").randomSplit([0, 0])
    with pytest.raises(ValueError):
        s.table("U").randomSplit([-1, 2])


def test_sample_by(s):
    df = s.table("U").sampleBy("s", {"a": 0.5, "b": 1}, seed=3)
    assert sql(df).count("MD5(") == 1
    assert sql(s.table("U").stat.sampleBy(F.col("s"), {"a": 0.5, "b": 1}, 3)) == sql(df)
    with pytest.raises(ValueError):
        s.table("U").sampleBy("s", {"a": 2})
    with pytest.raises(TypeError):
        s.table("U").sampleBy("s", {None: 0.5})


def test_random_in_join_condition_is_rejected(s):
    u, k = s.table("U"), s.table("K")
    with pytest.raises(UnsupportedOperationError):
        sql(u.join(k, (F.col("a") == F.col("id")) & (F.rand(1) < 0.5)))


def test_scanner_counts_sampling_as_supported():
    report = scan_source("from pyspark.sql import functions as F\ndf.sampleBy('k', {1: 0.5})\ndf.randomSplit([0.8, 0.2], seed=1)\n"
                         "df.withColumn('r', F.rand(seed=42))\ndf.na.replace(1, 2)\n", "x.py")
    statuses = {f.label: f.status for f in report.findings.values()}
    assert {statuses[n] for n in (".sampleBy()", ".randomSplit()", "F.rand", ".na.replace()")} == {"supported"}
