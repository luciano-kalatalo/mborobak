"""rand, randn, sample, randomSplit and sampleBy on IRIS (compiler spec §7.20)."""

from __future__ import annotations

import collections
import math
import statistics

import pytest

from mborobak.sql import functions as F

SCHEMA = "MborobakTest"  # as in conftest.py

pytestmark = pytest.mark.iris


def rows(df) -> collections.Counter:
    return collections.Counter(tuple(r) for r in df.collect())


@pytest.fixture()
def many(session):
    """2,000 rows, a third of them repeated: identical rows must still get their own values."""
    data = [(i % 1400, ["a", "b", "c"][i % 3], float(i % 7)) for i in range(2000)]
    return session.createDataFrame(data, "k int, s string, x double")


def test_rand_is_repeatable_and_uniform(session, many):
    df = many.withColumn("r", F.rand(42))
    first, again = df.collect(), df.collect()
    assert [tuple(r) for r in first] == [tuple(r) for r in again]
    values = [r.r for r in first]
    assert all(0.0 <= v < 1.0 for v in values)
    assert len(set(values)) == len(values)  # repeated rows get different values too
    assert abs(statistics.fmean(values) - 0.5) < 0.03
    assert df.dtypes[-1] == ("r", "double") and df.schema["r"].nullable is False
    other = [r.r for r in many.withColumn("r", F.rand(43)).collect()]
    assert sum(a < 0.5 and b < 0.5 for a, b in zip(values, other)) / len(values) == pytest.approx(0.25, abs=0.04)


def test_rand_on_special_values(session):
    sales = session.table(f"{SCHEMA}.Sales")
    out = sales.select("id", F.rand(1).alias("r")).collect()
    assert len(out) == 9 and all(0.0 <= r.r < 1.0 for r in out)


def test_randn(many):
    values = [r[0] for r in many.select(F.randn(7)).collect()]
    assert abs(statistics.fmean(values)) < 0.1
    assert abs(statistics.stdev(values) - 1.0) < 0.1
    assert all(math.isfinite(v) for v in values)


def test_random_split_covers_every_row_once(many):
    parts = many.randomSplit([0.8, 0.2], seed=7)
    counts = [p.count() for p in parts]
    assert sum(counts) == 2000 and 1500 < counts[0] < 1700
    assert rows(parts[0]) + rows(parts[1]) == rows(many)
    assert [p.count() for p in many.randomSplit([0.8, 0.2], seed=7)] == counts  # same seed, same split
    thirds = many.randomSplit([1, 1, 1], seed=3)
    assert sum((rows(p) for p in thirds), collections.Counter()) == rows(many)


def test_sample(many):
    s = many.sample(fraction=0.2, seed=42)
    n = s.count()
    assert 330 < n < 470 and n == many.sample(0.2, 42).count() == many.sample(False, 0.2, 42).count()
    assert many.sample(0.0, 1).count() == 0 and many.sample(1.0, 1).count() == 2000
    # three distinct values only: copies are sampled one by one, not all or none
    assert 330 < many.select("s").sample(0.2, 5).count() < 470


def test_sample_by(many):
    out = many.sampleBy("s", {"a": 0.5, "b": 1.0}, seed=9)
    by = collections.Counter(r.s for r in out.collect())
    assert by["b"] == 667 and by["c"] == 0 and 250 < by["a"] < 420
    assert out.count() == many.stat.sampleBy(F.col("s"), {"a": 0.5, "b": 1.0}, 9).count()


def test_order_by_rand_is_a_repeatable_shuffle(session):
    words = session.table(f"{SCHEMA}.Words")
    first = [r.id for r in words.orderBy(F.rand(11)).collect()]
    assert sorted(first) == list(range(1, 13)) and first != sorted(first)
    assert [r.id for r in words.orderBy(F.rand(11)).collect()] == first


def test_primary_key_skips_numbering(session, raw_connection):
    cur = raw_connection.cursor()
    try:
        cur.execute(f"DROP TABLE {SCHEMA}.Keyed")
    except Exception:  # noqa: BLE001 - may not exist
        pass
    cur.execute(f"CREATE TABLE {SCHEMA}.Keyed (id INT PRIMARY KEY, g VARCHAR(5))")
    cur.executemany(f"INSERT INTO {SCHEMA}.Keyed VALUES (?, ?)", [[i, "x"] for i in range(500)])
    try:
        df = session.table(f"{SCHEMA}.Keyed")
        train, test = df.randomSplit([0.75, 0.25], seed=1)
        assert "ROW_NUMBER" not in train.toSQL()[0]
        assert "ROW_NUMBER" in df.select("g").sample(0.5, 1).toSQL()[0]  # the key is gone: copies numbered
        assert train.count() + test.count() == 500 and not set(rows(train)) & set(rows(test))
    finally:
        cur.execute(f"DROP TABLE {SCHEMA}.Keyed")


def test_in_sql_expressions(session, many):
    many.createOrReplaceTempView("many")
    try:
        assert session.sql("SELECT k FROM many WHERE rand(5) < 0.5").count() == many.filter("rand(5) < 0.5").count()
        assert [r.k for r in session.sql("SELECT k FROM many ORDER BY rand(2) LIMIT 5").collect()] == \
            [r.k for r in many.orderBy(F.rand(2)).limit(5).select("k").collect()]
    finally:
        session.catalog.dropTempView("many")
