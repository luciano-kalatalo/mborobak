"""Differential tests for writing tables (compatibility matrix §2.16, rows WRITE-xx).

The same DataFrame is written with ``saveAsTable``/``insertInto`` on both sides and read back with
``table()``. Spark's tables are external tables in a temporary directory, so nothing lands in the working
directory.
"""

from __future__ import annotations

import itertools

import pytest

from mborobak.errors import AnalysisError, MborobakExecutionError

from .conftest import SCHEMA, assert_same

pytestmark = pytest.mark.compat

_counter = itertools.count(1)


class Tables:
    def __init__(self, pair, tmp_path):
        self.pair = pair
        self.tmp_path = tmp_path
        self.names: list[str] = []

    def new(self) -> str:
        name = f"Wr{next(_counter)}"
        self.names.append(name)
        return name

    def mb_name(self, name: str) -> str:
        return f"{SCHEMA}.{name}"

    def sp_write(self, df, name: str, mode: str = "errorifexists"):
        writer = df.write.mode(mode)
        if mode in ("errorifexists", "ignore") or not self.pair.spark.catalog.tableExists(name):
            writer = writer.option("path", str(self.tmp_path / name))
        writer.saveAsTable(name)

    def mb_write(self, df, name: str, mode: str = "errorifexists"):
        df.write.mode(mode).saveAsTable(self.mb_name(name))

    def write(self, name: str, build, dataset: str, mode: str = "errorifexists"):
        mb, sp = self.pair.both(dataset, build)
        self.mb_write(mb, name, mode)
        self.sp_write(sp, name, mode)

    def read(self, name: str):
        return self.pair.mb_session.table(self.mb_name(name)), self.pair.spark.table(name)

    def cleanup(self):
        cur = self.pair.mb_session._connection.cursor()
        for name in self.names:
            try:
                cur.execute(f"DROP TABLE {SCHEMA}.{name}")
            except Exception:  # noqa: BLE001 - may not exist
                pass
            self.pair.spark.sql(f"DROP TABLE IF EXISTS {name}")


@pytest.fixture()
def tables(pair, tmp_path):
    t = Tables(pair, tmp_path)
    yield t
    t.cleanup()


def round_trip(tables, dataset: str, build):
    name = tables.new()
    tables.write(name, build, dataset)
    assert_same(*tables.read(name))


def test_WRITE_01_round_trip_of_types_and_names(tables):
    round_trip(tables, "nums", lambda F, df: df.select("id", "i", "l", "d", "n", "n4", (F.col("id") + 1),
                                                       (F.col("i") > 0).alias("pos")))
    round_trip(tables, "strings", lambda F, df: df.select("id", "s", F.upper("s"), F.length("s")))
    round_trip(tables, "dates", lambda F, df: df.select("id", "d", "ts", F.date_add("d", 1).alias("next"),
                                                        F.year("ts")))
    round_trip(tables, "funcs", lambda F, df: df.groupBy("i").agg(F.sum("l"), F.max("s"), F.avg("n")))


def test_WRITE_02_written_strings_compare_as_in_spark(tables):
    name = tables.new()
    tables.write(name, lambda F, df: df.select("id", "s"), "strings")
    mb, sp = tables.read(name)
    assert_same(mb.filter(mb.s == "apple"), sp.filter(sp.s == "apple"))
    assert_same(mb.groupBy("s").count(), sp.groupBy("s").count())
    assert_same(mb.orderBy("s", "id").select("id"), sp.orderBy("s", "id").select("id"), ordered=True)
    mb_sql, _ = mb.filter(mb.s == "apple").toSQL()
    assert "%EXACT" not in mb_sql  # the column has EXACT collation, so no rewrite is needed


def test_WRITE_03_save_modes(tables, pair):
    name = tables.new()
    tables.write(name, lambda F, df: df.select("id", "s").filter("id < 3"), "strings")
    mb, sp = pair.both("strings", lambda F, df: df.select("id", "s"))
    with pytest.raises(AnalysisError, match="already exists"):
        tables.mb_write(mb, name)
    with pytest.raises(Exception, match="already exists|ALREADY_EXISTS"):  # noqa: B017
        tables.sp_write(sp, name)
    tables.mb_write(mb, name, "ignore")
    tables.sp_write(sp, name, "ignore")
    assert_same(*tables.read(name))  # still two rows
    tables.mb_write(mb, name, "append")
    tables.sp_write(sp, name, "append")
    assert_same(*tables.read(name))
    tables.write(name, lambda F, df: df.select("id", F.lit(1.5).alias("x")), "nums", "overwrite")
    assert_same(*tables.read(name))  # new columns


def test_WRITE_04_append_matches_names_and_casts(tables, pair):
    name = tables.new()
    tables.write(name, lambda F, df: df.select("id", F.col("l").alias("big"), F.col("d").alias("dbl"),
                                               F.col("n").cast("string").alias("txt")).limit(0), "nums")
    tables.write(name, lambda F, df: df.select(F.col("d").alias("DBL"), F.col("i").alias("Big"), "id",
                                               F.col("i").alias("txt")).filter("id < 9"), "nums", "append")
    tables.write(name, lambda F, df: df.select(F.col("id").alias("dbl"), F.col("id").alias("big"), "id",
                                               F.col("s").alias("txt")).filter("id < 4"), "strings", "append")
    assert_same(*tables.read(name))


def test_WRITE_05_append_rejects_unsafe_types_and_overflows(tables, pair):
    name = tables.new()
    tables.write(name, lambda F, df: df.select("id", "i").limit(0), "nums")
    mb, sp = pair.both("strings", lambda F, df: df.select("id", F.col("s").alias("i")))
    with pytest.raises(AnalysisError, match="store assignment"):
        tables.mb_write(mb, name, "append")
    with pytest.raises(Exception):  # noqa: B017 - Spark: INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST
        tables.sp_write(sp, name, "append")
    mb, sp = pair.both("nums", lambda F, df: df.select("id", F.col("l").alias("i")))
    with pytest.raises(MborobakExecutionError):
        tables.mb_write(mb, name, "append")
    with pytest.raises(Exception):  # noqa: B017 - Spark: CAST_OVERFLOW_IN_TABLE_INSERT
        tables.sp_write(sp, name, "append")
    assert_same(*tables.read(name))  # nothing was written


def test_WRITE_06_insert_into_by_position(tables, pair):
    name = tables.new()
    tables.write(name, lambda F, df: df.select("id", "s").filter("id = 1"), "strings")
    mb, sp = pair.both("strings", lambda F, df: df.select(F.col("id") + 100, F.upper("s")))
    mb.write.insertInto(tables.mb_name(name))
    sp.write.insertInto(name)
    assert_same(*tables.read(name))
    mb.filter("id > 7").write.insertInto(tables.mb_name(name), overwrite=True)
    sp.filter("id > 7").write.insertInto(name, overwrite=True)
    assert_same(*tables.read(name))


@pytest.mark.xfail(strict=True, reason="WRITE-07: known difference in the compatibility matrix")
def test_WRITE_07_decimals_with_more_than_19_integer_digits(tables):
    # NUMERIC(30,2) reads back as decimal(21,2): IRIS keeps at most 19 digits before the point
    round_trip(tables, "nums", lambda F, df: df.select("id", F.col("n").cast("decimal(30,2)").alias("n")))


def test_WRITE_09_names_iris_cannot_hold(pair, tables):
    mb, sp = pair.both("strings", lambda F, df: df.select(F.col("id").alias("a.b"), F.col("s").alias("x,y")))
    name = tables.new()
    with pytest.raises(AnalysisError, match="alias"):
        tables.mb_write(mb, name)
    tables.sp_write(sp, name)  # Spark writes them
