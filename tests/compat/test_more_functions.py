"""Differential tests for hashes, edit distance, trigonometry, day names, conv, parse_url, time zones and
first_value/last_value (compatibility matrix §2.12, rows FUN-27 to FUN-34; compiler spec §7.19)."""

from __future__ import annotations

import datetime

import pytest

from mborobak.errors import MborobakExecutionError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def same(pair, dataset, build, **kw):
    assert_same(*pair.both(dataset, build), **kw)


def both_local(pair, rows, schema, build, **kw):
    from mborobak.sql import functions as MF
    from pyspark.sql import functions as SF

    mb = build(MF, pair.mb_session.createDataFrame(rows, schema))
    sp = build(SF, pair.spark.createDataFrame(rows, schema))
    assert_same(mb, sp, **kw)


def test_FUN_27_hashes(pair):
    same(pair, "funcs", lambda F, df: df.select("id", F.md5("s"), F.sha1("s"), F.sha2("s", 256), F.sha2("t", 512),
                                                F.sha2("s", 0), F.sha2("s", 224), F.sha2("t", 384), F.crc32("s")))


def test_FUN_28_levenshtein(pair):
    same(pair, "funcs", lambda F, df: df.select("id", F.levenshtein("s", "t"), F.levenshtein("t", "s"),
                                                F.levenshtein("s", "t", 3)))


def test_FUN_29_trigonometry(pair):
    same(pair, "nums", lambda F, df: df.select("id", F.sin("d"), F.cos("d"), F.tan("d"), F.atan("d"), F.degrees("d"),
                                               F.radians("d"), F.pi()))
    same(pair, "funcs", lambda F, df: df.select("id", F.asin("d"), F.acos("d"), F.cot("i"), F.atan2("d", "i"),
                                                F.sin("n"), F.cos("i")))


def test_FUN_30_day_and_month_names(pair):
    same(pair, "dates", lambda F, df: df.select("id", F.dayname("d"), F.monthname("d"), F.dayname("ts"),
                                                F.monthname("s")))


def test_FUN_31_conv(pair):
    rows = [(1, "100"), (2, "ff"), (3, "-10"), (4, "12x"), (5, None), (6, "zz"), (7, "9223372036854775807"), (8, "")]
    both_local(pair, rows, "id int, s string", lambda F, df: df.select(
        "id", F.conv("s", 10, 2), F.conv("s", 8, 10), F.conv("s", 10, -16), F.conv("s", 10, 36)))
    from mborobak.errors import MborobakArithmeticError

    for name, error in ((pair.mb_session, MborobakArithmeticError), (pair.spark, Exception)):
        with pytest.raises(error):  # over 64 bits: an error under ANSI
            name.createDataFrame([("zzzzzzzzzzzzzzz",)], "s string").selectExpr("conv(s, 36, 10)").collect()


def test_FUN_32_parse_url(pair):
    rows = [(1, "https://user:pw@example.com:8080/a/b.html?q=1&r=two#frag"), (2, "http://Example.COM"),
            (3, None), (4, "ftp://h/x?a=1&a=2"), (5, "mailto:a@b.c"), (6, "/relative/path?x=y")]
    parts = ("HOST", "PATH", "QUERY", "REF", "PROTOCOL", "FILE", "AUTHORITY", "USERINFO")
    both_local(pair, rows, "id int, u string", lambda F, df: df.select(
        "id", *[F.parse_url("u", F.lit(p)) for p in parts], F.parse_url("u", F.lit("QUERY"), F.lit("r")),
        F.parse_url("u", F.lit("QUERY"), F.lit("a"))))


def test_FUN_32_parse_url_invalid(pair):
    from pyspark.sql import functions as SF

    from mborobak.sql import functions as MF

    with pytest.raises(MborobakExecutionError, match="URL"):
        pair.mb_session.createDataFrame([("not a url",)], "u string").select(MF.parse_url("u", MF.lit("HOST"))).collect()
    with pytest.raises(Exception):  # noqa: B017
        pair.spark.createDataFrame([("not a url",)], "u string").select(SF.parse_url("u", SF.lit("HOST"))).collect()


def test_FUN_33_time_zones(pair):
    rows = [(1, datetime.datetime(2026, 3, 8, 12, 30)), (2, datetime.datetime(2026, 11, 1, 1, 30)),
            (3, None), (4, datetime.datetime(2026, 3, 8, 2, 30)), (5, datetime.datetime(2026, 7, 1, 0, 0, 0, 123456))]
    both_local(pair, rows, "id int, ts timestamp_ntz", lambda F, df: df.select(
        "id", F.to_utc_timestamp("ts", "America/Sao_Paulo"), F.from_utc_timestamp("ts", "America/New_York"),
        F.from_utc_timestamp("ts", "+05:30"), F.to_utc_timestamp("ts", "UTC"), F.to_utc_timestamp("ts", "America/New_York"),
        F.from_utc_timestamp("ts", "Asia/Tokyo")), check_types=False)  # timestamp vs timestamp_ntz (TIME-05)


def test_FUN_34_first_value_last_value(pair):
    def build(F, df):
        from mborobak.sql.window import Window as MW
        from pyspark.sql.window import Window as SW

        W = MW if F.__name__.startswith("mborobak") else SW
        w = W.partitionBy("g").orderBy("id")
        return df.select("id", F.first_value("x").over(w), F.last_value("x").over(w),
                         F.first_value("x").over(w.rowsBetween(W.unboundedPreceding, W.unboundedFollowing)))

    same(pair, "wins", build)
