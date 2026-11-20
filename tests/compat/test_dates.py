"""Differential tests for date and time functions (compatibility matrix §2.14, rows DATE-xx).

Spark's to_timestamp, date_trunc and current_timestamp return ``timestamp`` (with a time zone), where
Mborobak's timestamps are ``timestamp_ntz`` (TIME-01): their rows are compared with check_types=False,
and TIME-05 records the type difference. The Spark fixture runs in UTC, so both give the same values.
"""

from __future__ import annotations

import pytest

from mborobak.errors import MborobakCastError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def known_difference(row_id: str):
    return pytest.mark.xfail(strict=True, reason=f"{row_id}: known difference in the compatibility matrix")


def same(pair, build, **kw):
    assert_same(*pair.both("dates", build), **kw)


def both_fail(pair, build, error=MborobakCastError):
    with pytest.raises(error):
        build(__import__("mborobak.sql.functions", fromlist=["x"]), pair.mb("dates")).collect()
    with pytest.raises(Exception):  # noqa: B017
        build(__import__("pyspark.sql.functions", fromlist=["x"]), pair.sp("dates")).collect()


def test_DATE_01_parts(pair):
    same(pair, lambda F, df: df.select("id", F.year("d"), F.month("d"), F.dayofmonth("d"), F.day("ts"),
                                       F.dayofweek("d"), F.dayofyear("ts"), F.quarter("d")))
    same(pair, lambda F, df: df.select("id", F.hour("ts"), F.minute("ts"), F.second("ts"), F.hour("d"),
                                       F.year("s"), F.month("s")))


def test_DATE_02_iso_week(pair):
    same(pair, lambda F, df: df.select("id", F.weekofyear("d"), F.weekofyear("ts")))


def test_DATE_03_date_arithmetic(pair):
    same(pair, lambda F, df: df.select("id", F.date_add("d", 1), F.date_sub("d", 1), F.date_add("ts", F.col("n")),
                                       F.date_sub("d", F.col("n")), F.add_months("d", 1), F.add_months("d", 12),
                                       F.add_months("ts", F.col("n")), F.date_add("s", 2)))


def test_DATE_04_datediff(pair):
    same(pair, lambda F, df: df.select("id", F.datediff("ts", "d"), F.datediff(F.lit("2026-03-01"), "d"),
                                       F.datediff("d", F.date_add("ts", 400))))


def test_DATE_05_months_between(pair):
    same(pair, lambda F, df: df.select("id", F.months_between("ts", F.lit("2026-01-31")),
                                       F.months_between("d", F.lit("2026-02-15 06:00:00").cast("timestamp_ntz")),
                                       F.months_between("ts", "d", False), F.months_between("d", F.add_months("d", 1))))


def test_DATE_06_last_day(pair):
    same(pair, lambda F, df: df.select("id", F.last_day("d"), F.last_day("ts"), F.last_day("s")))


def test_DATE_07_trunc(pair):
    same(pair, lambda F, df: df.select("id", *[F.trunc("d", u).alias(u) for u in
                                               ("year", "yy", "quarter", "month", "MON", "mm", "week", "day", "bogus")]))


def test_DATE_08_date_trunc(pair):
    same(pair, lambda F, df: df.select("id", *[F.date_trunc(u, "ts").alias(u) for u in
                                               ("year", "quarter", "month", "week", "day", "hour", "minute", "second",
                                                "millisecond", "MONTH", "bogus")]), check_types=False)
    same(pair, lambda F, df: df.select("id", F.date_trunc("month", "d").alias("m")), check_types=False)


def test_DATE_09_date_format(pair):
    patterns = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS", "dd/MM/yy", "d/M/yyyy H:m:s", "MMM d, yyyy",
                "MMMM", "EEE", "EEEE", "hh:mm a", "D", "yyyyMMdd", "Q", "'Q'Q yyyy", "SSSSSS"]
    same(pair, lambda F, df: df.select("id", *[F.date_format("ts", p).alias(f"p{i}") for i, p in enumerate(patterns)]))
    same(pair, lambda F, df: df.select("id", F.date_format("d", "dd/MM/yyyy"), F.date_format("d", "HH:mm")))


def test_DATE_10_to_date(pair):
    same(pair, lambda F, df: df.select("id", F.to_date("s"), F.to_date("ts"), F.to_date("f", "dd/MM/yyyy HH:mm"),
                                       F.to_date(F.date_format("d", "yyyyMMdd"), "yyyyMMdd")))
    for bad in ("2026-02-30", "abc", "", "2026/01/05", "2026-01-05x"):
        both_fail(pair, lambda F, df, bad=bad: df.select(F.to_date(F.lit(bad))))
    for bad in ("2026-1-5", "31/02/2026", "2026-01-05 10:00"):
        both_fail(pair, lambda F, df, bad=bad: df.select(F.to_date(F.lit(bad), "yyyy-MM-dd")))


def test_DATE_11_to_timestamp(pair):
    same(pair, lambda F, df: df.select("id", F.to_timestamp("s"), F.to_timestamp("f", "dd/MM/yyyy HH:mm"),
                                       F.to_timestamp(F.date_format("ts", "MMM d, yyyy h:mm a"), "MMM d, yyyy h:mm a")),
         check_types=False)
    for bad in ("bad", "2026-01-05 25:00:00"):
        both_fail(pair, lambda F, df, bad=bad: df.select(F.to_timestamp(F.lit(bad))))
    both_fail(pair, lambda F, df: df.select(F.to_timestamp(F.lit("31/12/2026"), "dd/MM/yyyy HH:mm")))


def test_DATE_12_current_date_and_timestamp(pair):
    same(pair, lambda F, df: df.filter(F.col("id") == 1).select(
        (F.year(F.current_date()) >= 2026).alias("y"), F.current_timestamp().isNotNull().alias("t"),
        (F.datediff(F.current_date(), F.current_timestamp()) == 0).alias("same_day")))


def test_DATE_13_date_functions_in_filters_and_groups(pair):
    same(pair, lambda F, df: df.filter(F.year("d") >= 2026).select("id"))
    same(pair, lambda F, df: df.groupBy(F.trunc("d", "year").alias("y")).count())
    same(pair, lambda F, df: df.groupBy(F.date_format("d", "yyyy-MM").alias("m")).agg(F.max("ts")))


def test_CAST_14_text_to_date_and_timestamp_shapes(pair):
    same(pair, lambda F, df: df.select("id", F.col("s").cast("date"), F.col("s").cast("timestamp_ntz")))


def test_CAST_18_invalid_text_to_date_fails(pair):
    for bad in ("2026-02-30", "abc", "", "2026-13-01", "20260105"):
        both_fail(pair, lambda F, df, bad=bad: df.select(F.lit(bad).cast("date")))


@known_difference("TIME-05")
def test_TIME_05_to_timestamp_type(pair):
    same(pair, lambda F, df: df.select(F.to_timestamp("s"), F.date_trunc("day", "ts")))
