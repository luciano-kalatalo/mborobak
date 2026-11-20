"""Differential tests for regular expressions (compatibility matrix §2.18, rows REGEX-xx).

IRIS runs them through ICU-regex SQL functions Mborobak creates on first use (compiler spec §7.16).
"""

from __future__ import annotations

import pytest

from mborobak.errors import MborobakExecutionError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def known_difference(row_id: str):
    return pytest.mark.xfail(strict=True, reason=f"{row_id}: known difference in the compatibility matrix")


def same(pair, build, **kw):
    assert_same(*pair.both("regex", build), **kw)


def test_REGEX_01_rlike(pair):
    same(pair, lambda F, df: df.select("id", F.col("s").rlike("b"), F.col("s").rlike("^a.c$"), F.col("s").rlike("(?i)abc"),
                                       F.col("s").rlike(""), F.rlike("s", "p"), F.regexp("s", F.lit("[0-9]{2}")),
                                       F.regexp_like("s", F.lit(r"\d"))))
    same(pair, lambda F, df: df.filter(F.col("s").rlike("^[a-z]")).select("id"))
    same(pair, lambda F, df: df.filter(~F.col("s").rlike("^[a-z]")).select("id"))  # NULL rows stay out


def test_REGEX_02_regexp_replace(pair):
    same(pair, lambda F, df: df.select("id", F.regexp_replace("s", "[0-9]+", "#"),
                                       F.regexp_replace("s", r"(\d+)-(\d+)-(\d+)", "$3/$2/$1"),
                                       F.regexp_replace("s", "x*", "-"), F.regexp_replace("s", "b", r"\$1"),
                                       F.regexp_replace("s", F.col("p"), F.lit("_")),
                                       F.regexp_replace("s", "(?i)[aeiou]", "")))


def test_REGEX_03_regexp_extract(pair):
    same(pair, lambda F, df: df.select("id", F.regexp_extract("s", r"([a-z]+)(\d+)", 1),
                                       F.regexp_extract("s", r"([a-z]+)(\d+)", 2), F.regexp_extract("s", r"([a-z]+)(\d+)", 0),
                                       F.regexp_extract("s", r"(x)?b", 1), F.regexp_extract("s", "zzz", 0)))


def test_REGEX_04_count_substr_instr(pair):
    same(pair, lambda F, df: df.select("id", F.regexp_count("s", F.lit("[0-9]")), F.regexp_substr("s", F.lit("[0-9]+")),
                                       F.regexp_instr("s", F.lit("[0-9]+")), F.regexp_count("s", F.lit("x*"))))


def test_REGEX_05_sql_text(pair):
    same(pair, lambda F, df: df.selectExpr("id", "s RLIKE 'b'", "s REGEXP '^A'", "s NOT RLIKE 'b'",
                                           "regexp_replace(s, '[a-z]', '*')", "regexp_extract(s, '([0-9]+)')",
                                           "regexp_count(s, 'o')"))
    same(pair, lambda F, df: df.filter("s RLIKE '[0-9]' AND NOT s RLIKE '-'").select("id"))


@pytest.mark.parametrize("build", [
    lambda F, df: df.select(F.col("s").rlike("(")),
    lambda F, df: df.select(F.regexp_extract("s", "(b)", 2)),
])
def test_REGEX_06_errors(pair, build):
    with pytest.raises(MborobakExecutionError):
        build(__import__("mborobak.sql.functions", fromlist=["x"]), pair.mb("regex")).collect()
    with pytest.raises(Exception):  # noqa: B017
        build(__import__("pyspark.sql.functions", fromlist=["x"]), pair.sp("regex")).collect()


@known_difference("REGEX-07")
def test_REGEX_07_unicode_classes(pair):
    from pyspark.sql import functions as SF

    from mborobak.sql import functions as MF

    data = [("é1",)]
    mb = pair.mb_session.createDataFrame(data, "s string")
    sp = pair.spark.createDataFrame(data, "s string")
    assert_same(mb.select(MF.regexp_replace("s", r"\w", "_")), sp.select(SF.regexp_replace("s", r"\w", "_")))
