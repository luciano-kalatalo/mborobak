"""Differential tests for the 0.2 scalar functions and fillna/dropna (compatibility matrix §2.12, rows FUN-xx).

Same conventions as test_matrix_rows.py: one test per matrix row, names starting with the row ID.
"""

from __future__ import annotations

import pytest

from mborobak.errors import MborobakArithmeticError, MborobakError

from .conftest import assert_both_fail, assert_same

pytestmark = pytest.mark.compat


def known_difference(row_id: str):
    return pytest.mark.xfail(strict=True, reason=f"{row_id}: known difference in the compatibility matrix")


def same(pair, build, dataset="funcs", **kw):
    assert_same(*pair.both(dataset, build), **kw)


# --------------------------------------------------------------------------- null functions


def test_FUN_01_coalesce_ifnull_nvl_nvl2(pair):
    same(pair, lambda F, df: df.select("id", F.coalesce("s", "t"), F.coalesce("i", "n"), F.coalesce("d", "i"),
                                       F.coalesce("i", "l", F.lit(0))))
    same(pair, lambda F, df: df.select("id", F.ifnull("t", "s"), F.nvl("i", F.lit(-1)),
                                       F.nvl2("t", "s", F.lit("none"))))


def test_FUN_02_nullif_is_case_sensitive(pair):
    same(pair, lambda F, df: df.select("id", F.nullif("s", "t"), F.nullif(F.lower("s"), "t"), F.nullif("i", "l")))


def test_FUN_03_greatest_least_skip_nulls(pair):
    same(pair, lambda F, df: df.select("id", F.greatest("i", "l"), F.least("i", "l"), F.greatest("s", "t"),
                                       F.least("s", "t"), F.greatest("d", "i"), F.least("n", "i", F.lit(1))))


def test_FUN_04_isnan_isnull_isnotnull(pair):
    same(pair, lambda F, df: df.select("id", F.isnan("d"), F.isnan("i"), F.isnull("s"), F.isnotnull("t")))
    same(pair, lambda F, df: df.filter(F.isnan("d") | F.isnull("d")).select("id"))
    same(pair, lambda F, df: df.filter(~F.isnan("d")).select("id"))


# --------------------------------------------------------------------------- strings


def test_FUN_05_case_trim_reverse_repeat(pair):
    same(pair, lambda F, df: df.select("id", F.upper("s"), F.lower("s"), F.trim("s"), F.ltrim("s"),
                                       F.rtrim("s"), F.reverse("s"), F.repeat("t", 2), F.repeat("t", 0)))


def test_FUN_05_initcap(pair):
    same(pair, lambda F, df: df.filter(F.col("id") != 10).select("id", F.initcap("s"), F.initcap("t")))


@known_difference("FUN-22")
def test_FUN_22_initcap_after_tab(pair):
    same(pair, lambda F, df: df.filter(F.col("id") == 10).select(F.initcap("s")))


def test_FUN_06_length_counts_trailing_spaces(pair):
    same(pair, lambda F, df: df.select("id", F.length("s"), F.char_length("t"), F.character_length("s")))


def test_FUN_07_substring_positions(pair):
    same(pair, lambda F, df: df.select("id", F.substring("s", 1, 3), F.substring("s", 0, 2),
                                       F.substring("s", -3, 2), F.substring("s", -20, 22), F.substring("s", 3, 100),
                                       F.substring("s", 2, 0), F.col("s").substr(2, 3), F.substr("s", F.lit(4))))


def test_FUN_08_concat_and_concat_ws(pair):
    same(pair, lambda F, df: df.select("id", F.concat("s", "t"), F.concat("s", F.lit("-"), "i", "l"),
                                       F.concat_ws("-", "s", "t"), F.concat_ws("", "t", "s"),
                                       F.concat_ws(", ", "t", "i")))


def test_FUN_09_lpad_rpad(pair):
    same(pair, lambda F, df: df.select("id", F.lpad("s", 6, "*"), F.rpad("s", 3, "xy"), F.lpad("t", 4, "ab"),
                                       F.rpad("t", 0, "x"), F.lpad("s", 4, "")))


def test_FUN_10_instr_locate(pair):
    same(pair, lambda F, df: df.select("id", F.instr("s", "l"), F.instr("s", "L"), F.instr("s", ""),
                                       F.locate("o", "s"), F.locate("o", "s", 6), F.locate("", "s", 3),
                                       F.locate("o", "s", 0)))


def test_FUN_11_replace(pair):
    same(pair, lambda F, df: df.select("id", F.replace("s", F.lit("l"), F.lit("L")), F.replace("s", F.lit("o")),
                                       F.replace("s", F.lit(""), F.lit("x")), F.replace("s", "t", F.lit("#"))))


# --------------------------------------------------------------------------- math


def test_FUN_12_round_decimals_and_integers(pair):
    same(pair, lambda F, df: df.select("id", F.round("n", 2), F.round("n"), F.round("n", -1), F.round("n", 5),
                                       F.round("i", -1), F.round("l", 2), F.round("i")))


def test_FUN_13_round_doubles(pair):
    same(pair, lambda F, df: df.select("id", F.round("d", 2), F.round("d"), F.round("d", 1), F.round("d", -2)))


def test_FUN_14_bround_half_even(pair):
    same(pair, lambda F, df: df.select("id", F.bround("n", 2), F.bround("n"), F.bround("n", 1), F.bround("i", -1),
                                       F.bround("d", 2), F.bround("d")))


def test_FUN_15_ceil_floor(pair):
    same(pair, lambda F, df: df.select("id", F.ceil("n"), F.floor("n"), F.ceiling("i"), F.floor("l")))
    same(pair, lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 4, 7, 8, 10, 11, 12))
         .select("id", F.ceil("d"), F.floor("d"), F.ceiling("d")))
    for value_id in (5, 6, 9):  # infinity, -infinity, 1e300: overflow in both
        assert_both_fail(*pair.both("funcs", lambda F, df: df.filter(F.col("id") == value_id).select(F.ceil("d"))),
                         MborobakArithmeticError)


def test_FUN_16_pow_sqrt_exp_log_domain(pair):
    same(pair, lambda F, df: df.select("id", F.pow("i", 2), F.pow("i", F.lit(-1)), F.pow("n", 0.5),
                                       F.power(F.lit(2.0), "i"), F.sqrt("d"), F.sqrt("i"), F.exp("d"), F.exp("i")))
    same(pair, lambda F, df: df.select("id", F.log("d"), F.ln("i"), F.log10("d"), F.log2("l"), F.log1p("d"),
                                       F.log(2.0, "l"), F.log(10.0, "d")))


def test_FUN_17_abs_signum(pair):
    same(pair, lambda F, df: df.select("id", F.abs("i"), F.abs("n"), F.abs("d"), F.signum("i"), F.sign("d"),
                                       F.signum("n")))


def test_FUN_18_pmod(pair):
    same(pair, lambda F, df: df.select("id", F.pmod("i", F.lit(3)), F.pmod("l", -3), F.pmod("n", 2)))
    assert_both_fail(*pair.both("funcs", lambda F, df: df.select(F.pmod("i", F.lit(0)))), MborobakArithmeticError)


# --------------------------------------------------------------------------- functions in queries


def test_FUN_19_functions_in_filters_groups_and_sorts(pair):
    same(pair, lambda F, df: df.filter(F.upper("s") == "HELLO WORLD").select("id"))
    same(pair, lambda F, df: df.filter(F.lower("t") == "abc").select("id"))
    same(pair, lambda F, df: df.groupBy(F.lower("t").alias("k")).count())
    same(pair, lambda F, df: df.orderBy(F.length("s").desc_nulls_last(), "id").select("id", F.length("s")),
         ordered=True)
    # id 6 is left out: its group would add +inf and -inf, which IRIS rejects (FP-12)
    same(pair, lambda F, df: df.filter(F.col("id") != 6).groupBy("i")
         .agg(F.round(F.avg("d"), 2).alias("a"), F.max(F.upper("s")), F.min("t")))


# --------------------------------------------------------------------------- missing values


def test_FUN_20_fillna(pair):
    same(pair, lambda F, df: df.fillna(0))
    same(pair, lambda F, df: df.fillna(1.7, subset=["i", "n"]))
    same(pair, lambda F, df: df.na.fill("?"))
    same(pair, lambda F, df: df.fillna({"t": "none", "d": -1.0, "l": 9}))


def test_FUN_21_dropna(pair):
    same(pair, lambda F, df: df.dropna())
    same(pair, lambda F, df: df.dropna(subset=["d"]))
    same(pair, lambda F, df: df.na.drop(how="all", subset=["t", "i"]))
    same(pair, lambda F, df: df.dropna(thresh=6))


@known_difference("FP-12")
def test_FP_12_sum_of_both_infinities(pair):
    same(pair, lambda F, df: df.filter(F.col("id").isin(5, 6)).agg(F.sum("d")))


# --------------------------------------------------------------------------- strings (STR-13, STR-16)


def test_STR_16_numeric_looking_strings_compare_as_text(pair):
    same(pair, lambda F, df: df.filter(F.col("s") > "5").select("id"))
    same(pair, lambda F, df: df.filter(F.col("s").between("1", "2")).select("id"))
    same(pair, lambda F, df: df.select("id", (F.col("s") < F.col("t")).alias("lt")))
    same(pair, lambda F, df: df.orderBy("s", "id").select("s"), ordered=True)
    same(pair, lambda F, df: df.orderBy(F.col("t").desc(), "id").select("t"), ordered=True)
    same(pair, lambda F, df: df.agg(F.min("s"), F.max("s"), F.min("t"), F.max("t")))
    same(pair, lambda F, df: df.groupBy(F.length("t")).agg(F.max("s")))


def test_STR_13_null_in_concatenation(pair):
    same(pair, lambda F, df: df.select("id", F.concat("s", "t"), F.concat(F.lit("a"), "t", F.lit("b"))))


def test_FP_11_functions_returning_nan_or_infinity(pair):
    same(pair, lambda F, df: df.select("id", F.sqrt(F.col("i") - 1000), F.log(F.col("i") * 0),
                                       F.pow(F.col("i") * 0, F.lit(-1.0)), F.exp(F.col("d") * 1000)))


@known_difference("FUN-24")
def test_FUN_24_pow_overflow(pair):
    same(pair, lambda F, df: df.filter(F.col("id") == 1).select(F.pow(F.lit(10.0), F.lit(400.0))))


@known_difference("FUN-25")
def test_FUN_25_integer_overflow_in_abs_and_round(pair):
    assert_both_fail(*pair.both("funcs", lambda F, df: df.filter(F.col("id") == 1)
                                .select(F.abs(F.lit(-2147483648)))), MborobakError)
