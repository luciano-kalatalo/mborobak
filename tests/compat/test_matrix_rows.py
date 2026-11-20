"""One test per compatibility-matrix row (docs/compatibility-matrix.md). Test names start with the row ID.

* ``assert_same``: Mborobak on IRIS and PySpark 4.3 (ANSI on) give the same names, types and rows.
* ``assert_both_fail``: both raise; Mborobak raises the error class the matrix names.
* ``known_difference``: rows marked "Known difference". They are strict xfails, so a test that starts
  passing fails the suite and the matrix row must be updated.
"""

from __future__ import annotations

import decimal
import math

import pytest

from mborobak.errors import AnalysisError, MborobakArithmeticError, MborobakCastError

from .conftest import assert_both_fail, assert_same

pytestmark = pytest.mark.compat
D = decimal.Decimal


def known_difference(row_id: str):
    return pytest.mark.xfail(strict=True, reason=f"{row_id}: known difference in the compatibility matrix")


# --------------------------------------------------------------------------- 2.1 strings


def test_STR_01_equality_is_case_sensitive(pair):
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s") == "APPLE").select("id")))


def test_STR_02_trailing_spaces_matter(pair):
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s") == "apple").select("id")))


def test_STR_03_like_isin_between(pair):
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s").like("app%")).select("id")))
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s").isin("apple", "Banana")).select("id")))
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s").between("B", "b")).select("id")))


def test_STR_04_string_join_keys(pair):
    def build(F, df):
        a, b = df.alias("a"), df.alias("b")
        return a.join(b, F.col("a.s") == F.col("b.s")).select(F.col("a.id").alias("x"), F.col("b.id").alias("y"))

    assert_same(*pair.both("strings", build))


def test_STR_05_group_by_string(pair):
    assert_same(*pair.both("strings", lambda F, df: df.groupBy("s").count()))


def test_STR_06_distinct_strings(pair):
    assert_same(*pair.both("strings", lambda F, df: df.select("s").distinct()))
    assert_same(*pair.both("strings", lambda F, df: df.select("s").union(df.select("s")).distinct()))


def test_STR_07_order_by_string(pair):
    assert_same(*pair.both("strings", lambda F, df: df.orderBy("s").select("s")), ordered=True)
    assert_same(*pair.both("strings", lambda F, df: df.orderBy(F.col("s").desc()).select("s")), ordered=True)


def test_STR_08_min_max_count_distinct(pair):
    assert_same(*pair.both("strings", lambda F, df: df.agg(F.min("s"), F.max("s"), F.countDistinct("s"))))


def test_STR_12_empty_string_is_not_null(pair):
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s") == "").select("id")))
    assert_same(*pair.both("strings", lambda F, df: df.filter(F.col("s").isNull()).select("id")))


# --------------------------------------------------------------------------- 2.2 NULL


def test_NULL_01_comparison_with_null_filters_out(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("i") > 0).select("id")))


def test_NULL_02_eq_null_safe(pair):
    assert_same(*pair.both("left", lambda F, df: df.select("id", F.col("k").eqNullSafe(None).alias("e"))))
    assert_same(*pair.both("left", lambda F, df: df.filter(F.col("k").eqNullSafe(2)).select("id")))


def test_NULL_03_default_null_order(pair):
    assert_same(*pair.both("nums", lambda F, df: df.orderBy("i", "id").select("id")), ordered=True)
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("i").desc(), "id").select("id")), ordered=True)


def test_NULL_04_explicit_null_order(pair):
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("i").asc_nulls_last(), "id").select("id")),
                ordered=True)
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("i").desc_nulls_first(), "id").select("id")),
                ordered=True)


def test_NULL_05_null_join_keys_never_match(pair):
    left, right = pair.mb("left"), pair.mb("right")
    mb = left.join(right, left.k == right.k).select(left.id, right.id.alias("rid"))
    sl, sr = pair.sp("left"), pair.sp("right")
    sp = sl.join(sr, sl.k == sr.k).select(sl.id, sr.id.alias("rid"))
    assert_same(mb, sp)


def test_NULL_06_group_by_nullable_key(pair):
    assert_same(*pair.both("left", lambda F, df: df.groupBy("k").count()))


def test_NULL_07_aggregates_on_empty_input(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") > 100).agg(
        F.count("*"), F.sum("i"), F.max("d"), F.avg("n"))))
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") > 100).groupBy("i").count()))


def test_NULL_08_is_null_in_select(pair):
    assert_same(*pair.both("nums", lambda F, df: df.select("id", F.col("i").isNull(), F.col("i").isNotNull())))


def test_NULL_09_null_divided_by_zero(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 8).select(F.col("i") / 0)))


# --------------------------------------------------------------------------- 2.3 numbers


def test_NUM_01_integer_division_is_double(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("i") != 0)
                           .select("id", F.col("i") / 2, F.col("l") / F.col("i"))))


def test_NUM_02_avg_of_integers(pair):
    assert_same(*pair.both("nums", lambda F, df: df.agg(F.avg("i"), F.avg("l"))))


def test_NUM_03_avg_and_sum_of_decimals(pair):
    assert_same(*pair.both("nums", lambda F, df: df.agg(F.sum("n"), F.avg("n"), F.sum("n4"), F.avg("n4"))))


@known_difference("NUM-04")
def test_NUM_04_integer_overflow(pair):
    assert_both_fail(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 6).select(F.col("i") + 1)),
                     MborobakArithmeticError)


@known_difference("NUM-05")
def test_NUM_05_long_sum_overflow(pair):
    assert_both_fail(*pair.both("nums", lambda F, df: df.agg(F.sum("l"))), MborobakArithmeticError)


@known_difference("NUM-06")
def test_NUM_06_decimal_precision_overflow(pair):
    assert_both_fail(*pair.both("nums", lambda F, df: df.select(F.col("n").cast("decimal(5,2)"))),
                     MborobakArithmeticError)


def test_NUM_07_division_by_zero_fails(pair):
    assert_both_fail(*pair.both("nums", lambda F, df: df.select(F.col("i") / (F.col("i") - F.col("i")))),
                     MborobakArithmeticError)


def test_NUM_08_modulo_sign(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") <= 5).select(
        "id", F.col("i") % 3, F.col("l") % -3)))


def test_NUM_09_double_sum_within_tolerance(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 7, 10)).agg(F.sum("d"))))


def test_NUM_11_count_is_long(pair):
    assert_same(*pair.both("nums", lambda F, df: df.groupBy("i").agg(F.count("d"), F.count("*"))))


# --------------------------------------------------------------------------- 2.4 casts


def test_CAST_01_to_05_valid_integer_text(pair):
    assert_same(*pair.both("texts", lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 4, 5, 6, 18))
                           .select("id", F.col("s").cast("int"))))


@pytest.mark.parametrize("row_id, text_id", [("CAST-01", 10), ("CAST-03", 7), ("CAST-03", 8), ("CAST-05", 9),
                                             ("CAST-03", 11), ("CAST-03", 12)])
def test_CAST_01_03_05_invalid_integer_text_fails(pair, row_id, text_id):
    assert_both_fail(*pair.both("texts", lambda F, df: df.filter(F.col("id") == text_id)
                                .select(F.col("s").cast("int"))), MborobakCastError)


def test_CAST_06_full_width_digits(pair):
    assert_both_fail(*pair.both("texts", lambda F, df: df.filter(F.col("id") == 17)
                                .select(F.col("s").cast("int"))), MborobakCastError)


def test_CAST_01_valid_double_text(pair):
    assert_same(*pair.both("texts", lambda F, df: df.filter(F.col("id").isin(1, 2, 5, 7, 8, 12, 15, 16))
                           .select("id", F.col("s").cast("double"))))


def test_CAST_07_nan_and_infinity_text(pair):
    assert_same(*pair.both("texts", lambda F, df: df.filter(F.col("id").isin(13, 14, 20, 21, 22, 23))
                           .select("id", F.col("s").cast("double"))))


def test_CAST_07_constant_casts(pair):
    assert_same(*pair.both("texts", lambda F, df: df.filter(F.col("id") == 1).select(
        F.lit(" 7 ").cast("int"), F.lit("NaN").cast("double"), F.lit("-Infinity").cast("double"),
        F.lit("1.555").cast("decimal(10,2)"), F.lit(-2.9).cast("int"))))
    assert_both_fail(*pair.both("texts", lambda F, df: df.select(F.lit("1.5").cast("int"))), MborobakCastError)


@known_difference("CAST-08")
def test_CAST_08_long_overflow_text(pair):
    assert_both_fail(*pair.both("texts", lambda F, df: df.filter(F.col("id") == 19)
                                .select(F.col("s").cast("bigint"))), MborobakArithmeticError)


def test_CAST_09_truncation_toward_zero(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 10))
                           .select("id", F.col("d").cast("int"), F.col("n").cast("int"), F.col("n4").cast("bigint"))))


def test_CAST_10_double_to_int_out_of_range(pair):
    assert_both_fail(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 8).select(F.col("d").cast("int"))),
                     MborobakArithmeticError)


def test_CAST_11_int_date_timestamp_to_string(pair):
    assert_same(*pair.both("times", lambda F, df: df.select(
        F.col("id").cast("string"), F.col("dt").cast("string"), F.col("ts").cast("string"))))


def test_CAST_15_infinity_and_nan_to_int_fail(pair):
    for row in (4, 5, 6):  # NaN, +inf, -inf
        assert_both_fail(*pair.both("nums", lambda F, df: df.filter(F.col("id") == row)
                                    .select(F.col("d").cast("int"))), MborobakArithmeticError)


@known_difference("CAST-12")
def test_CAST_12_double_to_string(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(3, 10)).select(F.col("d").cast("string"))))


@known_difference("CAST-13")
def test_CAST_13_decimal_to_string(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 1).select(F.col("n4").cast("string"))))


def test_CAST_14_text_to_date_and_timestamp(pair):
    assert_same(*pair.both("times", lambda F, df: df.select(
        F.lit("2026-01-02").cast("date").alias("d"), F.lit("2026-01-02 03:04:05.5").cast("timestamp_ntz")
        .alias("t"))))


# --------------------------------------------------------------------------- 2.5 floating point


def test_FP_01_negative_zero_equals_zero(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("d") == 0).select("id")))
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 2)).groupBy("d").count()))


def test_FP_02_negative_zero_value(pair):
    mb, sp = pair.both("nums", lambda F, df: df.filter(F.col("id") == 2).select("d"))
    assert math.copysign(1, mb.collect()[0][0]) == math.copysign(1, sp.collect()[0][0])


def test_FP_03_nan_groups_and_joins(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 4, 5)).union(
        df.filter(F.col("id") == 4)).groupBy("d").count()))

    def self_join(F, df):
        a, b = df.alias("a"), df.alias("b")
        return a.join(b, F.col("a.d") == F.col("b.d")).select(F.col("a.id").alias("x"), F.col("b.id").alias("y"))

    assert_same(*pair.both("nums", self_join))


def test_FP_04_05_09_double_sort_order(pair):
    # Ties (0.0 and -0.0 compare equal in both) are broken by id.
    assert_same(*pair.both("nums", lambda F, df: df.orderBy("d", "id").select("id", "d")), ordered=True)
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("d").desc(), "id").select("id", "d")),
                ordered=True)


@known_difference("FP-06")
def test_FP_06_max_with_nan(pair):
    assert_same(*pair.both("nums", lambda F, df: df.agg(F.max("d"))))


def test_FP_07_min_with_nan(pair):
    assert_same(*pair.both("nums", lambda F, df: df.agg(F.min("d"))))


def test_FP_08_nan_and_infinity_literals(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 1).select(
        F.lit(float("nan")).alias("a"), F.lit(float("inf")).alias("b"), F.lit(float("-inf")).alias("c"))))


def test_FP_13_comparisons_with_nan(pair):
    # NaN is above every double, +inf included, and NaN = NaN; IRIS's own answers vary (compiler spec §7.14)
    def pairs(F, df):
        a, b = df.select("id", "d").alias("a"), df.select("id", "d").alias("b")
        x, y = F.col("a.d"), F.col("b.d")
        return a.crossJoin(b).select(F.col("a.id").alias("i"), F.col("b.id").alias("j"),
                                     (x == y).alias("eq"), (x != y).alias("ne"), (x < y).alias("lt"),
                                     (x <= y).alias("le"), (x > y).alias("gt"), (x >= y).alias("ge"),
                                     x.eqNullSafe(y).alias("ns"))

    assert_same(*pair.both("nums", pairs))
    nan = float("nan")
    for cond in (lambda F: F.col("d") > 0, lambda F: F.col("d") >= 1e300, lambda F: F.col("d") <= 2.5,
                 lambda F: F.col("d") < float("inf"), lambda F: F.col("d") == nan, lambda F: F.col("d") != nan,
                 lambda F: F.col("d") > nan, lambda F: F.lit(0.0) < F.col("d"), lambda F: ~(F.col("d") <= 0),
                 lambda F: F.col("d").isin(nan, 2.5), lambda F: F.col("d").isin(1.0, None),
                 lambda F: F.col("d").between(0, nan), lambda F: F.col("i").isin(nan),
                 lambda F: F.col("d") > F.col("i")):
        assert_same(*pair.both("nums", lambda F, df: df.filter(cond(F)).select("id")))
        assert_same(*pair.both("nums", lambda F, df: df.select("id", cond(F).alias("c"))))
    assert_same(*pair.both("nums", lambda F, df: df.filter("d > 0 AND d BETWEEN -1 AND CAST('NaN' AS DOUBLE)").select("id")))


@known_difference("FP-10")
def test_FP_10_minus_1e19_groups_with_null(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(7, 9)).groupBy("d").count()))


# --------------------------------------------------------------------------- 2.6 ordering and limits


def test_ORD_01_05_order_limit_then_filter(pair):
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("n").desc()).limit(4)
                           .filter(F.col("id") > 1).select("id", "n")), ordered=True)


def test_ORD_03_limit_without_order_counts(pair):
    mb, sp = pair.both("nums", lambda F, df: df.limit(3))
    assert mb.count() == sp.count() == 3


# --------------------------------------------------------------------------- 2.7 grouping, joins, union


def test_GRP_02_filter_after_agg(pair):
    assert_same(*pair.both("left", lambda F, df: df.groupBy("k").agg(F.count("*").alias("c"))
                           .filter(F.col("c") > 1)))


@pytest.mark.parametrize("how", ["inner", "left", "right", "full", "cross"])
def test_JOIN_01_join_types(pair, how):
    left, right = pair.mb("left"), pair.mb("right")
    sl, sr = pair.sp("left"), pair.sp("right")
    if how == "cross":
        mb = left.crossJoin(right).select(left.id, right.id.alias("rid"))
        sp = sl.crossJoin(sr).select(sl.id, sr.id.alias("rid"))
    else:
        mb = left.join(right, left.k == right.k, how).select(left.id, right.id.alias("rid"))
        sp = sl.join(sr, sl.k == sr.k, how).select(sl.id, sr.id.alias("rid"))
    assert_same(mb, sp)


@pytest.mark.parametrize("how", ["inner", "left", "right", "full"])
def test_JOIN_02_03_join_on_name(pair, how):
    mb = pair.mb("left").join(pair.mb("right").withColumnRenamed("id", "rid"), "k", how)
    sp = pair.sp("left").join(pair.sp("right").withColumnRenamed("id", "rid"), "k", how)
    assert_same(mb, sp)


def test_JOIN_04_duplicate_names_after_expression_join(pair):
    left, right = pair.mb("left"), pair.mb("right")
    sl, sr = pair.sp("left"), pair.sp("right")
    assert_same(left.join(right, left.k == right.k), sl.join(sr, sl.k == sr.k))


def test_UNI_01_union_all_by_position(pair):
    left, right = pair.mb("left"), pair.mb("right")
    sl, sr = pair.sp("left"), pair.sp("right")
    assert_same(left.select("k", "name").union(right.select("k", "label")),
                sl.select("k", "name").union(sr.select("k", "label")))


def test_UNI_02_union_widens_numbers(pair):
    assert_same(*pair.both("nums", lambda F, df: df.select("i").union(df.select("n"))))
    assert_same(*pair.both("nums", lambda F, df: df.select("i").union(df.select("l"))))
    assert_same(*pair.both("nums", lambda F, df: df.select("n").union(df.select("d"))))


def test_UNI_03_union_of_int_and_string(pair):
    with pytest.raises(AnalysisError):
        pair.mb("left").select("k").union(pair.mb("left").select("name"))
    # PySpark (ANSI) widens to bigint and casts the strings at run time: 'a' fails, '7' would work.
    sl = pair.sp("left")
    sp = sl.select("k").union(sl.select("name"))
    assert sp.dtypes == [("k", "bigint")]
    with pytest.raises(Exception):  # noqa: B017
        sp.collect()


# --------------------------------------------------------------------------- 2.8 time


def test_TIME_03_04_dates_and_microseconds(pair):
    assert_same(*pair.both("times", lambda F, df: df.select("id", "dt", "ts")))


# --------------------------------------------------------------------------- 2.9 names


def test_NAME_07_filter_and_sort_on_a_column_dropped_by_select(pair):
    assert_same(*pair.both("nums", lambda F, df: df.select("id").filter(F.col("i") > 1)))
    assert_same(*pair.both("nums", lambda F, df: df.select("id").orderBy(F.col("n").desc(), "id")), ordered=True)
    assert_same(*pair.both("nums", lambda F, df: df.select(F.col("i").alias("x"), "id")
                           .filter((F.col("x") > 0) & (F.col("l") > 0))))


def test_NAME_01_case_insensitive_names(pair):
    assert_same(*pair.both("nums", lambda F, df: df.select(F.col("ID"), F.col("i")).filter(F.col("I") > 1)))


def test_NAME_03_default_names(pair):
    assert_same(*pair.both("nums", lambda F, df: df.groupBy("i").agg(
        F.sum("n"), F.count("*"), F.countDistinct("d"), F.avg("l"))))
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") <= 5).select(
        F.col("i") + F.col("l"), F.lit(1), F.col("i") * 2)))


def test_NAME_04_cast_and_when_names(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 10)).select(
        F.col("d").cast("int"), F.when(F.col("i") > 1, "hi"), F.when(F.col("i") > 1, "hi").otherwise("lo"))))


# --------------------------------------------------------------------------- 2.10 results


def test_RES_02_to_pandas_dtypes(pair):
    mb, sp = pair.both("nums", lambda F, df: df.select("id", "i", "l", "d", "n"))
    assert dict(mb.toPandas().dtypes.astype(str)) == dict(sp.toPandas().dtypes.astype(str))


def test_RES_04_show_format(pair, capsys):
    for name, cols in (("strings", ("id", "s")), ("nums", ("id", "d", "n"))):  # doubles print as in Java
        mb, sp = pair.both(name, lambda F, df: df.orderBy("id").select(*cols))
        mb.show()
        mb_out = capsys.readouterr().out
        sp.show()
        sp_out = capsys.readouterr().out
        assert mb_out == sp_out


# --------------------------------------------------------------------------- rows added after the code review


def test_NUM_12_decimal_divided_by_integer(pair):
    # Rows with non-zero divisors only (a zero divisor fails in both, NUM-07).
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(3, 7, 10)).select(
        "id", F.col("n") / 2, F.col("n") / F.col("i"), F.col("n") / F.col("d"))))


@known_difference("NUM-13")
def test_NUM_13_decimal_division_beyond_iris_precision(pair):
    # decimal(12,4) / bigint is decimal(38,25) in Spark; IRIS computes about 19 significant digits.
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id") == 10).select(F.col("n4") / F.col("l"))))


def test_CAST_16_boolean_to_string(pair):
    assert_same(*pair.both("nums", lambda F, df: df.select(
        "id", (F.col("i") > 1).cast("string"), F.col("i").isNull().cast("string"))))


def test_CAST_17_nested_casts(pair):
    assert_same(*pair.both("nums", lambda F, df: df.filter(F.col("id").isin(1, 2, 3, 10)).select(
        "id", (F.col("d") * 2).cast("string").cast("double").cast("int").alias("x"))))


def test_UNI_04_union_with_a_repeated_column(pair):
    def build(F, df):
        return df.select(F.col("k"), F.col("k").alias("k2")).union(df.select("id", "k"))

    assert_same(*pair.both("left", build))


def test_ORD_06_order_kept_when_select_drops_the_sort_key(pair):
    assert_same(*pair.both("nums", lambda F, df: df.orderBy(F.col("n").desc(), "id").select("id").limit(5)
                           .filter(F.col("id") > 1)), ordered=True)


def test_NAME_08_agg_dict_names(pair):
    mb, sp = pair.both("nums", lambda F, df: df.groupBy("i").agg({"n": "mean", "d": "count", "*": "count"}))
    assert sorted(mb.columns) == sorted(sp.columns)  # Spark's dict order is not the insertion order
    assert_same(mb.select(*sorted(mb.columns)), sp.select(*sorted(sp.columns)))


def test_GRP_03_min_max_without_columns(pair):
    assert_same(*pair.both("nums", lambda F, df: df.groupBy("i").min()))
    assert_same(*pair.both("nums", lambda F, df: df.groupBy("i").avg()))
