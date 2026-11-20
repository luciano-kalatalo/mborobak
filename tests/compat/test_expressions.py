"""Differential tests for Spark SQL expression strings (compatibility matrix §2.15, rows EXPR-xx).

The same strings go to selectExpr, filter/where and F.expr in both engines.
"""

from __future__ import annotations

import pytest

from mborobak.errors import AnalysisError, UnsupportedOperationError

from .conftest import assert_same

pytestmark = pytest.mark.compat


def sel(pair, dataset, *exprs, **kw):
    assert_same(*pair.both(dataset, lambda F, df: df.selectExpr(*exprs)), **kw)


def where(pair, dataset, condition, *cols):
    assert_same(*pair.both(dataset, lambda F, df: df.filter(condition).select(*cols)))
    assert_same(*pair.both(dataset, lambda F, df: df.where(condition).select(*cols)))


def test_EXPR_01_literals_and_their_types(pair):
    sel(pair, "funcs", "id", "1", "10000000000", "1L", "1.50", ".5", "1BD", "1e3", "2.0D", "-1", "'it\\'s'", '"dq"',
        "'a' 'b'", "true", "DATE '2026-01-01'")


def test_EXPR_02_operators_and_precedence(pair):
    sel(pair, "funcs", "id", "i + 1", "i + 1.5", "i * 2.0D", "1 + 2 * 3", "(1 + 2) * 3", "1 - 2 - 3", "i / 2", "i % 3",
        "-i", "+i", "n * 2 + l")
    sel(pair, "funcs", "id", "i = 1", "i == 1", "i != 1", "i <> 1", "i <=> NULL", "NOT i > 1",
        "i > 1 AND l < 3 OR s = 'abc'", "i > 1 = true", "s < t", "s >= '5'")


def test_EXPR_03_predicates(pair):
    sel(pair, "funcs", "id", "i IS NULL", "s IS NOT NULL", "i IN (1, 2, 3)", "i NOT IN (0, 3)", "s LIKE 'a%'",
        "s NOT LIKE '%o%'", "i BETWEEN 0 AND 5", "i NOT BETWEEN 0 AND 5", "s BETWEEN '1' AND '9'")


def test_EXPR_04_case_cast_if_concat(pair):
    sel(pair, "funcs", "id", "CASE WHEN i > 3 THEN 'big' WHEN i > 0 THEN 'small' ELSE 'none' END",
        "CASE i WHEN 3 THEN 'three' WHEN 7 THEN 'seven' END", "CAST(i AS STRING)", "CAST(n AS INT)",
        "cast(i as bigint)", "cast(l as double)", "CAST(i * 2 AS DECIMAL(12, 2))", "if(i > 3, 'a', 'b')", "s || t",
        "s || '-' || t")


def test_EXPR_05_functions_and_aggregates(pair):
    sel(pair, "funcs", "id", "upper(s)", "LOWER(s)", "coalesce(s, t, 'none')", "nvl(i, 0)", "length(s)",
        "substring(s, 2, 3)", "substr(s, 2)", "concat_ws('-', s, t)", "round(d, 1)", "pow(i, 2)", "power(i, 2)",
        "ceil(n)", "log(2, l)", "abs(i)", "greatest(i, l)", "instr(s, 'l')", "trim(s)", "isnull(t)")
    assert_same(*pair.both("funcs", lambda F, df: df.groupBy("i").agg(
        F.expr("count(*)"), F.expr("count(DISTINCT s)"), F.expr("sum(l)"), F.expr("avg(n) AS a"),
        F.expr("max(upper(t))"))))
    sel(pair, "dates", "id", "year(d)", "date_add(d, 1)", "datediff(ts, d)", "date_format(ts, 'yyyy-MM')",
        "to_date(f, 'dd/MM/yyyy HH:mm')", "last_day(d)", "trunc(d, 'month')")


def test_EXPR_06_windows_in_text(pair):
    sel(pair, "wins", "id", "row_number() OVER (PARTITION BY g ORDER BY k, id)",
        "rank() OVER (ORDER BY x DESC NULLS LAST)", "sum(x) OVER (PARTITION BY g)",
        "sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
        "avg(n) OVER (PARTITION BY g ORDER BY k)", "lag(x, 1) OVER (ORDER BY id)", "lead(x, 2, 0) OVER (ORDER BY id)",
        "count(*) OVER (PARTITION BY g) AS c")


def test_EXPR_07_filters_from_text(pair):
    where(pair, "funcs", "i > 1", "id")
    where(pair, "funcs", "s = 'Hello World' OR t LIKE 'a%'", "id")
    where(pair, "funcs", "i IS NOT NULL AND (l < 3 OR n > 2.5)", "id")
    where(pair, "funcs", "s IN ('abc', 'ab', '') AND NOT i = 0", "id")
    where(pair, "funcs", "upper(s) = 'HELLO WORLD'", "id")
    where(pair, "dates", "d >= DATE '2026-01-01' AND year(ts) < 2027", "id")
    assert_same(*pair.both("wins", lambda F, df: df.withColumn("rn", F.expr("row_number() OVER (PARTITION BY g "
                                                                              "ORDER BY id)")).filter("rn = 1").select("id")))


def test_EXPR_08_aliases_and_star(pair):
    sel(pair, "funcs", "*", "i * 2 AS twice", "s name", "`id` AS `my id`")
    assert_same(*pair.both("funcs", lambda F, df: df.select("id", F.expr("i + 1"), F.expr("i + 1 AS x"),
                                                             F.expr("upper(s)").alias("u"))))


def test_EXPR_09_unsupported_constructs(pair):
    from mborobak.sql import functions as MF

    df = pair.mb("funcs")
    for text in ("split(s, 'a')", "s ILIKE 'A%'", "i DIV 2", "i & 1",
                 "INTERVAL 1 DAY"):
        with pytest.raises(UnsupportedOperationError):
            df.selectExpr(text)
    pair.sp("funcs").selectExpr("split(s, 'a')").collect()  # Spark has them
    with pytest.raises(UnsupportedOperationError):
        df.select(MF.expr("i IN (SELECT 1)"))


def test_EXPR_10_parse_errors(pair):
    for text in ("i +", "CASE WHEN i THEN", "(i + 1", "i i i", "'open", "i + * 2"):
        with pytest.raises(AnalysisError):
            pair.mb("funcs").selectExpr(text)
        with pytest.raises(Exception):  # noqa: B017 - Spark's ParseException / AnalysisException
            pair.sp("funcs").selectExpr(text).collect()


def test_NUM_14_fractional_constants_beside_integer_columns(pair):
    # IRIS turned 1.5 into 2 as a parameter next to an integer column (release 0.1: 7 + 1.5 gave 9)
    sel(pair, "funcs", "id", "i + 1.5", "i - 0.25", "coalesce(i, 1.5)", "i + 2.5D", "1.5 + i")
    assert_same(*pair.both("funcs", lambda F, df: df.select("id", F.col("i") + 1.5, F.col("i") - 0.25,
                                                             F.coalesce("i", F.lit(0.5)), F.col("l") + 0.5)))
