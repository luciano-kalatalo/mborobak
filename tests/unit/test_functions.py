"""Scalar functions, string order and fillna/dropna, without IRIS (StaticCatalog).

Result values are checked against PySpark in tests/compat/test_functions.py; these tests pin the types,
default names, errors and the shape of the generated SQL.
"""

import math

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, ColumnNotFoundError, UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, DoubleType, IntegerType, LongType, StringType


@pytest.fixture()
def df():
    catalog = StaticCatalog({
        "T": [("id", IntegerType(), False), ("s", StringType()), ("t", StringType()), ("d", DoubleType()),
              ("n", DecimalType(10, 3)), ("i", IntegerType()), ("l", LongType()),
              ("code", StringType(), True, "EXACT")],
    })
    return MborobakSession._for_tests(catalog).table("T")


# Names and types as PySpark 4.3 reports them (probed 2026-11-19).
@pytest.mark.parametrize("column, name, dtype", [
    (F.coalesce("i", "l"), "coalesce(i, l)", "bigint"),
    (F.coalesce("i", "n"), "coalesce(i, n)", "decimal(13,3)"),
    (F.coalesce("n", "d"), "coalesce(n, d)", "double"),
    (F.nullif("s", "t"), "nullif(s, t)", "string"),
    (F.nvl2("s", "t", F.lit("x")), "nvl2(s, t, x)", "string"),
    (F.isnan("d"), "isnan(d)", "boolean"),
    (F.isnull("s"), "(s IS NULL)", "boolean"),
    (F.isnotnull("s"), "(s IS NOT NULL)", "boolean"),
    (F.greatest("i", "n"), "greatest(i, n)", "decimal(13,3)"),
    (F.least("s", "t"), "least(s, t)", "string"),
    (F.length("s"), "length(s)", "int"),
    (F.substring("s", 1, 3), "substring(s, 1, 3)", "string"),
    (F.col("s").substr(1, 3), "substr(s, 1, 3)", "string"),
    (F.concat("s", "i"), "concat(s, i)", "string"),
    (F.concat_ws("-", "s", "t"), "concat_ws(-, s, t)", "string"),
    (F.lpad("s", 8, "*"), "lpad(s, 8, *)", "string"),
    (F.instr("s", "l"), "instr(s, l)", "int"),
    (F.locate("l", "s"), "locate(l, s, 1)", "int"),
    (F.replace("s", F.lit("l")), "replace(s, l, )", "string"),
    (F.round("i"), "round(i, 0)", "int"),
    (F.round("d", 1), "round(d, 1)", "double"),
    (F.round("n", 2), "round(n, 2)", "decimal(10,2)"),
    (F.round("n"), "round(n, 0)", "decimal(8,0)"),
    (F.round("n", -1), "round(n, -1)", "decimal(8,0)"),
    (F.bround("n", 2), "bround(n, 2)", "decimal(10,2)"),
    (F.abs("n"), "abs(n)", "decimal(10,3)"),
    (F.ceil("d"), "CEIL(d)", "bigint"),
    (F.ceil("n"), "CEIL(n)", "decimal(8,0)"),
    (F.ceiling("d"), "ceiling(d)", "bigint"),
    (F.floor("i"), "FLOOR(i)", "bigint"),
    (F.pow("i", 2), "POWER(i, 2)", "double"),
    (F.power("d", "i"), "POWER(d, i)", "double"),
    (F.sqrt("i"), "SQRT(i)", "double"),
    (F.exp("i"), "EXP(i)", "double"),
    (F.log("d"), "ln(d)", "double"),
    (F.log(2.0, "d"), "LOG(2.0, d)", "double"),
    (F.log10("d"), "LOG10(d)", "double"),
    (F.log2("d"), "LOG2(d)", "double"),
    (F.log1p("d"), "LOG1P(d)", "double"),
    (F.signum("i"), "SIGNUM(i)", "double"),
    (F.sign("d"), "sign(d)", "double"),
    (F.pmod("i", F.lit(3)), "pmod(i, 3)", "int"),
    (F.pmod("n", F.lit(2)), "pmod(n, 2)", "decimal(4,3)"),
])
def test_names_and_types_follow_pyspark(df, column, name, dtype):
    assert df.select(column).dtypes == [(name, dtype)]


@pytest.mark.parametrize("build, error", [
    (lambda df: df.select(F.upper("i")), AnalysisError),
    (lambda df: df.select(F.sqrt("s")), AnalysisError),
    (lambda df: df.select(F.coalesce("s", "i")), AnalysisError),
    (lambda df: df.select(F.greatest("i")), AnalysisError),
    (lambda df: df.select(F.concat("s", "d")), UnsupportedOperationError),  # double text differs (CAST-12)
    (lambda df: df.select(F.upper("missing")), ColumnNotFoundError),
])
def test_argument_errors_at_the_call(df, build, error):
    with pytest.raises(error):
        build(df)


def test_constant_arguments_must_be_constants():
    with pytest.raises(TypeError):
        F.substring("s", F.col("i"), 2)
    with pytest.raises(TypeError):
        F.lpad("s", 3, F.col("t"))
    with pytest.raises(NotImplementedError):
        F.trim("s", "x")


def test_function_results_compare_exactly(df):
    text, params = df.filter(F.upper("s") == "A").select("id").toSQL()
    assert 'WHERE %EXACT(UPPER("s")) = ?' in text and params == ["A"]
    text, _ = df.select(F.nullif("s", "code")).toSQL()
    assert 'NULLIF(%EXACT("s"), "code")' in text  # an EXACT column needs no %EXACT


def test_string_order_uses_leading_space(df):
    text, params = df.filter(F.col("code") > "5").select("id").toSQL()
    assert "WHERE %EXACT(' ' || \"code\") > %EXACT(' ' || ?)" in text and params == ["5"]
    text, _ = df.orderBy("s").select("s").toSQL()
    assert text.endswith("ORDER BY %EXACT(' ' || \"s\")")
    text, _ = df.agg(F.min("s")).toSQL()
    assert "SUBSTRING(MIN(%EXACT(' ' || \"s\")), 2)" in text
    text, _ = df.select(F.greatest("s", "t")).toSQL()
    assert text.startswith("SELECT SUBSTRING(GREATEST(%EXACT(' ' || COALESCE(")
    # equality and numbers are unchanged
    assert "' '" not in df.filter(F.col("s") == "5").toSQL()[0]
    assert "' '" not in df.filter(F.col("i") > 5).orderBy("i").toSQL()[0]


def test_iris_string_semantics_keep_iris_order():
    catalog = StaticCatalog({"T": [("s", StringType())]})
    df = MborobakSession._for_tests(catalog, **{"mborobak.sql.stringSemantics": "iris"}).table("T")
    text, _ = df.filter(F.col("s") > "5").orderBy("s").toSQL()
    assert "%EXACT" not in text


def test_isnan_in_where_uses_case(df):
    text, _ = df.filter(F.isnan("d")).toSQL()
    assert text.endswith('WHERE CASE WHEN "d" <> "d" THEN 1 ELSE 0 END = 1')
    assert df.filter(F.isnan("i")).toSQL()[0].endswith("WHERE 1 = 0")  # integers are never NaN


def test_repeated_arguments_keep_parameter_order(df):
    # each use of an argument renders it again, so its parameters appear in text order
    text, params = df.select(F.concat_ws("+", F.concat("s", F.lit("a")), F.lit("b"))).toSQL()
    assert text.count("?") == len(params)
    assert params == ["+", "a", "+", "b"]
    text, params = df.select(F.pow(F.lit(2), "i")).toSQL()
    assert text.count("?") == len(params) and params[:3] == [2, 2, math.inf] and math.isnan(params[4])


def test_special_values_are_parameters(df):
    _, params = df.select(F.sqrt("d"), F.exp("d")).toSQL()
    assert math.isnan(params[0]) and params[1] == math.inf


def test_functions_inside_aggregates_and_keys(df):
    out = df.groupBy(F.lower("s").alias("k")).agg(F.round(F.avg("d"), 2).alias("a"), F.max(F.length("s")))
    assert out.dtypes == [("k", "string"), ("a", "double"), ("max(length(s))", "int")]
    text, _ = out.toSQL()
    assert 'GROUP BY %EXACT(LOWER("s"))' in text


def test_fillna_types_and_columns(df):
    out = df.fillna(0)
    assert out.dtypes == df.dtypes
    text, params = out.toSQL()
    assert 'COALESCE("i", ?) AS "i"' in text and '"s",' in text  # a number leaves strings alone
    assert '"id",' in text                                       # non-nullable columns are left as they are
    assert 'CASE WHEN ("d" IS NULL OR CASE WHEN "d" <> "d" THEN 1 ELSE 0 END = 1)' in text  # and NaN
    text, params = df.fillna("x", subset="s").toSQL()
    assert 'COALESCE("s", ?) AS "s"' in text and '"t",' in text and params == ["x"]
    assert df.na.fill({"s": "?", "i": 1}).toSQL()[1] == ["?", 1]
    with pytest.raises(AnalysisError):
        df.fillna({"s": 1})
    with pytest.raises(TypeError):
        df.fillna([1])


def test_dropna_conditions(df):
    text, _ = df.dropna(subset=["s", "d"]).toSQL()
    assert text.endswith('WHERE ("s" IS NOT NULL AND ("d" IS NOT NULL AND NOT (CASE WHEN "d" <> "d" '
                         'THEN 1 ELSE 0 END = 1)))')
    text, _ = df.na.drop(how="all", subset=["s", "t"]).toSQL()
    assert text.endswith('WHERE ("s" IS NOT NULL OR "t" IS NOT NULL)')
    assert df.dropna(how="all") is df   # "id" is never NULL, so no row has only NULLs
    assert df.dropna(subset=["id"]) is df
    text, params = df.dropna(thresh=2, subset=["id", "s", "t"]).toSQL()
    assert params[0] == 1 and params[-1] == 2  # "id" always counts
    with pytest.raises(ValueError):
        df.dropna(how="some")
