"""Spark SQL expression strings (sqlexpr.py), without IRIS (StaticCatalog).

Values are checked against PySpark in tests/compat/test_expressions.py; these tests pin the parser,
literal types, default names and errors.
"""

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DateType, DecimalType, DoubleType, IntegerType, LongType, StringType
from mborobak.sqlexpr import ParseError, parse_expression, tokenize


@pytest.fixture()
def df():
    catalog = StaticCatalog({"T": [("i", IntegerType()), ("l", LongType()), ("d", DoubleType()),
                                   ("n", DecimalType(10, 2)), ("s", StringType()), ("t", StringType()),
                                   ("dt", DateType())]})
    return MborobakSession._for_tests(catalog).table("T")


# Names and types as PySpark 4.3 reports them for selectExpr (probed 2026-11-20).
@pytest.mark.parametrize("text, dtype, name", [
    ("i + 1", "int", "(i + 1)"),
    ("i + 1.5", "decimal(12,1)", "(i + 1.5)"),
    ("i * 2.0D", "double", "(i * 2.0)"),
    ("1e3", "double", "1000.0"),
    ("1L", "bigint", "1"),
    ("10000000000", "bigint", "10000000000"),
    ("1.50", "decimal(3,2)", "1.50"),
    (".5", "decimal(1,1)", "0.5"),
    ("1BD", "decimal(1,0)", "1"),
    ("'it\\'s'", "string", "it's"),
    ('"dq"', "string", "dq"),
    ("'a' 'b'", "string", "ab"),
    ("DATE '2026-01-01'", "date", "DATE '2026-01-01'"),
    ("i == 1", "boolean", "(i = 1)"),
    ("i != 1", "boolean", "(NOT (i = 1))"),
    ("NOT i > 1", "boolean", "(NOT (i > 1))"),
    ("i > 1 AND l < 3 OR s = 'x'", "boolean", "(((i > 1) AND (l < 3)) OR (s = x))"),
    ("i NOT IN (1, 2)", "boolean", "(NOT (i IN (1, 2)))"),
    ("s NOT LIKE 'a%'", "boolean", "(NOT s LIKE a%)"),
    ("i BETWEEN 1 AND 2", "boolean", "between(i, 1, 2)"),
    ("i NOT BETWEEN 1 AND 2", "boolean", "(NOT between(i, 1, 2))"),
    ("CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END", "string",
     "CASE WHEN (i = 1) THEN one WHEN (i = 2) THEN two END"),
    ("CAST(s AS INT)", "int", "s"),
    ("cast(d as decimal(10,2))", "decimal(10,2)", "d"),
    ("s || t", "string", "concat(s, t)"),
    ("+i", "int", "(+ i)"),
    ("-i", "int", "(- i)"),
    ("UPPER(s)", "string", "upper(s)"),
    ("count(*)", "bigint", "count(1)"),
    ("count(DISTINCT s)", "bigint", "count(DISTINCT s)"),
    ("if(i > 1, 'a', 'b')", "string", "(IF((i > 1), a, b))"),
    ("pow(i, 2)", "double", "pow(i, 2)"),
    ("power(i, 2)", "double", "POWER(i, 2)"),
    ("log(d)", "double", "LOG(E(), d)"),
    ("log(2, d)", "double", "LOG(2, d)"),
    ("substr(s, 2)", "string", "substr(s, 2, 2147483647)"),
    ("current_date", "date", "current_date()"),
    ("abs(-1)", "int", "abs(-1)"),
    ("isnull(s)", "boolean", "(s IS NULL)"),
    ("1 + 2 * 3", "int", "(1 + (2 * 3))"),
    ("i > 1 = true", "boolean", "((i > 1) = true)"),
    ("`i`", "int", "i"),
    ("i + 1 AS x", "int", "x"),
    ("i + 1 x", "int", "x"),
    ("row_number() OVER (PARTITION BY s ORDER BY i)", "int",
     "row_number() OVER (PARTITION BY s ORDER BY i ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
    ("sum(i) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)", "bigint",
     "sum(i) OVER (ORDER BY i ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"),
    ("rank() OVER (ORDER BY d DESC NULLS LAST)", "int",
     "RANK() OVER (ORDER BY d DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
    ("lag(i, 1) OVER (ORDER BY i)", "int",
     "lag(i, 1, NULL) OVER (ORDER BY i ASC NULLS FIRST ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING)"),
    ("count(*) OVER (PARTITION BY s)", "bigint",
     "count(1) OVER (PARTITION BY s ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"),
])
def test_names_and_types_follow_pyspark(df, text, dtype, name):
    assert df.selectExpr(text).dtypes == [(name, dtype)]


def test_api_names_match_too(df):
    import datetime

    assert df.select(F.col("i") != 1).columns == ["(NOT (i = 1))"]
    assert df.select(F.lit(datetime.date(2026, 1, 1))).columns == ["DATE '2026-01-01'"]
    assert df.select(F.expr("i + 1")).columns == ["(i + 1)"]
    assert df.select(F.expr("i + 1 AS x")).columns == ["x"]
    assert df.selectExpr("*", "i * 2 AS j").columns == ["i", "l", "d", "n", "s", "t", "dt", "j"]


def test_filter_and_where_take_strings(df):
    text, params = df.filter("i > 1 AND s LIKE 'a%'").toSQL()
    assert text.endswith('WHERE ("i" > ? AND %EXACT("s") LIKE ?)') and params == [1, "a%"]
    assert df.where("s = 'x'").toSQL() == df.filter(F.col("s") == "x").toSQL()


def test_tokenizer():
    kinds = [(t.kind, t.text) for t in tokenize("a.`b c` >= 1.5e2D || 'x''y'")][:-1]
    assert kinds == [("ident", "a"), ("op", "."), ("qident", "`b c`"), ("op", ">="), ("num", "1.5e2D"),
                     ("op", "||"), ("str", "'x''y'")]
    assert tokenize("'x' 'y'")[0].value == "xy"  # adjacent literals concatenate


def test_precedence():
    from mborobak.naming import expr_name

    assert expr_name(parse_expression("NOT a = 1 AND b OR c")) == "(((NOT (a = 1)) AND b) OR c)"
    assert expr_name(parse_expression("a + b * c - d")) == "((a + (b * c)) - d)"
    assert expr_name(parse_expression("a || b = c")) == "(concat(a, b) = c)"


@pytest.mark.parametrize("text", ["i +", "CASE WHEN i THEN", "(i + 1", "i i i", "'open", "i + * 2", "count(i, s)",
                                  "DATE 'nope'", "sum(i) OVER (ROWS 1 BETWEEN)"])
def test_parse_errors(df, text):
    with pytest.raises(AnalysisError):
        df.selectExpr(text)


def test_parse_error_is_an_analysis_error():
    assert issubclass(ParseError, AnalysisError)
    with pytest.raises(ParseError, match="at position 4"):
        parse_expression("a + )")


@pytest.mark.parametrize("text", ["s RLIKE 'a'", "s REGEXP 'a'", "regexp_replace(s, 'a', 'b')", "s ILIKE 'A%'",
                                  "i DIV 2", "i & 1", "~i", "INTERVAL 1 DAY", "i IN (SELECT 1)", "sum(DISTINCT i)",
                                  "no_such_function(i)", "CAST(i AS SMALLINT)", "1S"])
def test_unsupported_constructs(df, text):
    with pytest.raises(UnsupportedOperationError):
        df.selectExpr(text)


def test_fractional_constants_are_cast(df):
    # IRIS turns a fractional parameter beside an integer column into an integer (matrix NUM-14)
    text, params = df.selectExpr("i + 1.5", "i - 0.25D").toSQL()
    assert '("i" + CAST(? AS NUMERIC(2,1)))' in text and '("i" - CAST(? AS DOUBLE))' in text
    text, _ = df.select(F.col("i") + 1.5, F.col("i") + 2, F.col("d") > float("inf")).toSQL()
    assert "CAST(? AS DOUBLE)" in text and '("i" + ?)' in text and '"d" > ?' in text  # integers, infinity: bare
