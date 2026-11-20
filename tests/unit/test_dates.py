"""Date and time functions and text-to-date casts, without IRIS (StaticCatalog).

Values are checked against PySpark in tests/compat/test_dates.py; these tests pin names, types,
errors, pattern translation and parameter order.
"""

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.dates import parse_plan, tokens
from mborobak.errors import AnalysisError, UnsupportedOperationError
from mborobak.sql import Window
from mborobak.sql import functions as F
from mborobak.sql.types import DateType, IntegerType, StringType, TimestampNTZType


@pytest.fixture()
def df():
    catalog = StaticCatalog({"T": [("id", IntegerType(), False), ("d", DateType()), ("ts", TimestampNTZType()),
                                   ("s", StringType()), ("n", IntegerType())]})
    return MborobakSession._for_tests(catalog).table("T")


# Names and types as PySpark 4.3 reports them (probed 2026-11-20); timestamps are timestamp_ntz (TIME-05).
@pytest.mark.parametrize("column, name, dtype", [
    (F.current_date(), "current_date()", "date"),
    (F.current_timestamp(), "current_timestamp()", "timestamp_ntz"),
    (F.year("d"), "year(d)", "int"),
    (F.year("s"), "year(s)", "int"),
    (F.day("d"), "day(d)", "int"),
    (F.weekofyear("d"), "weekofyear(d)", "int"),
    (F.hour("d"), "hour(d)", "int"),
    (F.second("ts"), "second(ts)", "int"),
    (F.date_add("d", 1), "date_add(d, 1)", "date"),
    (F.date_add("ts", F.col("n")), "date_add(ts, n)", "date"),
    (F.date_sub("d", 1), "date_sub(d, 1)", "date"),
    (F.datediff("ts", "d"), "datediff(ts, d)", "int"),
    (F.add_months("d", 1), "add_months(d, 1)", "date"),
    (F.months_between("ts", "d"), "months_between(ts, d, true)", "double"),
    (F.months_between("ts", "d", False), "months_between(ts, d, false)", "double"),
    (F.last_day("ts"), "last_day(ts)", "date"),
    (F.to_date("s"), "to_date(s)", "date"),
    (F.to_date("s", "yyyy-MM-dd"), "to_date(s, yyyy-MM-dd)", "date"),
    (F.to_timestamp("s"), "to_timestamp(s)", "timestamp_ntz"),
    (F.date_trunc("month", "ts"), "date_trunc(month, ts)", "timestamp_ntz"),
    (F.trunc("d", "month"), "trunc(d, month)", "date"),
    (F.date_format("ts", "yyyy-MM-dd HH:mm"), "date_format(ts, yyyy-MM-dd HH:mm)", "string"),
])
def test_names_and_types_follow_pyspark(df, column, name, dtype):
    assert df.select(column).dtypes == [(name, dtype)]


@pytest.mark.parametrize("build, error", [
    (lambda df: df.select(F.year("n")), AnalysisError),                         # not a date
    (lambda df: df.select(F.date_add("d", F.col("s"))), AnalysisError),         # days must be an integer
    (lambda df: df.select(F.date_format("ts", "yyyy ww")), UnsupportedOperationError),  # week-based fields
    (lambda df: df.select(F.to_date("s", "dd/MM/yy")), UnsupportedOperationError),      # yy: IRIS reads 19xx
    (lambda df: df.select(F.to_date("n", "yyyy")), AnalysisError),               # format needs text
])
def test_errors_at_the_call(df, build, error):
    with pytest.raises(error):
        build(df)


def test_pattern_tokens_and_parse_plan():
    assert tokens("yyyy-MM-dd'T'HH:mm") == [("field", "yyyy"), ("text", "-"), ("field", "MM"), ("text", "-"),
                                            ("field", "dd"), ("text", "T"), ("field", "HH"), ("text", ":"),
                                            ("field", "mm")]
    assert tokens("h 'o''clock'") == [("field", "h"), ("text", " "), ("text", "o'clock")]
    pattern, fmt, blanks = parse_plan("to_date", "dd/MM/yyyy HH:mm")
    assert pattern == '2N1"/"2N1"/"4N1" "2N1":"2N' and fmt == "DD/MM/YYYY HH24:MI" and blanks == []
    pattern, fmt, blanks = parse_plan("to_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS")
    assert fmt == "YYYY-MM-DD HH24:MI:SS.FF" and blanks == ["T"] and pattern.endswith('1"."3N')


def test_text_casts_are_guarded(df):
    text, _ = df.select(F.col("s").cast("date")).toSQL()
    assert text.startswith("SELECT TO_DATE(CASE WHEN \"s\" IS NULL THEN NULL WHEN TRIM(\"s\") %PATTERN '4N'")
    assert "$LISTGET(" in text  # the cast trigger for text of no known shape
    text, _ = df.select(F.to_timestamp("s")).toSQL()
    assert text.startswith("SELECT TO_TIMESTAMP(CASE") and "'YYYY-MM-DD HH24:MI:SS.FF'" in text


def test_sql_for_iris_quirks(df):
    assert "DATEDIFF('dd', \"d\", \"ts\")" in df.select(F.datediff(F.col("ts").cast("date"), "d")).toSQL()[0] \
        or "DATEDIFF('dd', \"d\", CAST(\"ts\" AS DATE))" in df.select(F.datediff("ts", "d")).toSQL()[0]
    assert "DATEPART('ss', \"ts\")" in df.select(F.second("ts")).toSQL()[0]   # SECOND() rounds the fraction
    assert "HOUR(CAST(\"d\" AS TIMESTAMP))" in df.select(F.hour("d")).toSQL()[0]  # HOUR(date) is wrong in IRIS
    assert "CASE WHEN \"d\" IS NULL THEN NULL ELSE CAST(LAST_DAY(\"d\")" in df.select(F.last_day("d")).toSQL()[0]
    assert df.select(F.trunc("d", "day")).toSQL()[0].startswith("SELECT CAST(NULL AS DATE)")  # as Spark


ALL = [
    F.year("s"), F.weekofyear("s"), F.hour("s"), F.date_add("s", F.lit(2)), F.date_sub(F.lit("2026-01-01"), 3),
    F.add_months("s", 1), F.datediff(F.lit("2026-03-01"), "s"), F.months_between(F.lit("2026-01-31"), "s"),
    F.months_between("ts", F.lit("2026-02-15 06:00:00"), False), F.last_day(F.lit("2026-02-10")),
    F.trunc(F.lit("2026-05-05"), "quarter"), F.date_trunc("second", F.lit("2026-01-01 10:00:00")),
    F.date_format(F.lit("2026-01-01"), "yyyy-MM-dd'x'EEE"), F.to_date(F.lit("01/02/2026"), "dd/MM/yyyy"),
    F.to_timestamp(F.lit("2026-01-01T10:00"), "yyyy-MM-dd'T'HH:mm"), F.to_date(F.lit(" 2026-1-5 ")),
]


@pytest.mark.parametrize("column", ALL)
def test_parameters_match_placeholders(df, column):
    # every argument is rendered again at each use: one parameter per '?'
    text, params = df.select(column).toSQL()
    assert text.count("?") == len(params)
    text, params = df.withColumn("w", F.sum("n").over(Window.orderBy("id"))).filter(column.isNotNull()).toSQL()
    assert text.count("?") == len(params)
