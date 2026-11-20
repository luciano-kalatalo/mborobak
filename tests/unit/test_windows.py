"""Window functions and dropDuplicates, without IRIS (StaticCatalog).

Values are checked against PySpark in tests/compat/test_windows.py; these tests pin names, types,
errors and the shape of the SQL.
"""

import re

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, UnsupportedOperationError
from mborobak.sql import Window
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, DoubleType, IntegerType, StringType


@pytest.fixture()
def df():
    catalog = StaticCatalog({"T": [("id", IntegerType(), False), ("g", StringType()), ("k", IntegerType()),
                                   ("d", DoubleType()), ("n", DecimalType(10, 2)), ("x", IntegerType())]})
    return MborobakSession._for_tests(catalog).table("T")


W = Window.partitionBy("g").orderBy("k")


# Names and types as PySpark 4.3 reports them (probed 2026-11-20).
@pytest.mark.parametrize("column, name, dtype", [
    (F.row_number().over(W), "row_number() OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN "
                             "UNBOUNDED PRECEDING AND CURRENT ROW)", "int"),
    (F.rank().over(W), "RANK() OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING "
                       "AND CURRENT ROW)", "int"),
    (F.cume_dist().over(W), "cume_dist() OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED "
                            "PRECEDING AND CURRENT ROW)", "double"),
    (F.ntile(3).over(W), "ntile(3) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN UNBOUNDED "
                         "PRECEDING AND CURRENT ROW)", "int"),
    (F.lag("x", 2, 0).over(W), "lag(x, 2, 0) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN "
                               "-2 FOLLOWING AND -2 FOLLOWING)", "int"),
    (F.lag("x", 0).over(W), "lag(x, 0, NULL) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN "
                            "0 FOLLOWING AND 0 FOLLOWING)", "int"),
    (F.first("x").over(W), "first(x) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED "
                           "PRECEDING AND CURRENT ROW)", "int"),
    (F.sum("x").over(Window.partitionBy("g")), "sum(x) OVER (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND "
                                               "UNBOUNDED FOLLOWING)", "bigint"),
    (F.count("*").over(Window.partitionBy()), "count(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED "
                                              "FOLLOWING)", "bigint"),
    (F.avg("x").over(W.rowsBetween(-1, 1)), "avg(x) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST ROWS BETWEEN "
                                            "-1 FOLLOWING AND 1 FOLLOWING)", "double"),
    (F.avg("n").over(W), "avg(n) OVER (PARTITION BY g ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED "
                         "PRECEDING AND CURRENT ROW)", "decimal(14,6)"),
    (F.row_number().over(Window.orderBy(F.col("k").desc_nulls_last(), "id")),
     "row_number() OVER (ORDER BY k DESC NULLS LAST, id ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND "
     "CURRENT ROW)", "int"),
])
def test_names_and_types_follow_pyspark(df, column, name, dtype):
    assert df.select(column).dtypes == [(name, dtype)]


def test_window_constants_match_pyspark():
    assert Window.unboundedPreceding == -(1 << 63)
    assert Window.unboundedFollowing == (1 << 63) - 1
    assert Window.currentRow == 0


@pytest.mark.parametrize("build", [
    lambda df: df.select(F.row_number().over(Window.partitionBy("g"))),     # needs an order
    lambda df: df.select(F.rank().over(W.rowsBetween(-1, 1))),              # fixed frame
    lambda df: df.select(F.lag("x").over(W.rowsBetween(-1, 1))),            # no frame for lag
    lambda df: df.select(F.countDistinct("x").over(W)),                     # distinct
    lambda df: df.select(F.row_number()),                                   # no OVER
    lambda df: df.select(F.sum("x").over(W.rowsBetween(1, -1))),            # start after end
    lambda df: df.filter(F.row_number().over(W) == 1),                      # not in WHERE
    lambda df: df.orderBy(F.rank().over(W)),
    lambda df: df.groupBy("g").agg(F.rank().over(W)),
    lambda df: df.groupBy(F.rank().over(W)).count(),
    lambda df: df.select(F.sum(F.lag("x").over(W)).over(W)),                # nested
])
def test_analysis_errors_at_the_call(df, build):
    with pytest.raises(AnalysisError):
        build(df)


@pytest.mark.parametrize("build", [
    lambda df: df.select(F.sum("x").over(W.rangeBetween(-1, 1))),           # IRIS has no RANGE frames
    lambda df: df.select(F.sum("x").over(W.rangeBetween(0, 0))),
    lambda df: df.select(F.sum("x").over(Window.partitionBy("g").rowsBetween(-1, 1))),  # rows without order
])
def test_unsupported_frames(df, build):
    with pytest.raises(UnsupportedOperationError):
        build(df)
    with pytest.raises(NotImplementedError):
        F.first("x", ignorenulls=True)


def test_windows_are_computed_in_a_subquery(df):
    text, params = df.withColumn("rn", F.row_number().over(W)).filter(F.col("rn") == 1).toSQL()
    assert text == (
        'SELECT t1."id", t1."g", t1."k", t1."d", t1."n", t1."x", t1."mborobak_w{}" AS "rn"\n'.format(
            text.split('"mborobak_w')[1].split('"')[0])
        + "FROM (\n"
        + '  SELECT "id", "g", "k", "d", "n", "x", ROW_NUMBER() OVER (PARTITION BY %EXACT("g") ORDER BY "k") '
          f'AS "mborobak_w{text.split(chr(34) + "mborobak_w")[1].split(chr(34))[0]}"\n'
        + '  FROM "SQLUser"."T"\n'
        + ") AS t1\n"
        + f'WHERE t1."mborobak_w{text.split(chr(34) + "mborobak_w")[1].split(chr(34))[0]}" = ?'
    )
    assert params == [1]


def test_case_reads_the_window_column(df):
    text, _ = df.select(F.when(F.row_number().over(W) == 1, "a").alias("p")).toSQL()
    outer = text.split("FROM (")[0]
    assert "OVER" not in outer and "CASE WHEN t1." in outer  # IRIS gets windows in CASE conditions wrong


def test_order_keys_reuse_sort_rules(df):
    text, _ = df.select(F.row_number().over(Window.orderBy("d"))).toSQL()
    assert 'OVER (ORDER BY CASE WHEN "d" IS NULL THEN 0' in text and '"d" + 0)' in text   # double bucket
    text, _ = df.select(F.row_number().over(Window.orderBy("g"))).toSQL()
    assert "OVER (ORDER BY %EXACT(' ' || \"g\"))" in text                                # string order
    text, _ = df.select(F.row_number().over(Window.orderBy(F.col("k").asc_nulls_last()))).toSQL()
    assert 'OVER (ORDER BY CASE WHEN "k" IS NULL THEN 1 ELSE 0 END, "k")' in text        # NULLS LAST flag
    text, _ = df.select(F.min("g").over(Window.partitionBy("k"))).toSQL()
    assert "SUBSTRING(MIN(%EXACT(' ' || \"g\")) OVER (PARTITION BY \"k\"), 2)" in text


def test_frames(df):
    def over(w):
        return df.select(F.sum("x").over(w)).toSQL()[0]
    assert 'ORDER BY "k" ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)' in over(W.rowsBetween(-1, 2))
    assert 'ORDER BY "k")' in over(W.rangeBetween(Window.unboundedPreceding, Window.currentRow))  # IRIS default
    assert "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING" in over(
        W.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    # current row to the end: the default frame over the reversed order
    assert 'ORDER BY "k" DESC)' in over(W.rangeBetween(Window.currentRow, Window.unboundedFollowing))
    text, _ = df.select(F.first("x").over(W.rangeBetween(0, Window.unboundedFollowing))).toSQL()
    assert "LAST_VALUE(" in text
    assert "OVER (PARTITION BY %EXACT(\"g\"))" in over(Window.partitionBy("g"))


def test_lag_lead_sql(df):
    text, params = df.select(F.lag("x", 2, 0).over(W), F.lag("x", -1).over(W), F.lead("g", 1, "z").over(W)).toSQL()
    assert 'LAG("x", 2, ?) OVER' in text and 'LEAD("x", 1) OVER' in text and 'LEAD("g", 1, ?) OVER' in text
    assert params == [0, "z"]
    assert "LAG(" not in df.select(F.lag("x", 0).over(W)).toSQL()[0]  # lag by 0 rows is the value itself


def test_windows_after_limit_and_aggregate(df):
    text, _ = df.limit(5).withColumn("r", F.rank().over(Window.orderBy("id"))).toSQL()
    assert text.index("TOP ?") > text.index("RANK()")  # TOP in an inner query: the window sees 5 rows
    text, _ = df.groupBy("g").agg(F.sum("x").alias("s")).withColumn("r", F.rank().over(Window.orderBy("s"))).toSQL()
    assert 'RANK() OVER (ORDER BY SUM("x"))' in text and "GROUP BY" in text


def test_drop_duplicates(df):
    text, params = df.dropDuplicates(["g", "k"]).toSQL()
    assert 'ROW_NUMBER() OVER (PARTITION BY %EXACT("g"), "k"' in text and params == [1]
    assert df.dropDuplicates(["g"]).columns == df.columns
    assert df.dropDuplicates().toSQL()[0].startswith("SELECT DISTINCT")
    def plain(text):
        return re.sub(r"mborobak_w\d+", "w", text)
    assert plain(df.drop_duplicates(["k"]).toSQL()[0]) == plain(df.dropDuplicates(["k"]).toSQL()[0])
    with pytest.raises(Exception):
        df.dropDuplicates(["missing"])
