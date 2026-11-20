"""Statistics, describe()/summary() and the small DataFrame API without IRIS (compiler spec §7.18)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, MborobakConnectionError, UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, DoubleType, IntegerType, StringType
from mborobak.sql.window import Window


@pytest.fixture()
def df():
    s = MborobakSession._for_tests(StaticCatalog({"T": [("g", StringType()), ("x", DoubleType()), ("i", IntegerType()),
                                                         ("n", DecimalType(10, 2)), ("s", StringType())]}))
    return s.table("T")


def test_names_and_types(df):
    out = df.groupBy("g").agg(F.stddev("x"), F.std("i"), F.var_pop("n"), F.corr("x", "i"), F.covar_samp("x", "n"),
                              F.skewness("x"), F.kurtosis("i"), F.median("n"), F.percentile("x", 0.25))
    assert out.dtypes == [("g", "string"), ("stddev(x)", "double"), ("std(i)", "double"), ("var_pop(n)", "double"),
                          ("corr(x, i)", "double"), ("covar_samp(x, n)", "double"), ("skewness(x)", "double"),
                          ("kurtosis(i)", "double"), ("median(n)", "double"), ("percentile(x, 0.25, 1)", "double")]
    assert df.selectExpr("stddev(x)", "median(i)", "percentile(x, 0.5)").columns == \
        ["stddev(x)", "median(i)", "percentile(x, 0.5, 1)"]


def test_stddev_sql(df):
    text, params = df.agg(F.stddev("i")).toSQL()
    assert 'STDDEV_SAMP(CAST("i" AS DOUBLE))' in text and "<> CAST" in text  # NaN check before IRIS's stddev
    assert text.count("SELECT") == 1


def test_median_is_one_statement_with_windows(df):
    text, params = df.groupBy("g").agg(F.median("x"), F.count("*")).toSQL()
    assert "ROW_NUMBER() OVER (PARTITION BY" in text and "GROUP BY" in text and len(params) < 60


def test_errors(df):
    with pytest.raises(AnalysisError, match="needs a number"):
        df.agg(F.stddev("s"))
    with pytest.raises(UnsupportedOperationError, match="array"):
        F.percentile("x", [0.1, 0.9])
    with pytest.raises(ValueError):
        F.percentile("x", 1.5)
    with pytest.raises(UnsupportedOperationError, match="over a window"):
        F.median("x").over(Window.partitionBy("g"))
    with pytest.raises(MborobakConnectionError):  # describe() loads its result as a DataFrame
        df.describe()


def test_small_api(df):
    assert df.toDF("a", "b", "c", "d", "e").columns == ["a", "b", "c", "d", "e"]
    with pytest.raises(ValueError):
        df.toDF("a")
    assert df.transform(lambda d, c: d.select(c), "x").columns == ["x"]
    with pytest.raises(TypeError):
        df.transform(lambda d: 1)
    df.createOrReplaceTempView("v")
    assert df._session.table("V").columns == df.columns
    with pytest.raises(AnalysisError, match="already exists"):
        df.createTempView("v")
    assert [t.name for t in df._session.catalog.listTables()] == ["v"]
    assert df._session.catalog.dropTempView("v") is True


def test_replace(df):
    text, _ = df.replace("a", "b").toSQL()
    assert '%EXACT("g") = ?' in text and '%EXACT("s") = ?' in text and '"x"' in text  # strings only
    with pytest.raises(ValueError, match="mixed"):
        df.replace([1, "a"], [2, "b"])
    with pytest.raises(ValueError):
        df.replace([1, 2], [3])
    assert df.na.replace({"a": "b"}).columns == df.columns
