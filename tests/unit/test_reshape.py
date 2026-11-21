"""pivot, unpivot, rollup and cube, compiled without IRIS (compiler spec §7.21)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, UnsupportedOperationError
from mborobak.sql import functions as F
from mborobak.sql.types import DoubleType, IntegerType, StringType


@pytest.fixture()
def t():
    s = MborobakSession._for_tests(StaticCatalog({"T": [("g", StringType()), ("k", StringType()),
                                                         ("i", IntegerType()), ("x", DoubleType())]}))
    return s.table("T")


def sql(df):
    return df._session._compile(df._plan)


def test_pivot_names_types(t):
    assert t.groupBy("g").pivot("k", ["p", None, True]).count().dtypes == \
        [("g", "string"), ("p", "bigint"), ("null", "bigint"), ("true", "bigint")]
    two = t.groupBy("g").pivot("k", ["p"]).agg(F.sum("i"), F.max("x").alias("mx"))
    assert two.columns == ["g", "p_sum(i)", "p_mx"]
    assert all(f.nullable for f in two.schema.fields)
    assert t.groupBy().pivot("i", [1, 2]).sum("x").columns == ["1", "2"]


def test_pivot_sql(t):
    stmt = sql(t.groupBy("g").pivot("k", ["p", None]).count())
    assert "GROUP BY" in stmt.text and "IS NULL" in stmt.text and stmt.params.count("p") == 2


def test_pivot_errors(t):
    with pytest.raises(AnalysisError):
        t.rollup("g").pivot("k", ["p"])
    with pytest.raises(UnsupportedOperationError):
        t.groupBy("g").pivot("k", ["p"]).agg(F.median("x"))
    with pytest.raises(TypeError):
        t.groupBy("g").pivot("k", "p")


def test_rollup_cube(t):
    r = t.rollup("g", "k").agg(F.sum("i"))
    assert r.dtypes == [("g", "string"), ("k", "string"), ("sum(i)", "bigint")]
    assert sql(r).text.count("UNION ALL") == 2 and "ROLLUP" not in sql(r).text
    c = t.cube("g", "k").count()
    assert sql(c).text.count("UNION ALL") == 3
    assert [f.nullable for f in c.schema.fields] == [True, True, False]


def test_unpivot(t):
    u = t.unpivot("g", ["i", "x"], "var", "val")
    assert u.dtypes == [("g", "string"), ("var", "string"), ("val", "double")]
    assert [f.nullable for f in u.schema.fields] == [True, False, True]
    assert t.melt(ids=["g", "k"], values=None, variableColumnName="v", valueColumnName="w").columns == \
        ["g", "k", "v", "w"]
    with pytest.raises(AnalysisError):
        t.unpivot("g", ["k", "i"], "v", "w")
    with pytest.raises(AnalysisError):
        t.select("g").unpivot("g", None, "v", "w")
