"""session.sql() without IRIS: query preparation and compilation."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog, type_from_description
from mborobak.errors import AnalysisError, UnsupportedOperationError, UnsupportedTypeError
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, IntegerType, StringType
from mborobak.sqltext import prepare

INT = ("a", 4, None, None, 10, 0, 1)
STR = ("name", 12, None, None, 20, 0, 1)


def session_with(*description):
    seen = []

    def describer(sql, params):
        seen.append((sql, params))
        return list(description)

    s = MborobakSession._for_tests(StaticCatalog({}), describer=describer, **{"mborobak.sql.dialect": "iris"})
    return s, seen


@pytest.mark.parametrize("query, body", [
    ("SELECT a FROM t ORDER BY a;", "SELECT TOP ALL a FROM t ORDER BY a"),
    ("select distinct a from t order by a", "select distinct TOP ALL a from t order by a"),
    ("SELECT TOP 5 a FROM t ORDER BY a", "SELECT TOP 5 a FROM t ORDER BY a"),
    ("SELECT a FROM (SELECT TOP 3 a FROM t ORDER BY a) x", "SELECT a FROM (SELECT TOP 3 a FROM t ORDER BY a) x"),
    ("SELECT a FROM t UNION ALL SELECT b FROM u ORDER BY 1",
     "SELECT TOP ALL * FROM (SELECT a FROM t UNION ALL SELECT b FROM u) AS mborobak_u ORDER BY 1"),
    ("SELECT 'it''s; ORDER BY', \"order\" FROM t -- ORDER BY z",
     "SELECT 'it''s; ORDER BY', \"order\" FROM t -- ORDER BY z"),
])
def test_prepare_makes_subquery_safe(query, body):
    assert prepare(query).body == body


def test_prepare_splits_cte_and_counts_parameters():
    q = prepare("WITH c AS (SELECT a FROM t WHERE x = ?) SELECT a FROM c WHERE a > ? ORDER BY a")
    assert q.cte == "WITH c AS (SELECT a FROM t WHERE x = ?)"
    assert (q.cte_params, q.params) == (1, 2)
    assert q.body == "SELECT TOP ALL a FROM c WHERE a > ? ORDER BY a"


@pytest.mark.parametrize("query", ["DELETE FROM t", "SELECT 1; SELECT 2", "UPDATE t SET a = 1",
                                   "SELECT a FROM t; DROP TABLE t"])
def test_prepare_rejects_non_select(query):
    with pytest.raises(UnsupportedOperationError):
        prepare(query)


def test_type_mapping_from_description():
    assert type_from_description(4, 10, 0)[1] == IntegerType()
    assert type_from_description(2, 10, 2)[1] == DecimalType(10, 2)
    assert type_from_description(12, 4096, 0)[1] == StringType()
    assert type_from_description(1092, 18, -1)[1] is None  # TIME


def test_sql_alone_runs_as_written():
    s, seen = session_with(INT, STR)
    df = s.sql("SELECT a, name FROM t WHERE a > ? ORDER BY a", [1])
    assert seen == [("SELECT * FROM (SELECT TOP ALL a, name FROM t WHERE a > ? ORDER BY a) AS mborobak_q "
                     "WHERE 1 = 0", [1])]
    assert df.toSQL() == ("SELECT a, name FROM t WHERE a > ? ORDER BY a", [1])
    assert df.dtypes == [("a", "int"), ("name", "string")]


def test_sql_as_subquery_keeps_parameter_order():
    s, _ = session_with(INT, STR)
    df = s.sql("SELECT a, name FROM t WHERE a > ?", [1]).select((F.col("a") + 5).alias("b"), "name")
    text, params = df.toSQL()
    assert text.startswith('SELECT (t1."a" + ?) AS "b", t1."name"\nFROM (\nSELECT a, name FROM t WHERE a > ?\n)')
    assert params == [5, 1]


def test_cte_moves_to_the_front():
    s, _ = session_with(INT, STR)
    df = s.sql("WITH c AS (SELECT a, name FROM t WHERE a > ?) SELECT a, name FROM c", [1]) \
        .filter(F.col("name") == "x")
    text, params = df.toSQL()
    assert text.startswith("WITH c AS (SELECT a, name FROM t WHERE a > ?)\nSELECT")
    assert params == [1, "x"]


def test_errors():
    s, _ = session_with(INT, INT)
    with pytest.raises(ValueError, match="1 '\\?' markers"):
        s.sql("SELECT a FROM t WHERE a > ?")
    with pytest.raises(ValueError, match="named parameter :x"):
        s.sql("SELECT a FROM t WHERE a > :x")
    with pytest.raises(ValueError, match="no value for parameter"):
        s.sql("SELECT a FROM t WHERE a > :x", {"y": 1})
    with pytest.raises(UnsupportedOperationError, match=":name markers, not \\?"):
        s.sql("SELECT a FROM t WHERE a > ? AND a < :x", {"x": 1})
    dup = s.sql("SELECT a, a FROM t")
    assert dup.toSQL()[0] == "SELECT a, a FROM t"  # fine on its own
    with pytest.raises(AnalysisError, match="duplicate column names"):
        dup.limit(1).toSQL()
    t, _ = session_with(("t", 1092, None, None, 18, -1, 1))
    with pytest.raises(UnsupportedTypeError, match="TIME|time"):
        t.sql("SELECT CAST('10:00' AS TIME) AS t")


def test_named_parameters():
    s, seen = session_with(INT)
    df = s.sql("SELECT a FROM t WHERE a > :low AND a < :high OR a = :low -- :ignored\n AND ':low' <> ''",
               {"low": 1, "high": 9, "unused": 0})
    assert df.toSQL() == ("SELECT a FROM t WHERE a > ? AND a < ? OR a = ? -- :ignored\n AND ':low' <> ''", [1, 9, 1])
    assert seen[0][1] == [1, 9, 1]


def test_read_table_is_table():
    from mborobak.sql.types import IntegerType as Int

    s = MborobakSession._for_tests(StaticCatalog({"Sales": [("id", Int())]}))
    assert s.read.table("Sales").toSQL() == s.table("Sales").toSQL()
    assert s.read.format("delta").option("x", 1).table("Sales").columns == ["id"]
    for name in ("load", "csv", "parquet", "json"):
        with pytest.raises(UnsupportedOperationError, match="reads files"):
            getattr(s.read, name)("/tmp/x")
