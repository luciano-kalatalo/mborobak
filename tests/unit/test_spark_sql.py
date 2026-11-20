"""Spark SQL statements in sql() without IRIS (sparksql.py, compiler spec §7.17)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError, UnsupportedOperationError
from mborobak.sql.types import DateType, DecimalType, IntegerType, StringType


@pytest.fixture()
def session():
    return MborobakSession._for_tests(StaticCatalog({
        "patients": [("id", StringType()), ("GENDER", StringType()), ("BIRTHDATE", DateType())],
        "cohort": [("patient", StringType()), ("name", StringType())],
        "Shop.Sales": [("k", IntegerType()), ("amount", DecimalType(10, 2))],
    }), **{"mborobak.sql.dialect": "spark"})


def test_the_dialect_is_a_setting():
    s = MborobakSession._for_tests(StaticCatalog({"T": [("a", IntegerType())]}),
                                   describer=lambda sql, params: [("a", 4, None, None, 10, 0, 1)])
    assert s.conf.get("mborobak.sql.dialect") == "iris"
    assert s.sql("SELECT a FROM T").toSQL()[0] == "SELECT a FROM T"  # passed through
    s.conf.set("mborobak.sql.dialect", "spark")
    assert s.sql("SELECT a FROM T").toSQL()[0] == 'SELECT "a"\nFROM "SQLUser"."T"'
    with pytest.raises(ValueError):
        s.conf.set("mborobak.sql.dialect", "postgres")


def test_notebook_queries(session):
    df = session.sql("select GENDER, ceil(months_between(current_date(),BIRTHDATE)/12/5)*5 as age, "
                     "count(*) as count from patients group by GENDER, age order by age")
    assert df.dtypes == [("GENDER", "string"), ("age", "bigint"), ("count", "bigint")]
    text, _ = df.toSQL()
    assert "GROUP BY" in text and text.count("SELECT") == 1  # one statement
    df = session.sql("SELECT p.* FROM cohort c INNER JOIN patients p on c.patient=p.id WHERE c.name=:n", {"n": "x"})
    assert df.columns == ["id", "GENDER", "BIRTHDATE"]
    assert df.toSQL()[1] == ["x"]


def test_names_qualifiers_and_schemas(session):
    df = session.sql("SELECT Sales.k, s2.amount FROM Shop.Sales JOIN Shop.Sales s2 USING (k)")
    assert df.columns == ["k", "amount"]
    assert session.sql("SELECT `k` AS `a b` FROM Shop.Sales;").columns == ["a b"]


def test_grouping_forms(session):
    by_alias = session.sql("SELECT k + 1 AS j, sum(amount) FROM Shop.Sales GROUP BY j")
    by_ordinal = session.sql("SELECT k + 1 AS j, sum(amount) FROM Shop.Sales GROUP BY 1")
    by_all = session.sql("SELECT k + 1 AS j, sum(amount) FROM Shop.Sales GROUP BY ALL")
    assert by_alias.toSQL() == by_ordinal.toSQL() == by_all.toSQL()
    assert by_alias.columns == ["j", "sum(amount)"]
    assert session.sql("SELECT sum(amount) FROM Shop.Sales HAVING sum(amount) > 1").columns == ["sum(amount)"]


def test_parameters(session):
    assert session.sql("SELECT k FROM Shop.Sales WHERE k > ? AND k < ?", [1, 9]).toSQL()[1] == [1, 9]
    with pytest.raises(ValueError, match="more \\? markers"):
        session.sql("SELECT k FROM Shop.Sales WHERE k > ?", [])
    with pytest.raises(ValueError, match="1 \\? markers but 2"):
        session.sql("SELECT k FROM Shop.Sales WHERE k > ?", [1, 2])
    with pytest.raises(ValueError, match=":x"):
        session.sql("SELECT k FROM Shop.Sales WHERE k > :x", {"y": 1})


def test_dataframe_placeholders(session):
    small = session.table("Shop.Sales").filter("k > 1")
    df = session.sql("SELECT s.k, c.name FROM {sales} s CROSS JOIN cohort c WHERE s.k < {limit}", sales=small, limit=5)
    assert df.columns == ["k", "name"]
    assert df.toSQL()[1] == [1, 5]


@pytest.mark.parametrize("query, error", [
    ("CREATE TABLE x (a INT)", UnsupportedOperationError), ("SELECT 1", UnsupportedOperationError),
    ("SELECT k FROM Shop.Sales LIMIT 1 OFFSET 2", UnsupportedOperationError),
    ("SELECT k FROM a.b.c", UnsupportedOperationError), ("SELECT k FROM Shop.Sales NATURAL JOIN cohort", UnsupportedOperationError),
    ("SELECT k FROM Shop.Sales EXCEPT SELECT k FROM Shop.Sales", UnsupportedOperationError),
    ("SELECT k, amount FROM Shop.Sales GROUP BY k", AnalysisError), ("SELECT k FROM Shop.Sales GROUP BY 3", AnalysisError),
    ("SELECT x.* FROM Shop.Sales", AnalysisError), ("SELECT k FROM Shop.Sales WHERE", AnalysisError),
])
def test_errors(session, query, error):
    with pytest.raises(error):
        session.sql(query)


def test_iris_dialect_formats_plain_values_only():
    s = MborobakSession._for_tests(StaticCatalog({}), describer=lambda sql, params: [("a", 4, None, None, 10, 0, 1)])
    assert s.sql("SELECT a FROM {t}", t="T").toSQL()[0] == "SELECT a FROM T"
    with pytest.raises(UnsupportedOperationError, match="Spark dialect"):
        s.sql("SELECT a FROM {t}", t=s.sql("SELECT 1 AS a FROM T"))
