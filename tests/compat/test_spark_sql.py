"""Differential tests for Spark SQL statements in sql() (compatibility matrix §2.19, rows SQL-xx).

The same statement runs on both sides; ``{t}``, ``{l}``, ``{r}`` are the datasets, passed as PySpark's
``spark.sql(query, t=df)`` placeholders, so table names don't differ.
"""

from __future__ import annotations

import pytest

from mborobak.errors import AnalysisError, UnsupportedOperationError

from .conftest import assert_same

pytestmark = pytest.mark.compat


@pytest.fixture()
def sql(pair):
    pair.mb_session.conf.set("mborobak.sql.dialect", "spark")

    def run(query, args=None, ordered=False, **datasets):
        mb = pair.mb_session.sql(query, args, **{k: pair.mb(v) for k, v in datasets.items()})
        sp = pair.spark.sql(query, args, **{k: pair.sp(v) for k, v in datasets.items()})
        assert_same(mb, sp, ordered=ordered)
        return mb

    return run


def test_SQL_01_select_where_and_names(sql):
    sql("SELECT * FROM {t}", t="nums")
    sql("SELECT id, id + 1, n * 2 AS twice, d, CAST(i AS bigint) l2, upper('x') FROM {t} WHERE i > 0 OR d IS NULL",
        t="nums")
    sql("SELECT s, length(s), s RLIKE '^a', s = 'apple' FROM {t} WHERE s LIKE '%pp%' OR s IS NULL", t="strings")
    sql("SELECT x.id, x.s FROM {t} AS x WHERE x.s BETWEEN 'A' AND 'b'", t="strings")


def test_SQL_02_joins(sql):
    sql("SELECT l.id, r.id AS rid FROM {l} l JOIN {r} r ON l.k = r.k", l="left", r="right")
    sql("SELECT l.id, r.label FROM {l} AS l LEFT OUTER JOIN {r} AS r ON l.k = r.k", l="left", r="right")
    sql("SELECT l.id, r.id rid FROM {l} l RIGHT JOIN {r} r ON l.k = r.k", l="left", r="right")
    sql("SELECT l.id, r.id rid FROM {l} l FULL OUTER JOIN {r} r ON l.k = r.k", l="left", r="right")
    sql("SELECT * FROM {l} l LEFT SEMI JOIN {r} r ON l.k = r.k", l="left", r="right")
    sql("SELECT * FROM {l} l LEFT ANTI JOIN {r} r ON l.k = r.k", l="left", r="right")
    sql("SELECT * FROM {l} JOIN {r} USING (k)", l="left", r="right")
    sql("SELECT r.* FROM {l} l JOIN {r} r ON l.k = r.k AND r.label <> 'Y'", l="left", r="right")
    sql("SELECT l.id, r.id rid FROM {l} l, {r} r WHERE l.k = r.k", l="left", r="right")
    sql("SELECT l.id, r.id rid FROM {l} l CROSS JOIN {r} r", l="left", r="right")


def test_SQL_03_grouping(sql):
    sql("SELECT k, count(*) AS c, sum(id) FROM {t} GROUP BY k", t="left")
    sql("SELECT k % 2 AS parity, count(*) n FROM {t} GROUP BY parity", t="left")  # by alias
    sql("SELECT k, max(name) FROM {t} GROUP BY 1", t="left")  # by ordinal
    sql("SELECT k, name, count(*) FROM {t} GROUP BY ALL", t="left")
    sql("SELECT k, count(*) FROM {t} GROUP BY k HAVING count(*) > 1", t="left")
    sql("SELECT count(*), avg(d), max(n) FROM {t}", t="nums")
    sql("SELECT i % 3 AS m, sum(id) + count(*) AS x, sum(id) / 2 FROM {t} GROUP BY i % 3", t="nums")
    sql("SELECT upper(s) AS u, count(*) FROM {t} GROUP BY upper(s)", t="strings")
    sql("SELECT k FROM {t} GROUP BY k", t="left")
    sql("SELECT g, count(*) c FROM {t} GROUP BY g ORDER BY c DESC, g", ordered=True, t="wins")


def test_SQL_04_order_limit_distinct(sql):
    sql("SELECT id, s FROM {t} ORDER BY s, id", ordered=True, t="strings")
    sql("SELECT id AS key, s FROM {t} ORDER BY key DESC", ordered=True, t="strings")  # by alias
    sql("SELECT s, id FROM {t} ORDER BY 2 DESC LIMIT 4", ordered=True, t="strings")  # by ordinal
    sql("SELECT id FROM {t} ORDER BY d DESC NULLS LAST, id", ordered=True, t="nums")  # a column not selected
    sql("SELECT DISTINCT k FROM {t} ORDER BY k NULLS LAST", ordered=True, t="left")
    sql("SELECT DISTINCT upper(s) FROM {t}", t="strings")
    sql("SELECT id FROM {t} ORDER BY id LIMIT 3", ordered=True, t="nums")


def test_SQL_05_union(sql):
    sql("SELECT k, name FROM {l} UNION ALL SELECT k, label FROM {r}", l="left", r="right")
    sql("SELECT k FROM {l} UNION SELECT k FROM {r} ORDER BY k NULLS FIRST", ordered=True, l="left", r="right")
    sql("SELECT k FROM {l} UNION DISTINCT SELECT k FROM {r} ORDER BY 1 DESC LIMIT 2", ordered=True, l="left",
        r="right")


def test_SQL_06_ctes_and_subqueries(sql):
    sql("WITH a AS (SELECT k, count(*) c FROM {t} GROUP BY k), b AS (SELECT * FROM a WHERE c > 1) "
        "SELECT b.k, b.c FROM b", t="left")
    sql("SELECT q.k, q.total FROM (SELECT k, sum(id) AS total FROM {t} GROUP BY k) AS q WHERE q.total > 2", t="left")
    sql("WITH x AS (SELECT * FROM {t}) SELECT a.id, b.id AS bid FROM x a JOIN x b ON a.k = b.k AND a.id < b.id",
        t="left")


def test_SQL_07_parameters_and_placeholders(sql):
    sql("SELECT id FROM {t} WHERE i > ? AND s <> ?", [0, "x"], t="funcs")
    sql("SELECT id, :factor * i AS scaled FROM {t} WHERE id < :top", {"factor": 2, "top": 5}, t="funcs")


@pytest.mark.parametrize("query", [
    "CREATE TABLE x (a INT)", "INSERT INTO x SELECT 1", "SELECT 1",
    "SELECT k FROM {t} EXCEPT SELECT k FROM {t}", "SELECT k FROM {t} LIMIT 1 OFFSET 1",
    "SELECT k, count(*), rank() OVER (ORDER BY k) FROM {t} GROUP BY k",
])
def test_SQL_08_unsupported_statements(pair, query):
    pair.mb_session.conf.set("mborobak.sql.dialect", "spark")
    with pytest.raises(UnsupportedOperationError):
        pair.mb_session.sql(query, t=pair.mb("left")).collect()


def test_SQL_09_analysis_errors(pair):
    pair.mb_session.conf.set("mborobak.sql.dialect", "spark")
    for query in ("SELECT k, name FROM {t} GROUP BY k", "SELECT nope FROM {t}", "SELECT k FROM {t} GROUP BY 5"):
        with pytest.raises(AnalysisError):
            pair.mb_session.sql(query, t=pair.mb("left"))
        with pytest.raises(Exception):  # noqa: B017 - AnalysisException
            pair.spark.sql(query, t=pair.sp("left")).collect()
