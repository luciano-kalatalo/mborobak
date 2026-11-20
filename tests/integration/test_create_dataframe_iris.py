"""createDataFrame against a live IRIS: the temporary table's lifecycle (compiler spec §9.8)."""

from __future__ import annotations

import decimal
import os

import pytest

from mborobak import MborobakSession
from mborobak.errors import MborobakExecutionError
from mborobak.sql import functions as F

pytestmark = pytest.mark.iris


def temp_tables(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'MborobakTemp'")
    return cur.fetchall()[0][0]


def test_rows_belong_to_the_connection_and_stop_drops_the_table(iris_env, raw_connection):
    import iris

    other = iris.connect(iris_env["IRIS_HOST"], int(iris_env["IRIS_PORT"]), iris_env["IRIS_NAMESPACE"],
                         iris_env["IRIS_USERNAME"], iris_env["IRIS_PASSWORD"])
    try:
        before = temp_tables(raw_connection)
        s = MborobakSession.builder.connection(raw_connection).create()
        df = s.createDataFrame([(i, f"s{i}") for i in range(1000)], "id int, s string")
        m = s.lastExecution
        assert (m.action, m.rows, m.data_queries) == ("createDataFrame", 1000, 2)
        assert df.count() == 1000
        table = s._temp_tables[0]
        cur = other.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table.quoted()}")
        assert cur.fetchall()[0][0] == 0  # another connection sees the definition, not the rows
        assert temp_tables(raw_connection) == before + 1
        s.stop()
        assert temp_tables(raw_connection) == before
    finally:
        other.close()


def test_join_with_an_iris_table(session):
    names = session.createDataFrame([(10, "Ana"), (20, "Bo")], ["customer_id", "who"])
    df = session.table("MborobakTest.Sales").join(names, "customer_id").groupBy("who").count()
    assert sorted((r.who, r["count"]) for r in df.collect()) == [("Ana", 2), ("Bo", 2)]


def test_write_a_created_dataframe(session, raw_connection):
    cur = raw_connection.cursor()
    try:
        cur.execute("DROP TABLE MborobakTest.FromLocal")
    except Exception:  # noqa: BLE001
        pass
    try:
        session.createDataFrame([(1, "a"), (2, "b")], "id int, s string").write.saveAsTable("MborobakTest.FromLocal")
        assert session.table("MborobakTest.FromLocal").count() == 2
    finally:
        cur.execute("DROP TABLE MborobakTest.FromLocal")


def test_values_iris_can_not_hold(session):
    # decimal(38,18), PySpark's type for Decimal values, holds at most 19 digits before the point in IRIS
    with pytest.raises(MborobakExecutionError, match="does not fit"):
        session.createDataFrame([(decimal.Decimal("1" * 20),)])


def test_pandas(session):
    pd = pytest.importorskip("pandas")
    df = session.createDataFrame(pd.DataFrame({"k": [1, 2], "s": ["x", None]}))
    assert df.filter(F.col("s").isNull()).select("k").collect()[0].k == 2


@pytest.mark.skipif("CI" in os.environ, reason="speed check")
def test_ten_thousand_rows_load_quickly(session):
    import time

    t0 = time.perf_counter()
    session.createDataFrame([(i, i * 0.5, f"row {i}") for i in range(10_000)], "i int, d double, s string")
    assert time.perf_counter() - t0 < 5
