"""df.write against a live IRIS: what PySpark can't be compared on (transactions, staging, IRIS limits)."""

from __future__ import annotations

import pytest

from mborobak.errors import AnalysisError, MborobakArithmeticError, MborobakExecutionError
from mborobak.sql import functions as F

SCHEMA = "MborobakTest"  # as in conftest.py

pytestmark = pytest.mark.iris

OUT = f"{SCHEMA}.WriteOut"


@pytest.fixture()
def out(session, raw_connection):
    cur = raw_connection.cursor()

    def drop():
        try:
            cur.execute(f"DROP TABLE {OUT}")
        except Exception:  # noqa: BLE001 - may not exist
            pass

    drop()
    yield OUT
    drop()


def classes(raw_connection) -> list[tuple]:
    cur = raw_connection.cursor()
    cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME",
                [SCHEMA])
    return [r[0] for r in cur.fetchall()]


def test_save_and_read_back(session, out):
    sales = session.table(f"{SCHEMA}.Sales")
    sales.groupBy("customer_id").agg(F.sum("amount").alias("total")).write.saveAsTable(out)
    m = session.lastExecution
    assert m.action == "saveAsTable" and m.rows == 5 and m.data_queries == 2
    df = session.table(out)
    assert df.dtypes == [("customer_id", "int"), ("total", "decimal(20,2)")]
    assert {r.customer_id: r.total for r in df.collect()}[10] == sales.filter("customer_id = 10") \
        .agg(F.sum("amount")).first()[0]


def test_overwrite_a_table_the_dataframe_reads(session, out, raw_connection):
    session.table(f"{SCHEMA}.Sales").select("id", "amount").write.saveAsTable(out)
    before = classes(raw_connection)
    for _ in range(2):  # twice: the second staging table must not clash with the first one's class
        df = session.table(out)
        df.filter("id > 2").withColumn("double", F.col("amount") * 2).write.mode("overwrite").saveAsTable(out)
    result = session.table(out)
    assert result.columns == ["id", "amount", "double"]  # withColumn replaced "double" the second time
    assert sorted(r.id for r in result.collect()) == [3, 4, 5, 6, 7, 8, 9]
    assert classes(raw_connection) == before  # no staging table is left


def test_overwrite_twice_with_new_columns(session, out):
    session.table(f"{SCHEMA}.Sales").select("id").write.saveAsTable(out)
    session.table(f"{SCHEMA}.Sales").select("id", "status").write.mode("overwrite").saveAsTable(out)
    assert session.table(out).columns == ["id", "status"]  # the catalog cache was refreshed


def test_a_failed_overwrite_keeps_the_old_table(session, out):
    # DROP, CREATE and a failing INSERT would lose the old rows on ROLLBACK (compiler spec §9.7)
    session.table(f"{SCHEMA}.Sales").select("id", "status").write.saveAsTable(out)
    bad = session.table(f"{SCHEMA}.Sales").select("id", (F.col("id") / (F.col("id") - 3)).alias("q"))
    with pytest.raises(MborobakArithmeticError, match="division by zero"):
        bad.write.mode("overwrite").saveAsTable(out)
    kept = session.table(out)
    assert kept.columns == ["id", "status"] and kept.count() == 9


def test_a_failed_create_leaves_no_table(session, out):
    bad = session.table(f"{SCHEMA}.Sales").select((F.col("id") / (F.col("id") - 3)).alias("q"))
    with pytest.raises(MborobakArithmeticError):
        bad.write.saveAsTable(out)
    assert not session.catalog.tableExists(out)


def test_a_failed_append_adds_nothing(session, out):
    sales = session.table(f"{SCHEMA}.Sales")
    sales.select("id").write.saveAsTable(out)
    with pytest.raises(MborobakArithmeticError):
        sales.select((F.col("id") / (F.col("id") - 3)).cast("int").alias("id")).write.mode("append") \
            .saveAsTable(out)
    assert session.table(out).count() == 9


def test_string_length_setting(session, out):
    session.conf.set("mborobak.write.stringLength", 4)
    with pytest.raises(MborobakExecutionError, match="longer than the column's length"):
        session.table(f"{SCHEMA}.Sales").select("status").write.saveAsTable(out)
    assert not session.catalog.tableExists(out)
    session.table(f"{SCHEMA}.Sales").select(F.substring("status", 1, 4).alias("s")).write.saveAsTable(out)
    assert session.table(out).count() == 9


def test_sql_source_with_cte(session, out):
    df = session.sql(f"WITH q AS (SELECT id, status FROM {SCHEMA}.Sales WHERE id > ?) SELECT * FROM q", [6])
    df.write.saveAsTable(out)
    assert sorted(r.id for r in session.table(out).collect()) == [7, 8, 9]


def test_insert_into_a_table_with_a_not_null_column(session, raw_connection, out):
    raw_connection.cursor().execute(f"CREATE TABLE {OUT} (id INT NOT NULL, s VARCHAR(10))")
    df = session.table(f"{SCHEMA}.Sales").select(F.col("customer_id"), F.col("status"))
    with pytest.raises(MborobakExecutionError, match="NOT NULL"):
        df.write.insertInto(out)
    assert session.table(out).count() == 0
    with pytest.raises(AnalysisError, match="already exists"):
        df.write.saveAsTable(out.lower())
