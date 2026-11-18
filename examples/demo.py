"""Mborobak demo: PySpark-style DataFrames that run as one IRIS SQL statement.

Run against any IRIS instance (see the README for the Docker setup):

    export IRIS_HOST=localhost IRIS_PORT=1972 IRIS_NAMESPACE=USER IRIS_USERNAME=... IRIS_PASSWORD=...
    python examples/demo.py

It creates two small tables in the MborobakDemo schema, walks through the main features, and drops
the tables at the end.
"""

from __future__ import annotations

import datetime
import decimal
import os

import iris

from mborobak import MborobakSession
from mborobak.errors import AmbiguousColumnError, MborobakCastError
from mborobak.sql import functions as F

SCHEMA = "MborobakDemo"
D = decimal.Decimal


def heading(text: str) -> None:
    print(f"\n{'=' * 78}\n{text}\n{'=' * 78}")


def create_tables(cur) -> None:
    for table in ("Sales", "Customers"):
        try:
            cur.execute(f"DROP TABLE {SCHEMA}.{table}")
        except Exception:  # noqa: BLE001 - first run
            pass
    cur.execute(f"CREATE TABLE {SCHEMA}.Customers (id INT PRIMARY KEY, name VARCHAR(20), country VARCHAR(4))")
    cur.executemany(f"INSERT INTO {SCHEMA}.Customers VALUES (?, ?, ?)",
                    [[1, "Ana", "BR"], [2, "Bruno", "BR"], [3, "Carla", "us"], [4, "Dev", "US"], [5, "Eve", "DE"]])
    cur.execute(f"CREATE TABLE {SCHEMA}.Sales (id INT PRIMARY KEY, customer_id INT, status VARCHAR(10), "
                "amount NUMERIC(10,2), score DOUBLE, day DATE, qty_text VARCHAR(10))")
    rows = [
        [1, 1, "PAID", D("120.50"), 1.5, datetime.date(2026, 11, 1), "3"],
        [2, 1, "paid", D("80.00"), None, datetime.date(2026, 11, 2), " 7 "],
        [3, 2, "PAID", D("300.00"), -1e19, datetime.date(2026, 11, 3), "1"],
        [4, 3, "PAID", D("45.25"), float("nan"), datetime.date(2026, 11, 3), "2"],
        [5, 4, "OPEN", D("99.99"), float("-inf"), datetime.date(2026, 11, 4), "5"],
        [6, 4, "PAID", D("10.00"), 2.5, datetime.date(2026, 11, 5), "1.5"],
        [7, None, "PAID", D("5.00"), 0.0, datetime.date(2026, 11, 6), "4"],
    ]
    cur.executemany(f"INSERT INTO {SCHEMA}.Sales VALUES (?, ?, ?, ?, ?, ?, ?)", rows)


def main() -> None:
    conn = iris.connect(os.environ["IRIS_HOST"], int(os.environ.get("IRIS_PORT", 1972)),
                        os.environ.get("IRIS_NAMESPACE", "USER"), os.environ["IRIS_USERNAME"],
                        os.environ["IRIS_PASSWORD"])
    create_tables(conn.cursor())
    session = MborobakSession.builder.connection(conn).getOrCreate()
    sales = session.table(f"{SCHEMA}.Sales")
    customers = session.table(f"{SCHEMA}.Customers")

    heading("1. Lazy pipeline: nothing runs until an action")
    revenue = (
        sales.filter(F.col("status") == "PAID")
        .groupBy("customer_id")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
        .orderBy(F.col("revenue").desc())
    )
    print(revenue)  # schema only: no query has run
    revenue.explain()  # plan, IRIS translations, SQL and parameters

    heading("2. One action, one IRIS query")
    revenue.show()
    m = session.lastExecution
    print(f"data queries: {m.data_queries}, rows: {m.rows}, Python side: {m.python_ms:.2f} ms, "
          f"IRIS + fetch: {m.server_ms + m.fetch_ms:.1f} ms")

    heading("3. PySpark semantics on IRIS: 'PAID' and 'paid' are different values")
    sales.groupBy("status").count().orderBy("status").show()
    session.conf.set("mborobak.sql.stringSemantics", "iris")
    print("Same query with IRIS's case-insensitive rules (mborobak.sql.stringSemantics = 'iris'):")
    sales.groupBy("status").count().orderBy("status").show()
    session.conf.set("mborobak.sql.stringSemantics", "pyspark")

    heading("4. Joins, aliases and union")
    by_country = (
        sales.join(customers, sales["customer_id"] == customers["id"], "left")
        .groupBy(customers["country"])
        .agg(F.sum("amount").alias("revenue"))
        .orderBy("country")
    )
    by_country.show()
    a, b = customers.alias("a"), customers.alias("b")
    a.join(b, (F.col("a.country") == F.col("b.country")) & (F.col("a.id") < F.col("b.id"))) \
        .select(F.col("a.name").alias("customer"), F.col("b.name").alias("same_country_as")).show()
    sales.select("id").union(customers.select("id")).distinct().orderBy(F.col("id").desc()).show(3)

    heading("5. Edge cases handled like Spark: NaN, infinity, NULL and -1e19 in a DOUBLE sort")
    sales.select("id", "score").orderBy("score").show()

    heading("6. Casts and errors with Spark's ANSI rules")
    sales.filter(F.col("id") <= 3).select("id", "qty_text", F.col("qty_text").cast("int").alias("qty")).show()
    try:
        sales.select(F.col("qty_text").cast("int")).collect()  # row 6 holds '1.5'
    except MborobakCastError as exc:
        print(f"MborobakCastError: {exc}")
    try:
        sales.join(customers, sales["customer_id"] == customers["id"]).select("id")
    except AmbiguousColumnError as exc:
        print(f"AmbiguousColumnError raised by select(), before any query:\n{exc}")

    heading("7. IRIS SQL in, DataFrame out")
    top = session.sql(f"SELECT customer_id, SUM(amount) AS total FROM {SCHEMA}.Sales GROUP BY customer_id")
    top.filter(F.col("total") > 50).orderBy(F.col("total").desc()).show()

    heading("8. Results as pandas, with the schema's types")
    try:
        pdf = sales.select("id", "amount", "day").toPandas()
        print(pdf)
        print(pdf.dtypes.to_string())
    except ImportError:
        print("pandas is not installed: pip install mborobak[pandas]")

    for table in ("Sales", "Customers"):
        conn.cursor().execute(f"DROP TABLE {SCHEMA}.{table}")
    session.stop()
    conn.close()


if __name__ == "__main__":
    main()
