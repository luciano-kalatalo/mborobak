"""Seeded, reproducible benchmark data (scope §8.3).

Two tables: ``Customers`` (n / 10 rows, at least 100) and ``Orders`` (n rows). ``skew`` is the
exponent of a Zipf-like distribution of orders over customers: 0 is uniform, 1.1 puts a large share
of orders on a few customers. Strings are mixed case on purpose, so case-sensitive (PySpark) and
case-insensitive (IRIS default) results differ, and %EXACT's cost shows up.
"""

from __future__ import annotations

import bisect
import datetime
import decimal
import itertools
import random
from typing import Iterator

COUNTRIES = ["BR", "US", "DE", "FR", "JP", "IN", "CN", "GB", "IT", "ES", "MX", "CA", "AR", "CL", "PT",
             "NL", "SE", "NO", "PL", "TR", "KR", "AU", "ZA", "NG", "EG", "CO", "PE", "AT", "CH", "BE"]
STATUSES = [("PAID", 50), ("Paid", 5), ("paid", 5), ("OPEN", 20), ("CANCELLED", 20)]

DDL = {
    "Customers": "id INT NOT NULL PRIMARY KEY, name VARCHAR(40), country VARCHAR(4)",
    "Orders": "id INT NOT NULL PRIMARY KEY, customer_id INT, status VARCHAR(12), amount NUMERIC(12,2), "
              "score DOUBLE, created DATE",
}
INDEXES = [("Orders", "OrdersStatus", "status"), ("Orders", "OrdersCustomer", "customer_id"),
           ("Customers", "CustomersCountry", "country")]


def customer_count(n_orders: int) -> int:
    return max(100, n_orders // 10)


def customers(n_orders: int, seed: int) -> Iterator[tuple]:
    rng = random.Random(f"{seed}-customers")
    for i in range(1, customer_count(n_orders) + 1):
        country = rng.choice(COUNTRIES)
        if rng.random() < 0.05:
            country = country.lower()  # a few lowercase codes: 'br' and 'BR' differ under PySpark rules
        yield (i, f"Customer {i}", country)


def orders(n_orders: int, seed: int, skew: float) -> Iterator[tuple]:
    rng = random.Random(f"{seed}-orders")
    n_customers = customer_count(n_orders)
    weights = [1.0 / (k ** skew) for k in range(1, n_customers + 1)]
    cumulative = list(itertools.accumulate(weights))
    total = cumulative[-1]
    status_values = [s for s, _ in STATUSES]
    status_cumulative = list(itertools.accumulate(w for _, w in STATUSES))
    start = datetime.date(2025, 1, 1)
    cent = decimal.Decimal("0.01")
    for i in range(1, n_orders + 1):
        customer = bisect.bisect_left(cumulative, rng.random() * total) + 1
        status = status_values[bisect.bisect_left(status_cumulative, rng.random() * status_cumulative[-1])]
        amount = decimal.Decimal(rng.uniform(1, 1000)).quantize(cent)
        score = rng.gauss(0, 100)
        created = start + datetime.timedelta(days=rng.randrange(0, 700))
        yield (i, customer, status, amount, score, created)


def load(cursor, schema: str, n_orders: int, seed: int, skew: float, batch: int = 10_000) -> dict:
    """(Re)create both tables, insert the rows, add indexes and tune the tables. Returns timings."""
    import time

    for table in ("Orders", "Customers"):
        try:
            cursor.execute(f"DROP TABLE {schema}.{table}")
        except Exception:  # noqa: BLE001 - may not exist yet
            pass
    for table, ddl in DDL.items():
        cursor.execute(f"CREATE TABLE {schema}.{table} ({ddl})")
    t0 = time.perf_counter()
    for table, rows in (("Customers", customers(n_orders, seed)), ("Orders", orders(n_orders, seed, skew))):
        marks = ", ".join("?" * (3 if table == "Customers" else 6))
        chunk: list[list] = []
        for row in rows:
            chunk.append(list(row))
            if len(chunk) == batch:
                cursor.executemany(f"INSERT INTO {schema}.{table} VALUES ({marks})", chunk)
                chunk = []
        if chunk:
            cursor.executemany(f"INSERT INTO {schema}.{table} VALUES ({marks})", chunk)
    t1 = time.perf_counter()
    for table, name, column in INDEXES:
        cursor.execute(f"CREATE INDEX {name} ON {schema}.{table} ({column})")
    for table in ("Customers", "Orders"):
        cursor.execute(f"TUNE TABLE {schema}.{table}")
    t2 = time.perf_counter()
    return {"insert_s": round(t1 - t0, 2), "index_and_tune_s": round(t2 - t1, 2)}
