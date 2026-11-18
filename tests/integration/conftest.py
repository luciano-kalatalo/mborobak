"""Fixtures for tests against a live IRIS (connection from IRIS_* environment variables)."""

from __future__ import annotations

import datetime
import decimal
import os

import pytest

SCHEMA = "MborobakTest"

SALES = [
    # id, customer_id, status, amount, score, day
    (1, 10, "PAID", decimal.Decimal("100.50"), 1.5, datetime.date(2026, 1, 1)),
    (2, 10, "paid", decimal.Decimal("20.00"), None, datetime.date(2026, 1, 2)),
    (3, 20, "PAID", decimal.Decimal("300.00"), -1e19, datetime.date(2026, 1, 3)),
    (4, 20, "PAID ", decimal.Decimal("5.25"), float("nan"), None),
    (5, 30, "Pending", decimal.Decimal("70.00"), float("-inf"), datetime.date(2026, 1, 5)),
    (6, 30, "PAID", None, 2.5, datetime.date(2026, 1, 6)),
    (7, None, "PAID", decimal.Decimal("1.00"), 0.0, datetime.date(2026, 1, 7)),
    (8, 40, "Pending", decimal.Decimal("8.00"), float("inf"), datetime.date(2026, 1, 8)),
    (9, 40, None, decimal.Decimal("9.00"), -3.0, datetime.date(2026, 1, 9)),
]

WORDS = [(1, "apple"), (2, "Apple"), (3, "APPLE"), (4, "banana"), (5, "apple "), (6, " apple"), (7, "Banana"),
         (8, "12"), (9, " 7 "), (10, "1.5"), (11, "abc"), (12, None)]


def _env() -> dict[str, str] | None:
    keys = ("IRIS_HOST", "IRIS_PORT", "IRIS_NAMESPACE", "IRIS_USERNAME", "IRIS_PASSWORD")
    if not all(k in os.environ for k in keys):
        return None
    return {k: os.environ[k] for k in keys}


@pytest.fixture(scope="session")
def iris_env():
    env = _env()
    if env is None:
        pytest.skip("IRIS_* environment variables are not set")
    return env


@pytest.fixture(scope="session")
def raw_connection(iris_env):
    import iris

    conn = iris.connect(iris_env["IRIS_HOST"], int(iris_env["IRIS_PORT"]), iris_env["IRIS_NAMESPACE"],
                        iris_env["IRIS_USERNAME"], iris_env["IRIS_PASSWORD"])
    cur = conn.cursor()
    for t in ("Sales", "Words"):
        try:
            cur.execute(f"DROP TABLE {SCHEMA}.{t}")
        except Exception:  # noqa: BLE001 - table may not exist
            pass
    cur.execute(f"CREATE TABLE {SCHEMA}.Sales (id INT NOT NULL, customer_id INT, status VARCHAR(20), "
                "amount NUMERIC(10,2), score DOUBLE, day DATE)")
    cur.executemany(f"INSERT INTO {SCHEMA}.Sales VALUES (?,?,?,?,?,?)", [list(r) for r in SALES])
    cur.execute(f"CREATE TABLE {SCHEMA}.Words (id INT, w VARCHAR(20))")
    cur.executemany(f"INSERT INTO {SCHEMA}.Words VALUES (?,?)", [list(r) for r in WORDS])
    yield conn
    for t in ("Sales", "Words"):
        try:
            cur.execute(f"DROP TABLE {SCHEMA}.{t}")
        except Exception:  # noqa: BLE001
            pass
    conn.close()


@pytest.fixture()
def session(iris_env, raw_connection):
    from mborobak import MborobakSession

    s = MborobakSession.builder.connection(raw_connection).create()
    yield s
    s.stop()  # does not close a connection it was given
