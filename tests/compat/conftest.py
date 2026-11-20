"""Differential tests: the same DataFrame code on Mborobak (IRIS) and PySpark 4.3 (compatibility matrix §3).

Needs IRIS_* environment variables, PySpark and a Java runtime (JAVA_HOME, or Homebrew's openjdk@21).
"""

from __future__ import annotations

import datetime
import decimal
import math
import os
import shutil
from dataclasses import dataclass

import pytest

SCHEMA = "MborobakCompat"
D = decimal.Decimal
NAN, INF = float("nan"), float("inf")

# name: (IRIS DDL columns, Spark schema, rows)
DATASETS: dict[str, tuple[str, str, list[tuple]]] = {
    "strings": (
        "id INT, s VARCHAR(20)",
        "id int, s string",
        [(1, "apple"), (2, "Apple"), (3, "APPLE"), (4, "banana"), (5, "apple "), (6, " apple"), (7, "Banana"),
         (8, ""), (9, None)],
    ),
    "nums": (
        "id INT, i INT, l BIGINT, d DOUBLE, n NUMERIC(10,2), n4 NUMERIC(12,4)",
        "id int, i int, l bigint, d double, n decimal(10,2), n4 decimal(12,4)",
        [
            (1, 0, 0, 0.0, D("0.00"), D("1.5000")),
            (2, 1, 10, -0.0, D("1.25"), D("-0.1000")),
            (3, -1, -10, 2.5, D("-1.25"), D("100.0000")),
            (4, 7, 2**40, NAN, D("99999999.99"), None),
            (5, -7, -(2**40), INF, None, D("0.0001")),
            (6, 2147483647, 9223372036854775807, -INF, D("10.10"), D("2.0000")),
            (7, -2147483648, None, -1e19, D("20.20"), D("3.0000")),
            (8, None, 5, 1e300, D("30.30"), D("4.0000")),
            (9, 3, 6, None, D("40.40"), D("5.0000")),
            (10, 3, 7, 1.0, D("50.50"), D("6.0000")),
        ],
    ),
    "texts": (
        "id INT, s VARCHAR(30)",
        "id int, s string",
        [(1, "1"), (2, "-2"), (3, "+3"), (4, "007"), (5, " 1"), (6, "1 "), (7, "1.5"), (8, "1e3"), (9, ""),
         (10, "abc"), (11, "1.") , (12, ".5"), (13, "NaN"), (14, "Infinity"), (15, "2.7"), (16, "-2.7"),
         (17, "１"), (18, None), (19, "12345678901234567890"), (20, "inf"), (21, " -INFINITY "), (22, "+Inf"),
         (23, "nan")],
    ),
    "left": (
        "id INT, k INT, name VARCHAR(10)",
        "id int, k int, name string",
        [(1, 1, "a"), (2, 1, "b"), (3, 2, "c"), (4, None, "d"), (5, 9, "e")],
    ),
    "right": (
        "id INT, k INT, label VARCHAR(10)",
        "id int, k int, label string",
        [(10, 1, "x"), (11, 2, "y"), (12, 2, "Y"), (13, None, "z"), (14, 7, "w")],
    ),
    "funcs": (
        "id INT, s VARCHAR(30), t VARCHAR(30), d DOUBLE, n NUMERIC(10,3), i INT, l BIGINT",
        "id int, s string, t string, d double, n decimal(10,3), i int, l bigint",
        [
            (1, "Hello World", "hello", 2.5, D("2.345"), 7, 10),
            (2, "", "x", -2.5, D("-2.500"), -7, -10),
            (3, None, None, None, None, None, None),
            (4, "abc", "ABC", NAN, D("1.005"), 0, 0),
            (5, "  pad  ", "a-b c", INF, D("0.285"), 3, 2**40),
            (6, "hello world-foo bar", "aBc", -INF, D("99.500"), 3, -3),
            (7, "ab", "b", 0.285, D("-0.500"), -1, 5),
            (8, "çÃo Ñ", "É", 1.005, D("0.125"), 2, 7),
            (9, "x", "", 1e300, D("1234567.999"), 100, 1),
            (10, "tab\there", None, -1e-300, D("0.000"), -100, -1),
            (11, "12345", "5", 0.0, D("2.500"), 5, 2),
            (12, "Hi", "i", 0.125, D("-1.250"), None, None),
            (13, "10", "9", 1.5, D("3.000"), 4, 3),
            (14, "-1", "+1", 2.0, D("4.000"), 6, 4),
            (15, "0.5", ".5", 3.0, D("5.000"), 8, 5),
        ],
    ),
    "wins": (
        "id INT, g VARCHAR(10), k INT, d DOUBLE, n NUMERIC(10,2), x INT",
        "id int, g string, k int, d double, n decimal(10,2), x int",
        [
            (1, "a", 1, 1.0, D("1.50"), 10),
            (2, "a", 2, 2.0, D("2.25"), None),
            (3, "a", 2, NAN, D("-1.00"), 30),
            (4, "a", None, -1e19, D("4.00"), 40),
            (5, "A", 1, None, D("5.50"), 50),
            (6, "b", 3, -INF, None, 60),
            (7, "b", 3, INF, D("7.75"), None),
            (8, "10", 4, 0.0, D("8.00"), 80),
            (9, "9", 5, -0.0, D("9.25"), 90),
            (10, "b", 1, 2.5, D("1.00"), 15),
            (11, None, 2, 3.5, D("2.00"), 25),
            (12, None, None, 1e300, D("3.00"), 35),
        ],
    ),
    "times": (
        "id INT, dt DATE, ts TIMESTAMP",
        "id int, dt date, ts timestamp_ntz",
        [(1, datetime.date(2026, 1, 31), datetime.datetime(2026, 1, 31, 23, 59, 59, 999999)),
         (2, datetime.date(2026, 2, 28), datetime.datetime(2026, 2, 28, 0, 0, 0, 1)),
         (3, None, None)],
    ),
}


def _java_home() -> str | None:
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for candidate in ("/opt/homebrew/opt/openjdk@21", "/opt/homebrew/opt/openjdk@17", "/opt/homebrew/opt/openjdk"):
        if os.path.exists(os.path.join(candidate, "bin", "java")):
            return candidate
    return None if shutil.which("java") is None else ""


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    java_home = _java_home()
    if java_home is None:
        pytest.skip("no Java runtime for PySpark")
    if java_home:
        os.environ["JAVA_HOME"] = java_home
    import sys

    os.environ["PYSPARK_PYTHON"] = sys.executable  # Python workers must use this interpreter too
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("mborobak-compat")
        .config("spark.sql.ansi.enabled", "true")  # D9
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def iris_conn():
    keys = ("IRIS_HOST", "IRIS_PORT", "IRIS_NAMESPACE", "IRIS_USERNAME", "IRIS_PASSWORD")
    if not all(k in os.environ for k in keys):
        pytest.skip("IRIS_* environment variables are not set")
    import iris

    conn = iris.connect(os.environ["IRIS_HOST"], int(os.environ["IRIS_PORT"]), os.environ["IRIS_NAMESPACE"],
                        os.environ["IRIS_USERNAME"], os.environ["IRIS_PASSWORD"])
    cur = conn.cursor()

    def drop_all():
        for name in DATASETS:
            try:
                cur.execute(f"DROP TABLE {SCHEMA}.Ds{name.capitalize()}")
            except Exception:  # noqa: BLE001 - may not exist
                pass

    drop_all()
    for name, (ddl, _, rows) in DATASETS.items():
        cur.execute(f"CREATE TABLE {SCHEMA}.Ds{name.capitalize()} ({ddl})")
        marks = ", ".join("?" * len(rows[0]))
        cur.executemany(f"INSERT INTO {SCHEMA}.Ds{name.capitalize()} VALUES ({marks})", [list(r) for r in rows])
    yield conn
    drop_all()
    conn.close()


@dataclass
class Pair:
    mb_session: object
    spark: object

    def mb(self, name: str):
        return self.mb_session.table(f"{SCHEMA}.Ds{name.capitalize()}")

    def sp(self, name: str):
        _, schema, rows = DATASETS[name]
        return self.spark.createDataFrame(rows, schema)

    def both(self, name: str, build):
        from mborobak.sql import functions as MF
        from pyspark.sql import functions as SF

        return build(MF, self.mb(name)), build(SF, self.sp(name))


@pytest.fixture()
def pair(spark, iris_conn):
    from mborobak import MborobakSession

    s = MborobakSession.builder.connection(iris_conn).create()
    yield Pair(s, spark)
    s.stop()


# --------------------------------------------------------------------------- comparison


def _norm(v):
    if isinstance(v, float):
        if math.isnan(v):
            return ("nan",)
        return 0.0 if v == 0 else v
    return v


def _values_equal(a, b) -> bool:
    a, b = _norm(a), _norm(b)
    if isinstance(a, float) and isinstance(b, float):
        if math.isinf(a) or math.isinf(b):
            return a == b
        return math.isclose(a, b, rel_tol=1e-12, abs_tol=0.0)
    return type(a) is type(b) and a == b


def _sort_key(row):
    return tuple((v is not None, repr(_norm(v))) for v in row)


def assert_same(mb_df, sp_df, ordered: bool = False, check_types: bool = True):
    """Same column names, same types, same rows (as multisets unless ``ordered``)."""
    assert mb_df.columns == sp_df.columns, f"names: {mb_df.columns} vs {sp_df.columns}"
    if check_types:
        assert mb_df.dtypes == sp_df.dtypes, f"types: {mb_df.dtypes} vs {sp_df.dtypes}"
    mb_rows = [tuple(r) for r in mb_df.collect()]
    sp_rows = [tuple(r) for r in sp_df.collect()]
    if not ordered:
        mb_rows, sp_rows = sorted(mb_rows, key=_sort_key), sorted(sp_rows, key=_sort_key)
    assert len(mb_rows) == len(sp_rows), f"row count: {len(mb_rows)} vs {len(sp_rows)}\n{mb_rows}\n{sp_rows}"
    for m, s in zip(mb_rows, sp_rows):
        assert len(m) == len(s) and all(_values_equal(x, y) for x, y in zip(m, s)), \
            f"rows differ:\n mborobak {m}\n pyspark  {s}\nall mborobak: {mb_rows}\nall pyspark:  {sp_rows}"


def assert_both_fail(mb_df, sp_df, mb_error):
    with pytest.raises(mb_error):
        mb_df.collect()
    with pytest.raises(Exception):  # noqa: B017 - PySpark's error classes vary by version
        sp_df.collect()
