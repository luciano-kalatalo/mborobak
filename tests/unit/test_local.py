"""createDataFrame (local.py) without IRIS: schemas, inference and value checks.

The rules are PySpark 4.3's, probed 2026-11-20; tests/compat/test_create_dataframe.py compares results.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

from mborobak import MborobakSession, Row
from mborobak.catalog import StaticCatalog
from mborobak.errors import MborobakConnectionError, UnsupportedTypeError
from mborobak.local import convert, infer_type, merge, parse_schema, rows_of
from mborobak.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

D = decimal.Decimal


def test_schema_forms():
    assert parse_schema(None) == (None, None, None)
    assert parse_schema(["a", "b"]) == (["a", "b"], None, None)
    assert parse_schema("a int, b string") == (["a", "b"], [IntegerType(), StringType()], [True, True])
    assert parse_schema("a: decimal(10, 2), `b c`: long NOT NULL") == \
        (["a", "b c"], [DecimalType(10, 2), LongType()], [True, False])
    assert parse_schema("int") == (["value"], [IntegerType()], [True])
    st = StructType([StructField("x", DoubleType(), False)])
    assert parse_schema(st) == (["x"], [DoubleType()], [False])
    with pytest.raises(TypeError):
        parse_schema(42)


@pytest.mark.parametrize("value, dtype", [
    (True, BooleanType()), (1, LongType()), (1.5, DoubleType()), ("a", StringType()),
    (D("1.25"), DecimalType(38, 18)), (datetime.date(2026, 1, 1), DateType()),
    (datetime.datetime(2026, 1, 1), TimestampNTZType()), (None, NullType()),
])
def test_inference(value, dtype):
    assert infer_type(value) == dtype


def test_inference_errors():
    with pytest.raises(UnsupportedTypeError):
        infer_type([1, 2])
    with pytest.raises(TypeError, match="can't merge"):
        merge(LongType(), DoubleType(), "x")
    assert merge(NullType(), LongType(), "x") == LongType()


def test_rows_of():
    assert rows_of([{"b": 1, "a": "x"}, {"a": "y"}]) == (["a", "b"], [("x", 1), ("y", None)], None, True)
    assert rows_of([Row(b=1, a="x")]) == (["b", "a"], [(1, "x")], None, False)
    assert rows_of([[1, "a"]]) == (None, [(1, "a")], None, False)
    assert rows_of([1, 2]) == (None, [(1,), (2,)], None, False)
    with pytest.raises(TypeError):
        rows_of("abc")


@pytest.mark.parametrize("value, dtype, expected", [
    (1, IntegerType(), 1), (2**40, LongType(), 2**40), (1.5, DoubleType(), 1.5),
    (D("1.25"), DecimalType(4, 1), D("1.3")),  # HALF_UP, as PySpark
    (D("1" * 20), DecimalType(38, 18), D("1" * 20 + "." + "0" * 18)),
    (1, StringType(), "1"), (1.5, StringType(), "1.5"), (True, StringType(), "true"),
    (1e20, StringType(), "1e+20"), (datetime.date(2026, 1, 2), StringType(), "2026-01-02"),
    (datetime.datetime(2026, 1, 1, 5), DateType(), datetime.date(2026, 1, 1)),
    (datetime.datetime(2026, 1, 1, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=-3))), TimestampNTZType(),
     datetime.datetime(2026, 1, 1, 6)),
    (None, IntegerType(), None), (float("nan"), DoubleType(), float("nan")),
])
def test_accepted_values(value, dtype, expected):
    got = convert(value, dtype, "x", True)
    assert got == expected or (expected != expected and got != got)


@pytest.mark.parametrize("value, dtype, error", [
    (1, DoubleType(), TypeError), (1.5, IntegerType(), TypeError), ("1", IntegerType(), TypeError),
    (True, IntegerType(), TypeError), (2**40, IntegerType(), ValueError), (1, DecimalType(4, 1), TypeError),
    (1.5, DecimalType(4, 1), TypeError), (D("12345.5"), DecimalType(4, 1), ValueError),
    (datetime.date(2026, 1, 1), TimestampNTZType(), TypeError), (1, BooleanType(), TypeError),
    (float("nan"), IntegerType(), TypeError),
])
def test_rejected_values(value, dtype, error):
    with pytest.raises(error):
        convert(value, dtype, "x", True)


def test_not_nullable():
    with pytest.raises(ValueError, match="not nullable"):
        convert(None, IntegerType(), "x", False)


def test_needs_a_connection():
    s = MborobakSession._for_tests(StaticCatalog({}))
    with pytest.raises(MborobakConnectionError, match="needs a connection"):
        s.createDataFrame([(1,)], "x int")
    with pytest.raises(ValueError, match="only None"):
        s.createDataFrame([(None,)])
    with pytest.raises(ValueError, match="empty"):
        s.createDataFrame([])
    with pytest.raises(ValueError, match="2 values"):
        s.createDataFrame([(1, 2)], "x int")
