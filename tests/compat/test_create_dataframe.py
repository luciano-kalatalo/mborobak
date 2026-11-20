"""Differential tests for createDataFrame (compatibility matrix §2.17, rows CDF-xx).

The same local data goes to ``createDataFrame`` on both sides; names, types and rows are compared.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

from .conftest import assert_same

pytestmark = pytest.mark.compat

D = decimal.Decimal
NAN, INF = float("nan"), float("inf")


def known_difference(row_id: str):
    return pytest.mark.xfail(strict=True, reason=f"{row_id}: known difference in the compatibility matrix")


def both(pair, data, schema=None, **kw):
    mb = pair.mb_session.createDataFrame(data, schema, **kw)
    sp = pair.spark.createDataFrame(data, schema, **kw)
    return mb, sp


def same(pair, data, schema=None, check_types=True):
    assert_same(*both(pair, data, schema), check_types=check_types)


def test_CDF_01_inferred_types(pair):
    same(pair, [(1, "a", 1.5, True, datetime.date(2026, 2, 28), D("1.25")),
                (-2**40, "", NAN, False, None, None),
                (None, None, -0.0, None, datetime.date(1999, 12, 31), D("-0.5")),
                (7, "Ab ", INF, True, None, D("99999999.123456789"))])


def test_CDF_02_rows_dicts_lists_and_names(pair):
    from mborobak import Row as MRow
    from pyspark.sql import Row as SRow

    same(pair, [{"b": 1, "a": "x"}, {"a": "y", "b": None}])  # dict keys sorted
    same(pair, [[1, "a"], [2, "b"]], ["k", "v"])
    same(pair, [(1, 2, 3)], ["x"])  # the other columns keep _2, _3
    assert_same(pair.mb_session.createDataFrame([MRow(b=1, a="x")]), pair.spark.createDataFrame([SRow(b=1, a="x")]))


def test_CDF_03_explicit_schemas_and_conversions(pair):
    rows = [(1, 2**40, 1.5, D("1.25"), "s", True, datetime.date(2026, 1, 1),
             datetime.datetime(2026, 1, 1, 1, 2, 3, 456789)),
            (None, None, None, None, None, None, None, None)]
    ddl = "i int, l bigint, d double, n decimal(4,1), s string, b boolean, dt date, ts timestamp_ntz"
    same(pair, rows, ddl)
    same(pair, [(1, 1.5, True, datetime.date(2026, 1, 2), 1e20, D("2.50"))],
         "a string, b string, c string, d string, e string, f string")  # anything into a string
    same(pair, [(datetime.datetime(2026, 1, 1, 23, 59),)], "d date")
    same(pair, [(1,), (2,)], "x: long")
    same(pair, [1, 2, None], "int")  # atomic type: one column named value


@pytest.mark.parametrize("data, schema", [
    ([(1,)], "x double"), ([(1.5,)], "x int"), (["1"], "int"), ([(True,)], "x int"), ([(2**40,)], "x int"),
    ([(1,)], "x decimal(4,1)"), ([(D("12345.5"),)], "x decimal(4,1)"), ([(datetime.date(2026, 1, 1),)], "x timestamp_ntz"),
    ([(1, 2)], "x int"), ([], None), ([(None,)], None), ([(1,), (2.5,)], None),
])
def test_CDF_04_rejected_input(pair, data, schema):
    with pytest.raises((TypeError, ValueError)):
        pair.mb_session.createDataFrame(data, schema)
    with pytest.raises(Exception):  # noqa: B017 - PySpark's error classes
        pair.spark.createDataFrame(data, schema).collect()


def test_CDF_04_rejected_none_in_a_not_nullable_field(pair):
    from mborobak.sql.types import IntegerType as MInt, StructField as MField, StructType as MStruct
    from pyspark.sql.types import IntegerType as SInt, StructField as SField, StructType as SStruct

    with pytest.raises(ValueError):
        pair.mb_session.createDataFrame([(None,)], MStruct([MField("x", MInt(), False)]))
    with pytest.raises(Exception):  # noqa: B017
        pair.spark.createDataFrame([(None,)], SStruct([SField("x", SInt(), False)]))


def test_CDF_05_pandas(pair):
    pd = pytest.importorskip("pandas")
    np = pytest.importorskip("numpy")
    pdf = pd.DataFrame({"i": [1, 2, 3], "i32": np.array([1, 2, 3], dtype="int32"), "f": [1.5, np.nan, -np.inf],
                        "b": [True, False, True], "s": ["a", "B", "c"], "gap": [1, None, 3]})
    same(pair, pdf)
    same(pair, pdf[["i", "s"]], "a long, b string")
    pdt = pd.DataFrame({"t": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-02 03:04:05.123456"], format="mixed")})
    same(pair, pdt, check_types=False)  # timestamp vs timestamp_ntz: CDF-07


def test_CDF_06_used_with_tables(pair):
    lookup = [("a", "first"), ("A", "upper"), ("apple ", "space")]
    mb, sp = both(pair, lookup, "s string, label string")
    assert_same(pair.mb("strings").join(mb, "s"), pair.sp("strings").join(sp, "s"))  # strings compare exactly
    assert_same(mb.groupBy("label").count(), sp.groupBy("label").count())
    assert_same(mb.filter(mb.s > "Z"), sp.filter(sp.s > "Z"))


@known_difference("CDF-07")
def test_CDF_07_inferred_datetimes_are_timestamp_ntz(pair):
    same(pair, [(datetime.datetime(2026, 1, 1, 1, 2, 3),)])


@known_difference("CDF-08")
def test_CDF_08_missing_pandas_strings(pair):
    pd = pytest.importorskip("pandas")
    same(pair, pd.DataFrame({"s": ["a", None]}))  # PySpark 4.3 with pandas 3 gives 'nan'; Mborobak NULL


def test_CDF_09_names_iris_tables_can_not_hold(pair):
    same(pair, [(1, 2, 3)], ["a.b", "c,d", "A"])
    same(pair, [(1, 2)], ["a", "A"])
