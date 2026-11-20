"""Result conversion fast paths (compiler spec §9.9): which columns are plain, and what conversion skips."""

from __future__ import annotations

import decimal

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.execution import convert_columns
from mborobak.sql import functions as F
from mborobak.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

D = decimal.Decimal


@pytest.fixture()
def session():
    return MborobakSession._for_tests(StaticCatalog({
        "Sales": [("id", IntegerType()), ("amount", DecimalType(10, 2)), ("s", StringType())],
        "Other": [("id", IntegerType()), ("amount", DecimalType(10, 2))],
    }))


def plain(df):
    return df._session._compile(df._plan).plain


def test_plain_columns(session):
    s, o = session.table("Sales"), session.table("Other")
    assert plain(s) == (True, True, True)
    assert plain(s.filter(F.col("id") > 1).select("amount", F.col("s").alias("t")).orderBy("t").limit(5)) == \
        (True, True)
    assert plain(s.select(F.col("amount") * 2, F.col("amount").cast("decimal(12,3)"))) == (False, False)
    assert plain(s.groupBy("id").agg(F.sum("amount"))) == (False, False)
    assert plain(s.select("id").union(o.select("id"))) == (False,)
    joined = s.join(o, s.id == o.id, "left").select(s.amount, o.amount.alias("other"))
    assert plain(joined) == (True, True)
    assert plain(s.join(o, "id", "semi")) == (True, True, True)
    assert plain(s.withColumn("x", F.lit(1)).distinct()) == (True, True, True, False)


def test_plain_decimals_skip_rounding_only_when_every_value_is_a_decimal():
    schema = StructType([StructField("n", DecimalType(10, 2))])
    rows = [(D("1.50"),), (None,), (D("2.25"),)]
    out = convert_columns(rows, schema, (True,))[0]
    assert out[0] is rows[0][0]  # untouched
    whole = [(D("1.50"),), (20,), (None,)]  # IRIS returns whole NUMERIC values as int
    assert [str(v) for v in convert_columns(whole, schema, (True,))[0][:2]] == ["1.50", "20.00"]
    computed = [(D("1.505"),)]  # not plain: rounded to the scale, as before
    assert str(convert_columns(computed, schema, (False,))[0][0]) == "1.51"
