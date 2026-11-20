"""Hashes, edit distance, trigonometry, day names, conv, parse_url, time zones (compiler spec §7.19)."""

from __future__ import annotations

import pytest

from mborobak import MborobakSession
from mborobak.catalog import StaticCatalog
from mborobak.errors import AnalysisError
from mborobak.irisfuncs import FUNCTIONS, create_sql
from mborobak.sql import functions as F
from mborobak.sql.types import DateType, DoubleType, IntegerType, StringType, TimestampNTZType
from mborobak.sql.window import Window


@pytest.fixture()
def df():
    s = MborobakSession._for_tests(StaticCatalog({"T": [("s", StringType()), ("t", StringType()), ("x", DoubleType()),
                                                         ("i", IntegerType()), ("d", DateType()),
                                                         ("ts", TimestampNTZType())]}))
    return s.table("T")


def test_names_and_types(df):
    out = df.select(F.md5("s"), F.sha1("s"), F.sha2("s", 256), F.crc32("s"), F.levenshtein("s", "t"),
                    F.levenshtein("s", "t", 2), F.sin("x"), F.atan2("x", 1.0), F.degrees("i"), F.pi(),
                    F.dayname("d"), F.monthname("ts"), F.conv("s", 10, 2), F.parse_url("s", F.lit("HOST")),
                    F.to_utc_timestamp("ts", "America/Sao_Paulo"))
    assert out.dtypes == [("md5(s)", "string"), ("sha1(s)", "string"), ("sha2(s, 256)", "string"),
                          ("crc32(s)", "bigint"), ("levenshtein(s, t)", "int"), ("levenshtein(s, t, 2)", "int"),
                          ("SIN(x)", "double"), ("ATAN2(x, 1.0)", "double"), ("DEGREES(i)", "double"), ("PI()", "double"),
                          ("dayname(d)", "string"), ("monthname(ts)", "string"), ("conv(s, 10, 2)", "string"),
                          ("parse_url(s, HOST)", "string"),
                          ("to_utc_timestamp(ts, America/Sao_Paulo)", "timestamp_ntz")]
    w = Window.partitionBy("s").orderBy("i")
    assert df.select(F.first_value("x").over(w)).columns[0].startswith("first_value(x) OVER (PARTITION BY s")


def test_functions_recorded_and_parameters_in_order(df):
    stmt = df._session._compile(df.select(F.parse_url("s", F.lit("QUERY"), F.lit("k")), F.sha2("t", 512))._plan)
    assert stmt.functions == frozenset({"ParseUrl1", "Sha2Hex1"})
    assert stmt.params == ["QUERY", "QUERY", "k", 512]  # guard, then the call's arguments, then sha2's bits


def test_errors(df):
    with pytest.raises(AnalysisError, match="binary"):
        df.select(F.md5("i"))
    with pytest.raises(ValueError, match="numBits"):
        F.sha2("s", 128)
    with pytest.raises(AnalysisError):
        df.select(F.sin("s"))


def test_function_bodies_avoid_driver_parameter_markers():
    import re

    for name, (_, _, body, *language) in FUNCTIONS.items():
        if name == "RegexpInstr1":
            continue  # created and tested before this rule
        unquoted = re.sub(r'"[^"]*"|\'[^\']*\'', "", body)  # the driver skips quoted text
        assert "?" not in unquoted and not re.search(r":\d", unquoted), name
        assert create_sql(name).endswith("}")
