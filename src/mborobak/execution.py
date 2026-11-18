"""Execution adapter: runs statements, converts values, maps errors (compiler spec §7.5)."""

from __future__ import annotations

import datetime
import decimal
import math
import re
import time
from dataclasses import dataclass, field

from .errors import (
    MborobakArithmeticError,
    MborobakCastError,
    MborobakExecutionError,
)
from .sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampNTZType,
)


@dataclass
class ExecutionMetrics:
    """``session.lastExecution`` (API contract §7.2)."""

    action: str
    sql: str
    params: list
    data_queries: int = 0
    schema_lookups: int = 0
    rows: int = 0
    python_ms: float = 0.0
    server_ms: float = 0.0
    fetch_ms: float = 0.0
    cache_hit: bool = False


@dataclass
class QueryCounter:
    data_queries: int = 0
    schema_lookups: int = 0
    log: list[str] = field(default_factory=list)


_SQLCODE = re.compile(r"SQLCODE:\s*<(-?\d+)>")
_IRIS_CODE = re.compile(r"<(DIVIDE|ILLEGAL VALUE|MAXNUMBER|RANGE|[A-Z ]+)>[^<]*\^%sqlcq")


def map_error(exc: Exception, sql: str, operation: str | None, candidates: list[str]) -> Exception:
    text = str(exc)
    m = _SQLCODE.search(text)
    sqlcode = int(m.group(1)) if m else None
    code_match = _IRIS_CODE.search(text)
    iris_code = f"<{code_match.group(1)}>" if code_match else None
    details = dict(sqlcode=sqlcode, iris_code=iris_code, iris_message=text, sql=sql)
    hint = f" (possible causes: {', '.join(candidates)})" if candidates else ""
    if iris_code == "<DIVIDE>":
        return MborobakArithmeticError("division by zero" + hint, operation=operation, **details)
    if iris_code == "<ILLEGAL VALUE>":
        return MborobakCastError("invalid input for cast" + hint, operation=operation, **details)
    if iris_code == "<MAXNUMBER>":
        return MborobakArithmeticError("numeric overflow" + hint, operation=operation, **details)
    first_line = text.splitlines()[0] if text else type(exc).__name__
    return MborobakExecutionError(f"IRIS error: {first_line}", operation=operation, **details)


# --------------------------------------------------------------------------- value conversion


def _to_int(v):
    return int(v)


def _to_float(v):
    if isinstance(v, float):
        return v
    if isinstance(v, str):
        upper = v.strip().upper()
        if upper == "NAN":
            return math.nan
        if upper in ("INF", "INFINITY"):
            return math.inf
        if upper in ("-INF", "-INFINITY"):
            return -math.inf
    return float(v)


def _decimal_converter(t: DecimalType):
    quantum = decimal.Decimal(1).scaleb(-t.scale)

    def conv(v):
        d = v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))
        return d.quantize(quantum, rounding=decimal.ROUND_HALF_UP)

    return conv


def _to_str(v):
    return v if isinstance(v, str) else str(v)


def _to_bool(v):
    if isinstance(v, str):
        return v.strip() not in ("0", "", "false", "False")
    return bool(v)


def _to_date(v):
    if isinstance(v, datetime.datetime):
        return v.date()
    if isinstance(v, datetime.date):
        return v
    return datetime.date.fromisoformat(str(v)[:10])


def _to_timestamp(v):
    if isinstance(v, datetime.datetime):
        return v
    if isinstance(v, datetime.date):
        return datetime.datetime(v.year, v.month, v.day)
    return datetime.datetime.fromisoformat(str(v))


def converter(t: DataType):
    if isinstance(t, (IntegerType, LongType)):
        return _to_int
    if isinstance(t, DoubleType):
        return _to_float
    if isinstance(t, DecimalType):
        return _decimal_converter(t)
    if isinstance(t, StringType):
        return _to_str
    if isinstance(t, BooleanType):
        return _to_bool
    if isinstance(t, DateType):
        return _to_date
    if isinstance(t, TimestampNTZType):
        return _to_timestamp
    return lambda v: v


def convert_rows(rows, schema: StructType) -> list[tuple]:
    convs = [converter(f.dataType) for f in schema.fields]
    out = []
    for r in rows:
        out.append(tuple(None if v is None else c(v) for c, v in zip(convs, r)))
    return out


class Executor:
    def __init__(self, connection, counter: QueryCounter):
        self.connection = connection
        self.counter = counter

    def lookup(self, sql: str, params: list) -> list[tuple]:
        cur = self.connection.cursor()
        try:
            cur.execute(sql, params)
            self.counter.schema_lookups += 1
            return [tuple(r) for r in cur.fetchall()]
        finally:
            cur.close()

    def describe(self, sql: str, params: list, operation: str | None):
        """Column metadata of a query, read without fetching rows (counts as a schema lookup)."""
        cur = self.connection.cursor()
        try:
            try:
                cur.execute(sql, params)
            except Exception as exc:  # noqa: BLE001
                raise map_error(exc, sql, operation, []) from exc
            self.counter.schema_lookups += 1
            description = [tuple(d) for d in (cur.description or [])]
            cur.fetchall()
            return description
        finally:
            cur.close()

    def run(self, sql: str, params: list, operation: str | None, candidates: list[str]):
        """Execute and fetch; returns (rows, server_ms, fetch_ms)."""
        cur = self.connection.cursor()
        try:
            t0 = time.perf_counter()
            try:
                cur.execute(sql, params)
                self.counter.data_queries += 1
                t1 = time.perf_counter()
                rows = cur.fetchall()
            except Exception as exc:  # noqa: BLE001 - mapped to Mborobak errors
                raise map_error(exc, sql, operation, candidates) from exc
            t2 = time.perf_counter()
            return rows, (t1 - t0) * 1000, (t2 - t1) * 1000
        finally:
            cur.close()
