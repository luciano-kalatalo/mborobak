"""``session.createDataFrame``: local Python data or a pandas DataFrame, loaded into IRIS (API contract §3.6).

The rows go into a ``GLOBAL TEMPORARY`` table: its rows belong to this connection only and disappear when it
closes (probed on 2026.2, compiler spec §9.8). The table's columns are named ``c1``, ``c2``, …, and a
projection gives them the DataFrame's names, so any Spark column name works. Schema inference and value
checks follow PySpark 4.3's ``createDataFrame`` (``verifySchema=True``).
"""

from __future__ import annotations

import datetime
import decimal
import math
import re
import time
import uuid
from typing import TYPE_CHECKING, Any

from . import expressions as E
from . import plan as P
from .errors import MborobakConnectionError, UnsupportedTypeError
from .sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructType,
    TimestampNTZType,
)

if TYPE_CHECKING:
    from .dataframe import DataFrame
    from .session import MborobakSession

TEMP_SCHEMA = "MborobakTemp"
_INT_RANGE = {IntegerType: (-2**31, 2**31 - 1), LongType: (-2**63, 2**63 - 1)}


# --------------------------------------------------------------------------- schema
def parse_schema(schema: Any) -> tuple[list[str] | None, list[DataType] | None, list[bool] | None]:
    """(names, types, nullables) of a ``schema`` argument; None parts are inferred from the data."""
    if schema is None:
        return None, None, None
    if isinstance(schema, StructType):
        fields = list(schema.fields)
        return [f.name for f in fields], [f.dataType for f in fields], [f.nullable for f in fields]
    if isinstance(schema, DataType):
        return ["value"], [schema], [True]
    if isinstance(schema, str):
        return _parse_ddl(schema)
    if isinstance(schema, (list, tuple)) and all(isinstance(n, str) for n in schema):
        return list(schema), None, None
    raise TypeError("schema must be a StructType, a DDL string such as 'a int, b string', or a list of names")


def _parse_ddl(text: str) -> tuple[list[str], list[DataType], list[bool]]:
    from .sqlexpr import sql_type

    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        if ch in "(<":
            depth += 1
        elif ch in ")>":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    if len(parts) == 1 and not re.search(r"[:\s]", parts[0].strip()):
        return ["value"], [sql_type(parts[0])], [True]  # an atomic type: 'int'
    names, types, nullables = [], [], []
    for part in parts:
        m = re.fullmatch(r"\s*(`[^`]+`|[^\s:`]+)\s*:?\s*(.+?)(\s+not\s+null)?\s*", part, re.I | re.S)
        if not m:
            raise ValueError(f"invalid schema string {text!r}: expected 'name type, …'")
        names.append(m.group(1).strip("`"))
        types.append(sql_type(m.group(2)))
        nullables.append(m.group(3) is None)
    return names, types, nullables


# --------------------------------------------------------------------------- rows
def _plain(v: Any) -> Any:
    """numpy and pandas scalars → Python values; pandas' missing markers → None."""
    if v is None:
        return None
    module = type(v).__module__
    if module.startswith(("numpy", "pandas")):
        try:
            import pandas as pd

            if v is pd.NaT or v is pd.NA:
                return None
            if isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
        except ImportError:
            pass
        if hasattr(v, "item"):
            v = v.item()
    return v


def from_pandas(pdf: Any) -> tuple[list[str], list[tuple], list[DataType | None]]:
    """Names, rows and, per column, the type pandas' dtype implies (None: infer from the values)."""
    import pandas as pd

    names = [str(c) for c in pdf.columns]
    hints: list[DataType | None] = []
    columns = []
    for c in pdf.columns:
        s = pdf[c]
        kind = s.dtype.kind
        if kind in "iu":
            hints.append(LongType())
            columns.append([int(v) for v in s.tolist()])
        elif kind == "f":
            hints.append(DoubleType())
            columns.append([float(v) for v in s.tolist()])  # NaN stays NaN, as in PySpark
        elif kind == "b":
            hints.append(BooleanType())
            columns.append([bool(v) for v in s.tolist()])
        elif kind == "M":
            hints.append(TimestampNTZType())
            columns.append([None if pd.isna(v) else v.to_pydatetime().replace(tzinfo=None) for v in s])
        else:  # object, string, category: missing values are NULL
            hints.append(None)
            columns.append([None if _missing(v) else _plain(v) for v in s.tolist()])
    return names, list(zip(*columns)) if columns else [], hints


def _missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        import pandas as pd

        return v is pd.NA or v is pd.NaT
    except ImportError:
        return False


def rows_of(data: Any) -> tuple[list[str] | None, list[tuple], list[DataType | None] | None, bool]:
    """(names from the data, rows as tuples, type hints, whether rows were dicts and are matched by name)."""
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            names, rows, hints = from_pandas(data)
            return names, rows, hints, False
    except ImportError:
        pass
    if isinstance(data, (str, bytes)) or not hasattr(data, "__iter__"):
        raise TypeError("createDataFrame() takes a list of rows (tuples, lists, Rows or dicts) or a pandas "
                        "DataFrame")
    items = list(data)
    if not items:
        return None, [], None, False
    first = items[0]
    if isinstance(first, dict):
        names = sorted({k for row in items for k in row})  # PySpark sorts dict keys
        return names, [tuple(row.get(n) for n in names) for row in items], None, True
    names = list(getattr(first, "__fields__", ()) or ()) or None
    if isinstance(first, (tuple, list)):
        return names, [tuple(_plain(v) for v in row) for row in items], None, False
    return None, [(_plain(v),) for v in items], None, False  # scalars: a one-column schema


# --------------------------------------------------------------------------- inference
def infer_type(v: Any) -> DataType:
    """PySpark's type for a Python value (NullType for None)."""
    if v is None:
        return NullType()
    if isinstance(v, bool):
        return BooleanType()
    if isinstance(v, int):
        return LongType()
    if isinstance(v, float):
        return DoubleType()
    if isinstance(v, str):
        return StringType()
    if isinstance(v, decimal.Decimal):
        return DecimalType(38, 18)
    if isinstance(v, datetime.datetime):
        return TimestampNTZType()  # Spark infers timestamp (with time zone); Mborobak has timestamp_ntz (TIME-01)
    if isinstance(v, datetime.date):
        return DateType()
    raise UnsupportedTypeError(f"createDataFrame(): values of type {type(v).__name__} are not supported "
                               "(arrays, maps, structs and binary data have no IRIS column type here)")


def merge(a: DataType, b: DataType, name: str) -> DataType:
    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType) or a == b:
        return a
    raise TypeError(f"createDataFrame(): column {name!r} has values of types {a.simpleString()} and "
                    f"{b.simpleString()}, which PySpark can't merge either; give a schema")


# --------------------------------------------------------------------------- values
def convert(v: Any, t: DataType, name: str, nullable: bool) -> Any:
    """A value checked and converted for column ``name`` of type ``t``, as PySpark's verifySchema does."""
    if v is None or (isinstance(v, float) and math.isnan(v) and not isinstance(t, (DoubleType, StringType))):
        if v is not None:
            raise TypeError(f"field {name}: {t.simpleString()} can not accept NaN")
        if not nullable:
            raise ValueError(f"field {name}: this field is not nullable, but got None")
        return None

    def reject() -> TypeError:
        return TypeError(f"field {name}: {t.simpleString()} can not accept object {v!r} in type "
                         f"{type(v).__name__}")

    if isinstance(t, (IntegerType, LongType)):
        if not isinstance(v, int) or isinstance(v, bool):
            raise reject()
        low, high = _INT_RANGE[type(t)]
        if not low <= v <= high:
            raise ValueError(f"field {name}: value {v} is out of range for {t.simpleString()}")
        return v
    if isinstance(t, DoubleType):
        if not isinstance(v, float):
            raise reject()
        return v
    if isinstance(t, DecimalType):
        if not isinstance(v, decimal.Decimal):
            raise reject()
        # Python's default context keeps 28 digits; decimal(38, s) needs up to 38
        q = v.quantize(decimal.Decimal(1).scaleb(-t.scale), rounding=decimal.ROUND_HALF_UP,
                       context=decimal.Context(prec=80))
        if len(q.as_tuple().digits) - max(0, -q.as_tuple().exponent) > t.precision - t.scale and q != 0:
            raise ValueError(f"field {name}: {v} can't be represented as {t.simpleString()}")
        return q
    if isinstance(t, StringType):
        if isinstance(v, str):
            return v
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (float, datetime.date, int, decimal.Decimal)):
            return str(v)  # PySpark converts with Python's str(): 1e+20, not Java's 1.0E20
        raise reject()
    if isinstance(t, BooleanType):
        if not isinstance(v, bool):
            raise reject()
        return v
    if isinstance(t, DateType):
        if isinstance(v, datetime.datetime):
            return v.date()  # PySpark accepts a datetime and keeps its date
        if isinstance(v, datetime.date):
            return v
        raise reject()
    if isinstance(t, TimestampNTZType):
        if not isinstance(v, datetime.datetime):
            raise reject()
        if v.tzinfo is not None:
            v = v.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return v
    raise UnsupportedTypeError(f"createDataFrame(): type {t.simpleString()} is not supported")


# --------------------------------------------------------------------------- entry point
def create_data_frame(session: "MborobakSession", data: Any, schema: Any, verify: bool, op: str) -> "DataFrame":
    from .dataframe import DataFrame
    from .writer import ddl_type

    names, types, nullables = parse_schema(schema)
    data_names, rows, hints, by_name = rows_of(data)

    if types is not None:  # a full schema
        if by_name:  # dicts: by name
            order = [data_names.index(n) if n in data_names else None for n in names]
            rows = [tuple(None if i is None else r[i] for i in order) for r in rows]
        width = len(names)
    else:
        width = len(rows[0]) if rows else len(data_names or names or [])
        if not rows and data_names is None:
            raise ValueError("createDataFrame(): can not infer a schema from empty data; give a schema")
        base = data_names or [f"_{i + 1}" for i in range(width)]
        names = (list(names or []) + base[len(names or []):])[:width] if names else base
        types = []
        for i in range(width):
            t: DataType = hints[i] if hints and hints[i] is not None else NullType()
            if not (hints and hints[i] is not None):
                for r in rows:
                    t = merge(t, infer_type(r[i]) if i < len(r) else NullType(), names[i])
            if isinstance(t, NullType):
                raise ValueError(f"createDataFrame(): the type of column {names[i]!r} can't be inferred: it has "
                                 "only None values; give a schema")
            types.append(t)
        nullables = [True] * width

    for r in rows:
        if len(r) != width:
            raise ValueError(f"createDataFrame(): a row has {len(r)} values and the schema {width} columns")
    if verify or schema is not None:
        rows = [tuple(convert(v, t, n, nl) for v, t, n, nl in zip(r, types, names, nullables)) for r in rows]

    session._check_open()
    executor = session._executor
    if executor is None:
        raise MborobakConnectionError("createDataFrame() loads the rows into IRIS and needs a connection",
                                      operation=op)
    ref = P.TableRef(TEMP_SCHEMA, f"DF{uuid.uuid4().hex[:16]}")
    exact = session.conf.get("mborobak.sql.stringSemantics") == "pyspark"
    length = session.conf.get("mborobak.write.stringLength")
    columns = ", ".join(f'"c{i + 1}" {ddl_type(t, exact, length)}' for i, t in enumerate(types))
    t0 = time.perf_counter()
    executor.execute(f"CREATE GLOBAL TEMPORARY TABLE {ref.quoted()} ({columns})", [], op)
    if not session._temp_tables:  # a script that never calls stop() still drops its definitions at exit
        import atexit
        import weakref

        ref_session = weakref.ref(session)
        atexit.register(lambda: (lambda s: s is not None and not s._closed and drop_temp_tables(s))(ref_session()))
    session._temp_tables.append(ref)
    if rows:
        marks = ", ".join("?" * width)
        cur = executor.connection.cursor()
        try:
            try:
                cur.executemany(f"INSERT INTO {ref.quoted()} VALUES ({marks})", [list(r) for r in rows])
            except Exception as exc:  # noqa: BLE001 - mapped to Mborobak errors
                from .execution import map_error

                raise map_error(exc, "INSERT", op, []) from exc
        finally:
            cur.close()
        executor.counter.data_queries += 1
    from .execution import ExecutionMetrics

    session.lastExecution = ExecutionMetrics(action="createDataFrame", sql=f"INSERT INTO {ref.quoted()}",
                                             params=[], data_queries=2 if rows else 1, rows=len(rows),
                                             server_ms=(time.perf_counter() - t0) * 1000)

    scan_attrs = tuple(E.Attribute(E.new_id(), f"c{i + 1}", t, nl, "EXACT" if isinstance(t, StringType) and exact
                                   else None) for i, (t, nl) in enumerate(zip(types, nullables)))
    scan = P.TableScan(ref, scan_attrs)
    items = tuple((E.Attribute(E.new_id(), n, a.dtype, a.nullable, a.collation), E.AttrRef(a))
                  for n, a in zip(names, scan_attrs))
    return DataFrame(session, P.Project(scan, items))


def drop_temp_tables(session: "MborobakSession") -> None:
    if session._executor is None:
        return
    for ref in session._temp_tables:
        try:
            session._executor.execute(f"DROP TABLE {ref.quoted()}", [], "stop")
        except Exception:  # noqa: BLE001 - the connection may be gone; the rows went with it
            pass
    session._temp_tables.clear()

