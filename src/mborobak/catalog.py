"""Schema lookups, cached per session (scope §5.2, compiler spec §6.4)."""

from __future__ import annotations

import re
from dataclasses import dataclass

from . import expressions as E
from .errors import TableNotFoundError
from .plan import TableRef
from .sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampNTZType,
)

DEFAULT_SCHEMA = "SQLUser"

COLUMNS_SQL = """SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.NUMERIC_PRECISION,
       c.NUMERIC_SCALE, c.IS_NULLABLE, p.Parameters, c.PRIMARY_KEY
FROM INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
LEFT JOIN %Dictionary.CompiledProperty p ON p.parent = t.CLASSNAME AND p.SqlFieldName = c.COLUMN_NAME
WHERE UPPER(c.TABLE_SCHEMA) = UPPER(?) AND UPPER(c.TABLE_NAME) = UPPER(?)
ORDER BY c.ORDINAL_POSITION"""


@dataclass(frozen=True)
class CatalogColumn:
    name: str
    iris_type: str
    dtype: DataType | None  # None: unsupported in P0
    nullable: bool
    collation: str | None
    primary_key: bool = False


@dataclass(frozen=True)
class TableInfo:
    table: TableRef
    columns: tuple[CatalogColumn, ...]

    def attributes(self) -> tuple[tuple[E.Attribute, ...], tuple[tuple[str, str], ...]]:
        attrs, unsupported = [], []
        for c in self.columns:
            if c.dtype is None:
                unsupported.append((c.name, c.iris_type))
            else:
                attrs.append(E.Attribute(E.new_id(), c.name, c.dtype, c.nullable, c.collation))
        return tuple(attrs), tuple(unsupported)

    def key(self, attrs: tuple[E.Attribute, ...]) -> tuple[int, ...]:
        """Ids of the primary key's columns among ``attrs``; () without a key or if a key column is missing."""
        names = [c.name for c in self.columns if c.primary_key]
        ids = {a.name: a.id for a in attrs}
        return tuple(ids[n] for n in names) if names and all(n in ids for n in names) else ()


def map_type(data_type: str, precision, scale) -> DataType | None:
    t = (data_type or "").lower()
    if t in ("integer", "smallint", "tinyint"):
        return IntegerType()
    if t == "bigint":
        return LongType()
    if t in ("double", "float", "real"):
        return DoubleType()
    if t in ("numeric", "decimal", "money"):
        return DecimalType(int(precision or 18), int(scale or 0))
    if t in ("varchar", "char", "longvarchar"):
        return StringType()
    if t == "bit":
        return BooleanType()
    if t == "date":
        return DateType()
    if t in ("timestamp", "datetime", "posixtime"):
        return TimestampNTZType()
    return None


# JDBC type codes reported by the IRIS driver in cursor.description (probe for session.sql()).
_DESCRIPTION_TYPES = {
    4: "integer", 5: "smallint", -6: "tinyint", -5: "bigint",
    8: "double", 6: "float", 7: "real",
    2: "numeric", 3: "decimal",
    12: "varchar", 1: "char", -1: "longvarchar", -9: "varchar", -15: "char",
    -7: "bit", 16: "bit",
    1091: "date", 91: "date",
    1093: "timestamp", 93: "timestamp",
    # Not supported in P0; named so error messages say what the column is.
    1092: "time", 92: "time", -2: "binary", -3: "varbinary", -4: "longvarbinary",
    2004: "blob", 2005: "clob",
}


def type_from_description(type_code, precision, scale) -> tuple[str, DataType | None]:
    """IRIS type name and Mborobak type for one ``cursor.description`` entry."""
    name = _DESCRIPTION_TYPES.get(type_code, f"type code {type_code}")
    if name in ("numeric", "decimal"):
        p = int(precision or 38)
        s = max(0, int(scale or 0))
        return name, DecimalType(min(max(p, s, 1), 38), min(s, 38))
    return name, map_type(name, precision, scale)


def parse_collation(parameters: str | None) -> str | None:
    """Value after ``COLLATION`` in ``CompiledProperty.Parameters``; '' means the namespace default."""
    if parameters is None:
        return None
    items = parameters.split(",")
    pairs = dict(zip(items[0::2], items[1::2]))
    if "COLLATION" not in pairs:
        return None
    return pairs["COLLATION"].upper() or "SQLUPPER"


_PART = r"(?:`([^`]+)`|([^.`]+))"
_NAME = re.compile(rf"^{_PART}(?:\.{_PART})?$")


def parse_table_name(name: str) -> TableRef:
    m = _NAME.match(name.strip())
    if not m:
        raise ValueError(f"invalid table name {name!r}")
    first = m.group(1) or m.group(2)
    second = m.group(3) or m.group(4)
    if second is None:
        return TableRef(DEFAULT_SCHEMA, first.strip())
    return TableRef(first.strip(), second.strip())


class Catalog:
    """Reads INFORMATION_SCHEMA once per table and caches the result."""

    def __init__(self, run_lookup):
        self._run_lookup = run_lookup  # (sql, params) -> rows; counts as a schema lookup
        self._cache: dict[tuple[str, str], TableInfo] = {}

    def lookup(self, name: str, operation: str | None = None) -> TableInfo:
        ref = parse_table_name(name)
        key = (ref.schema.upper(), ref.name.upper())
        if key in self._cache:
            return self._cache[key]
        rows = self._run_lookup(COLUMNS_SQL, [ref.schema, ref.name])
        if not rows:
            raise TableNotFoundError(f"table {ref} not found (or no SELECT permission)", operation=operation)
        schema, table = rows[0][0], rows[0][1]
        columns = tuple(
            CatalogColumn(
                name=r[2],
                iris_type=r[3],
                dtype=map_type(r[3], r[4], r[5]),
                nullable=str(r[6]).upper() != "NO",
                collation=parse_collation(r[7]) if map_type(r[3], r[4], r[5]) == StringType() else None,
                primary_key=len(r) > 8 and str(r[8]).upper() == "YES",
            )
            for r in rows
        )
        info = TableInfo(TableRef(schema, table), columns)
        self._cache[key] = info
        return info

    def refresh(self, name: str | None = None) -> None:
        if name is None:
            self._cache.clear()
            return
        ref = parse_table_name(name)
        self._cache.pop((ref.schema.upper(), ref.name.upper()), None)


class StaticCatalog(Catalog):
    """In-memory catalog for unit tests: ``{"Sales": [("id", IntegerType(), True, None), …]}``."""

    def __init__(self, tables: dict[str, list[tuple]]):
        super().__init__(run_lookup=None)
        for name, cols in tables.items():
            ref = parse_table_name(name)
            info = TableInfo(ref, tuple(
                CatalogColumn(c[0], c[1].simpleString(), c[1], c[2] if len(c) > 2 else True,
                              c[3] if len(c) > 3 else ("SQLUPPER" if isinstance(c[1], StringType) else None),
                              len(c) > 4 and c[4])
                for c in cols))
            self._cache[(ref.schema.upper(), ref.name.upper())] = info

    def lookup(self, name: str, operation: str | None = None) -> TableInfo:
        ref = parse_table_name(name)
        try:
            return self._cache[(ref.schema.upper(), ref.name.upper())]
        except KeyError:
            raise TableNotFoundError(f"table {ref} not found", operation=operation) from None
