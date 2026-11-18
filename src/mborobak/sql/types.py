"""Data types (API contract §10, compiler spec §3)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

__all__ = [
    "DataType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "DecimalType",
    "StringType",
    "BooleanType",
    "DateType",
    "TimestampNTZType",
    "NullType",
    "StructField",
    "StructType",
    "parse_type",
]


class DataType:
    """Base class. Types compare equal by value."""

    def simpleString(self) -> str:  # noqa: N802 - PySpark name
        raise NotImplementedError

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash((type(self).__name__, tuple(sorted(self.__dict__.items()))))

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"

    # Classification helpers used by the analyzer and compiler.
    @property
    def is_numeric(self) -> bool:
        return isinstance(self, (IntegerType, LongType, DoubleType, DecimalType))

    @property
    def is_integral(self) -> bool:
        return isinstance(self, (IntegerType, LongType))


class IntegerType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "int"


class LongType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "bigint"


class DoubleType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "double"


class DecimalType(DataType):
    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = min(int(precision), 38)
        self.scale = int(scale)

    def simpleString(self) -> str:  # noqa: N802
        return f"decimal({self.precision},{self.scale})"

    def __repr__(self) -> str:
        return f"DecimalType({self.precision}, {self.scale})"


class StringType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "string"


class BooleanType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "boolean"


class DateType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "date"


class TimestampNTZType(DataType):
    def simpleString(self) -> str:  # noqa: N802
        return "timestamp_ntz"


class NullType(DataType):
    """Type of a bare ``lit(None)``."""

    def simpleString(self) -> str:  # noqa: N802
        return "void"


@dataclass(frozen=True)
class StructField:
    name: str
    dataType: DataType  # noqa: N815 - PySpark name
    nullable: bool = True

    def simpleString(self) -> str:  # noqa: N802
        return f"{self.name}:{self.dataType.simpleString()}"


class StructType(DataType):
    def __init__(self, fields: list[StructField] | None = None):
        self.fields = list(fields or [])

    def __iter__(self) -> Iterator[StructField]:
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __getitem__(self, key: int | str) -> StructField:
        if isinstance(key, int):
            return self.fields[key]
        for f in self.fields:
            if f.name == key:
                return f
        raise KeyError(key)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StructType) and self.fields == other.fields

    def __hash__(self) -> int:
        return hash(tuple(self.fields))

    def fieldNames(self) -> list[str]:  # noqa: N802
        return [f.name for f in self.fields]

    def simpleString(self) -> str:  # noqa: N802
        return "struct<" + ",".join(f.simpleString() for f in self.fields) + ">"

    def __repr__(self) -> str:
        return f"StructType({self.fields!r})"


_SIMPLE = {
    "int": IntegerType,
    "integer": IntegerType,
    "bigint": LongType,
    "long": LongType,
    "double": DoubleType,
    "float": DoubleType,
    "string": StringType,
    "boolean": BooleanType,
    "bool": BooleanType,
    "date": DateType,
    "timestamp_ntz": TimestampNTZType,
}
_DECIMAL = re.compile(r"^decimal\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$")


def parse_type(value: DataType | str) -> DataType:
    """Accept a DataType or a PySpark type name (API contract §6.3)."""
    if isinstance(value, DataType):
        return value
    if not isinstance(value, str):
        raise TypeError(f"expected a DataType or a type name, got {type(value).__name__}")
    name = value.strip().lower()
    if name in _SIMPLE:
        return _SIMPLE[name]()
    m = _DECIMAL.match(name)
    if m:
        return DecimalType(int(m.group(1) or 10), int(m.group(2) or 0))
    raise ValueError(f"unknown type name {value!r}")
