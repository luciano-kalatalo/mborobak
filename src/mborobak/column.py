"""``Column`` (API contract §6)."""

from __future__ import annotations

import datetime
import decimal
from typing import Any

from . import expressions as E
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
    TimestampNTZType,
    parse_type,
)

__all__ = ["Column"]

_INT32 = (-(2**31), 2**31 - 1)


def literal(value: Any) -> E.Literal:
    """Python value → ``Literal`` with PySpark's literal type."""
    if value is None:
        return E.Literal(None, NullType())
    if isinstance(value, bool):
        return E.Literal(value, BooleanType())
    if isinstance(value, int):
        return E.Literal(value, IntegerType() if _INT32[0] <= value <= _INT32[1] else LongType())
    if isinstance(value, float):
        return E.Literal(value, DoubleType())
    if isinstance(value, decimal.Decimal):
        sign, digits, exp = value.as_tuple()
        scale = max(0, -exp) if isinstance(exp, int) else 0
        precision = max(len(digits), scale, 1)
        return E.Literal(value, DecimalType(precision, scale))
    if isinstance(value, str):
        return E.Literal(value, StringType())
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            raise TypeError("timestamps with a time zone are not supported; IRIS TIMESTAMP has none "
                            "(compatibility matrix TIME-02)")
        return E.Literal(value, TimestampNTZType())
    if isinstance(value, datetime.date):
        return E.Literal(value, DateType())
    raise TypeError(f"unsupported literal type: {type(value).__name__}")


def to_expr(value: Any) -> E.Expr:
    """A Column, or a plain value turned into a literal."""
    if isinstance(value, Column):
        return value._expr
    return literal(value)


def to_column_expr(value: Any) -> E.Expr:
    """A Column or a column name (``str``)."""
    if isinstance(value, Column):
        return value._expr
    if isinstance(value, str):
        return E.Star() if value == "*" else E.UnresolvedColumn(value)
    raise TypeError(f"expected a column name or Column, got {type(value).__name__}")


class Column:
    """An expression over a DataFrame's columns. Immutable."""

    __slots__ = ("_expr",)

    def __init__(self, expr: E.Expr):
        self._expr = expr

    # ---------------------------------------------------------------- arithmetic
    def _bin(self, op: str, other: Any, reverse: bool = False) -> "Column":
        left, right = self._expr, to_expr(other)
        if reverse:
            left, right = right, left
        return Column(E.BinaryArithmetic(op, left, right))

    def __add__(self, o): return self._bin("+", o)
    def __radd__(self, o): return self._bin("+", o, True)
    def __sub__(self, o): return self._bin("-", o)
    def __rsub__(self, o): return self._bin("-", o, True)
    def __mul__(self, o): return self._bin("*", o)
    def __rmul__(self, o): return self._bin("*", o, True)
    def __truediv__(self, o): return self._bin("/", o)
    def __rtruediv__(self, o): return self._bin("/", o, True)
    def __mod__(self, o): return self._bin("%", o)
    def __rmod__(self, o): return self._bin("%", o, True)
    def __neg__(self): return Column(E.Negate(self._expr))

    # ---------------------------------------------------------------- comparison
    def _cmp(self, op: str, other: Any) -> "Column":
        return Column(E.Comparison(op, self._expr, to_expr(other)))

    def __eq__(self, o): return self._cmp("=", o)  # type: ignore[override]
    def __ne__(self, o): return self._cmp("<>", o)  # type: ignore[override]
    def __lt__(self, o): return self._cmp("<", o)
    def __le__(self, o): return self._cmp("<=", o)
    def __gt__(self, o): return self._cmp(">", o)
    def __ge__(self, o): return self._cmp(">=", o)

    def eqNullSafe(self, other: Any) -> "Column":  # noqa: N802
        return self._cmp("<=>", other)

    __hash__ = None  # type: ignore[assignment]  # == builds an expression

    # ---------------------------------------------------------------- logic
    def __and__(self, o): return Column(E.And(self._expr, to_expr(o)))
    def __rand__(self, o): return Column(E.And(to_expr(o), self._expr))
    def __or__(self, o): return Column(E.Or(self._expr, to_expr(o)))
    def __ror__(self, o): return Column(E.Or(to_expr(o), self._expr))
    def __invert__(self): return Column(E.Not(self._expr))

    def __bool__(self):
        raise ValueError("Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
                         "'~' for 'not' when building DataFrame boolean expressions.")

    def __iter__(self):
        raise TypeError("Column is not iterable")

    # ---------------------------------------------------------------- predicates
    def isNull(self) -> "Column":  # noqa: N802
        return Column(E.IsNull(self._expr))

    def isNotNull(self) -> "Column":  # noqa: N802
        return Column(E.IsNull(self._expr, negated=True))

    def isin(self, *values: Any) -> "Column":
        if len(values) == 1 and isinstance(values[0], (list, tuple, set)):
            values = tuple(values[0])
        return Column(E.In(self._expr, tuple(to_expr(v) for v in values)))

    def like(self, pattern: str) -> "Column":
        if not isinstance(pattern, str):
            raise TypeError("like() takes a string pattern")
        return Column(E.Like(self._expr, E.Literal(pattern, StringType())))

    def rlike(self, other: str) -> "Column":
        """A match of the regular expression ``other`` anywhere in the string (compiler spec §7.16)."""
        if not isinstance(other, str):
            raise TypeError("rlike() takes a string pattern")
        return Column(E.Func("rlike", (self._expr, E.Literal(other, StringType()))))

    def between(self, lowerBound: Any, upperBound: Any) -> "Column":  # noqa: N803
        return (self >= lowerBound) & (self <= upperBound)

    def over(self, window: Any) -> "Column":
        """This aggregate or window function, computed over ``window`` (a ``WindowSpec``)."""
        from .sql.window import WindowSpec

        if not isinstance(window, WindowSpec):
            raise TypeError("over() takes a WindowSpec, e.g. Window.partitionBy('g').orderBy('t')")
        expr = self._expr.child if isinstance(self._expr, E.Alias) else self._expr
        if not isinstance(expr, (E.AggregateCall, E.WindowFunc)):
            raise TypeError("over() applies to an aggregate (sum, count, …) or a window function (row_number, "
                            "rank, lag, …)")
        if isinstance(expr, E.AggregateCall):
            from .analyzer import STAT_AGGREGATES

            name = expr.function.split(":")[0]
            if name in STAT_AGGREGATES or name in ("median", "percentile", "percentile_approx"):
                from .errors import UnsupportedOperationError

                raise UnsupportedOperationError(f"{name}() over a window is not supported; aggregate with groupBy() "
                                                "and join the result back")
        return Column(E.WindowExpr(expr, window._partition, window._order, window._frame))

    def substr(self, startPos: Any, length: Any) -> "Column":  # noqa: N803
        from .sql.functions import substr

        return substr(Column(self._expr), startPos, length)

    # ---------------------------------------------------------------- naming, types
    def alias(self, alias: str) -> "Column":
        if not isinstance(alias, str) or not alias:
            raise ValueError("alias() takes a non-empty string")
        expr = self._expr.child if isinstance(self._expr, E.Alias) else self._expr
        return Column(E.Alias(expr, alias))

    name = alias

    def cast(self, dataType: DataType | str) -> "Column":  # noqa: N803
        return Column(E.Cast(self._expr, parse_type(dataType)))

    astype = cast

    def try_cast(self, dataType: DataType | str) -> "Column":  # noqa: N803
        """As ``cast``, but text that isn't a valid number gives NULL instead of an error (Spark 4)."""
        return Column(E.Cast(self._expr, parse_type(dataType), lenient=True))

    # ---------------------------------------------------------------- ordering
    def asc(self) -> "Column":
        return Column(E.SortOrder(self._expr, True, None))

    def desc(self) -> "Column":
        return Column(E.SortOrder(self._expr, False, None))

    def asc_nulls_first(self) -> "Column":
        return Column(E.SortOrder(self._expr, True, True))

    def asc_nulls_last(self) -> "Column":
        return Column(E.SortOrder(self._expr, True, False))

    def desc_nulls_first(self) -> "Column":
        return Column(E.SortOrder(self._expr, False, True))

    def desc_nulls_last(self) -> "Column":
        return Column(E.SortOrder(self._expr, False, False))

    # ---------------------------------------------------------------- when / otherwise
    def when(self, condition: "Column", value: Any) -> "Column":
        if not isinstance(self._expr, E.CaseWhen) or self._expr.otherwise is not None:
            raise TypeError("when() can only be applied on a Column previously generated by when()")
        branches = self._expr.branches + ((to_expr(condition), to_expr(value)),)
        return Column(E.CaseWhen(branches, None))

    def otherwise(self, value: Any) -> "Column":
        if not isinstance(self._expr, E.CaseWhen) or self._expr.otherwise is not None:
            raise TypeError("otherwise() can only be applied once on a Column previously generated by when()")
        return Column(E.CaseWhen(self._expr.branches, to_expr(value)))

    def __repr__(self) -> str:
        from .naming import expr_name

        try:
            return f"Column<'{expr_name(self._expr)}'>"
        except Exception:  # noqa: BLE001 - repr must not fail
            return "Column<?>"
