"""Column functions (API contract §6.4). Import as ``F``."""

from __future__ import annotations

from typing import Any

from .. import expressions as E
from ..column import Column, literal, to_column_expr, to_expr

__all__ = [
    "col",
    "column",
    "lit",
    "when",
    "count",
    "countDistinct",
    "count_distinct",
    "sum",
    "avg",
    "mean",
    "min",
    "max",
    "asc",
    "desc",
    "asc_nulls_first",
    "asc_nulls_last",
    "desc_nulls_first",
    "desc_nulls_last",
]


def col(name: str) -> Column:
    if not isinstance(name, str):
        raise TypeError("col() takes a column name")
    return Column(E.UnresolvedColumn(name))


column = col


def lit(value: Any) -> Column:
    if isinstance(value, Column):
        return value
    return Column(literal(value))


def when(condition: Column, value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("when() takes a Column condition")
    return Column(E.CaseWhen(((condition._expr, to_expr(value)),), None))


def _agg(function: str, c: Column | str, distinct: bool = False) -> Column:
    return Column(E.AggregateCall(function, to_column_expr(c), distinct))


def count(c: Column | str) -> Column:
    expr = to_column_expr(c)
    if isinstance(expr, E.Literal):
        expr = E.Star()  # count(lit(1)) counts rows
    return Column(E.AggregateCall("count", expr))


def countDistinct(c: Column | str, *cols: Column | str) -> Column:  # noqa: N802
    if cols:
        raise NotImplementedError("countDistinct over several columns is not in the prototype")
    return _agg("count", c, distinct=True)


count_distinct = countDistinct


def sum(c: Column | str) -> Column:  # noqa: A001 - PySpark name
    return _agg("sum", c)


def avg(c: Column | str) -> Column:
    return _agg("avg", c)


mean = avg


def min(c: Column | str) -> Column:  # noqa: A001
    return _agg("min", c)


def max(c: Column | str) -> Column:  # noqa: A001
    return _agg("max", c)


def asc(name: str) -> Column:
    return col(name).asc()


def desc(name: str) -> Column:
    return col(name).desc()


def asc_nulls_first(name: str) -> Column:
    return col(name).asc_nulls_first()


def asc_nulls_last(name: str) -> Column:
    return col(name).asc_nulls_last()


def desc_nulls_first(name: str) -> Column:
    return col(name).desc_nulls_first()


def desc_nulls_last(name: str) -> Column:
    return col(name).desc_nulls_last()
