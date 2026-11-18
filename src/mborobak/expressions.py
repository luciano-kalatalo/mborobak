"""Expression nodes (compiler spec §4).

All nodes are immutable. User-facing nodes are built by ``Column`` and the
functions module; the analyzer turns ``UnresolvedColumn`` into ``AttrRef`` and
annotates types. Internal nodes (``Exact``, ``SqlCol`` …) are created only by
the compiler.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any

from .sql.types import DataType

_ids = itertools.count(1)


def new_id() -> int:
    return next(_ids)


@dataclass(frozen=True)
class Attribute:
    """A column produced by a plan node; ``id`` is unique within the process."""

    id: int
    name: str
    dtype: DataType
    nullable: bool = True
    collation: str | None = None  # "EXACT", "SQLUPPER", … for table columns; None if unknown

    def renamed(self, name: str) -> "Attribute":
        return Attribute(self.id, name, self.dtype, self.nullable, self.collation)


class Expr:
    """Base class of expression nodes."""

    def children(self) -> tuple["Expr", ...]:
        return ()


# --------------------------------------------------------------------------- user nodes
@dataclass(frozen=True)
class UnresolvedColumn(Expr):
    name: str
    plan_id: int | None = None  # set for df["x"] (compiler spec §6.1)


@dataclass(frozen=True)
class Star(Expr):
    """``"*"`` in ``select`` and ``count("*")``."""


@dataclass(frozen=True)
class AttrRef(Expr):
    attr: Attribute


@dataclass(frozen=True)
class Literal(Expr):
    value: Any
    dtype: DataType


@dataclass(frozen=True)
class Alias(Expr):
    child: Expr
    name: str

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class BinaryArithmetic(Expr):
    op: str  # + - * / %
    left: Expr
    right: Expr

    def children(self):
        return (self.left, self.right)


@dataclass(frozen=True)
class Negate(Expr):
    child: Expr

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class Comparison(Expr):
    op: str  # = <> < <= > >= <=>
    left: Expr
    right: Expr

    def children(self):
        return (self.left, self.right)


@dataclass(frozen=True)
class And(Expr):
    left: Expr
    right: Expr

    def children(self):
        return (self.left, self.right)


@dataclass(frozen=True)
class Or(Expr):
    left: Expr
    right: Expr

    def children(self):
        return (self.left, self.right)


@dataclass(frozen=True)
class Not(Expr):
    child: Expr

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class IsNull(Expr):
    child: Expr
    negated: bool = False

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class In(Expr):
    child: Expr
    values: tuple[Expr, ...]

    def children(self):
        return (self.child, *self.values)


@dataclass(frozen=True)
class Like(Expr):
    child: Expr
    pattern: Expr

    def children(self):
        return (self.child, self.pattern)


@dataclass(frozen=True)
class Cast(Expr):
    child: Expr
    to: DataType

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class CaseWhen(Expr):
    branches: tuple[tuple[Expr, Expr], ...]
    otherwise: Expr | None = None

    def children(self):
        out = [e for pair in self.branches for e in pair]
        if self.otherwise is not None:
            out.append(self.otherwise)
        return tuple(out)


AGG_FUNCTIONS = ("count", "sum", "avg", "min", "max")


@dataclass(frozen=True)
class AggregateCall(Expr):
    function: str  # count, sum, avg, min, max
    child: Expr  # Star for count(*)
    distinct: bool = False

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class SortOrder(Expr):
    child: Expr
    ascending: bool = True
    nulls_first: bool | None = None  # None = Spark default (first asc, last desc)

    def children(self):
        return (self.child,)

    @property
    def effective_nulls_first(self) -> bool:
        return self.ascending if self.nulls_first is None else self.nulls_first


# --------------------------------------------------------------------------- compiler-only nodes
@dataclass(frozen=True)
class SqlCol(Expr):
    """A column of the FROM source of a SELECT block."""

    name: str
    qualifier: str | None = None
    dtype: DataType | None = field(default=None, compare=False)
    collation: str | None = field(default=None, compare=False)


@dataclass(frozen=True)
class Exact(Expr):
    child: Expr

    def children(self):
        return (self.child,)


def walk(expr: Expr):
    """Pre-order traversal."""
    yield expr
    for c in expr.children():
        yield from walk(c)


def contains_aggregate(expr: Expr) -> bool:
    return any(isinstance(e, AggregateCall) for e in walk(expr))
