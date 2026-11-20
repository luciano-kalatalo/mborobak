"""Expression nodes (compiler spec §4).

All nodes are immutable. User-facing nodes are built by ``Column`` and the
functions module; the analyzer turns ``UnresolvedColumn`` into ``AttrRef`` and
annotates types. Internal nodes (``Exact``, ``SqlCol`` …) are created only by
the compiler.
"""

from __future__ import annotations

import itertools
import sys
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


@dataclass(frozen=True)
class Func(Expr):
    """A scalar function call (``upper``, ``round`` …). Typing, naming and SQL come from the registry in
    ``scalar.py``; ``name`` is the registry key. Constant arguments (a scale, a pad string) stay
    ``Literal`` arguments so the default column name shows them, as Spark's does."""

    name: str
    args: tuple[Expr, ...]

    def children(self):
        return self.args


@dataclass(frozen=True)
class Coalesce(Expr):
    """Internal: the key of a full outer join on column names (compiler spec §6.3)."""

    values: tuple[Expr, ...]

    def children(self):
        return self.values


@dataclass(frozen=True)
class AggregateCall(Expr):
    function: str  # count, sum, avg, min, max
    child: Expr  # Star for count(*)
    distinct: bool = False

    def children(self):
        return (self.child,)


@dataclass(frozen=True)
class WindowFunc(Expr):
    """A function that only exists over a window: ``row_number``, ``rank``, ``lag``, ``first`` …
    (compiler spec §7.11). Constant arguments (an offset, a bucket count) are ``Literal`` arguments."""

    name: str
    args: tuple[Expr, ...] = ()

    def children(self):
        return self.args


UNBOUNDED_PRECEDING = -(1 << 63)  # PySpark's Window.unboundedPreceding
UNBOUNDED_FOLLOWING = (1 << 63) - 1
PRECEDING_LIMIT = max(-sys.maxsize, UNBOUNDED_PRECEDING)  # PySpark treats anything beyond as unbounded
FOLLOWING_LIMIT = min(sys.maxsize, UNBOUNDED_FOLLOWING)


@dataclass(frozen=True)
class Frame:
    """``rowsBetween``/``rangeBetween``: ``kind`` is "rows" or "range"; ``start``/``end`` are offsets
    from the current row, None for unbounded."""

    kind: str
    start: int | None
    end: int | None
    written: bool = field(default=False, compare=False)  # from SQL text: Spark names "1 PRECEDING" there


@dataclass(frozen=True)
class WindowExpr(Expr):
    """``function.over(window)``: an ``AggregateCall`` or a ``WindowFunc`` with its window."""

    function: Expr
    partition: tuple[Expr, ...] = ()
    order: tuple["SortOrder", ...] = ()
    frame: Frame | None = None

    def children(self):
        return (self.function, *self.partition, *self.order)


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


@dataclass(frozen=True)
class TextOrder(Expr):
    """A string compared or sorted by code point, as Spark does (compiler spec §7.1): ``%EXACT(' ' || x)``.
    With ``%EXACT`` alone IRIS orders strings that look like numbers as numbers ('10' > '9'); the
    leading space makes every value a plain string."""

    child: Expr

    def children(self):
        return (self.child,)


def _map_children(expr: Expr, go) -> Expr:
    """``expr`` with ``go`` applied to every child expression (fields, tuples, CASE branch pairs)."""
    import dataclasses

    if not dataclasses.is_dataclass(expr):
        return expr
    changes = {}
    for f in dataclasses.fields(expr):
        value = getattr(expr, f.name)
        if isinstance(value, Expr):
            new = go(value)
        elif isinstance(value, tuple) and value and all(isinstance(v, Expr) for v in value):
            new = tuple(go(v) for v in value)
        elif isinstance(value, tuple) and value and all(isinstance(v, tuple) for v in value):
            new = tuple(tuple(go(x) if isinstance(x, Expr) else x for x in v) for v in value)
        else:
            continue
        if not _same(new, value):  # identity, not ==: cheap, and NaN literals never equal themselves
            changes[f.name] = new
    return dataclasses.replace(expr, **changes) if changes else expr


def _same(a, b) -> bool:
    if isinstance(a, tuple) and isinstance(b, tuple) and len(a) == len(b):
        return all(_same(x, y) for x, y in zip(a, b))
    return a is b


def transform(expr: Expr, fn) -> Expr:
    """Bottom-up rewrite: children first, then ``fn`` on the rebuilt node."""
    return fn(_map_children(expr, lambda c: transform(c, fn)))


def transform_down(expr: Expr, fn) -> Expr:
    """Top-down rewrite: ``fn`` returns a replacement (not descended into) or None to keep going."""
    replaced = fn(expr)
    if replaced is not None:
        return replaced
    return _map_children(expr, lambda c: transform_down(c, fn))


def walk(expr: Expr):
    """Pre-order traversal."""
    yield expr
    for c in expr.children():
        yield from walk(c)


def contains_aggregate(expr: Expr) -> bool:
    """An aggregate that groups rows; ``sum(x).over(w)`` is a window function, not one."""
    if isinstance(expr, WindowExpr):
        return any(contains_aggregate(c) for c in (*expr.function.children(), *expr.partition, *expr.order))
    if isinstance(expr, AggregateCall):
        return True
    return any(contains_aggregate(c) for c in expr.children())


def contains_window(expr: Expr) -> bool:
    return any(isinstance(e, (WindowExpr, WindowFunc)) for e in walk(expr))
