"""Logical plan nodes (compiler spec §5).

Plans are immutable and already resolved: every expression in a node refers
to its child's output attributes by ``AttrRef``. Resolution happens when the
DataFrame method is called, so analysis errors surface at the operation that
caused them (API contract §1, principle 4).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import expressions as E


@dataclass(frozen=True)
class TableRef:
    schema: str
    name: str

    def quoted(self) -> str:
        return f'"{self.schema}"."{self.name}"'

    def __str__(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True, eq=False)
class Plan:
    @property
    def output(self) -> tuple[E.Attribute, ...]:
        raise NotImplementedError

    def children(self) -> tuple["Plan", ...]:
        return ()


@dataclass(frozen=True, eq=False)
class TableScan(Plan):
    table: TableRef
    attrs: tuple[E.Attribute, ...]
    unsupported: tuple[tuple[str, str], ...] = ()  # (column, IRIS type) left out of attrs

    @property
    def output(self):
        return self.attrs


@dataclass(frozen=True, eq=False)
class Project(Plan):
    child: Plan
    items: tuple[tuple[E.Attribute, E.Expr], ...]  # (output attribute, expression)

    @property
    def output(self):
        return tuple(a for a, _ in self.items)

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class Filter(Plan):
    child: Plan
    condition: E.Expr

    @property
    def output(self):
        return self.child.output

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class Aggregate(Plan):
    child: Plan
    keys: tuple[tuple[E.Attribute, E.Expr], ...]
    aggs: tuple[tuple[E.Attribute, E.Expr], ...]

    @property
    def output(self):
        return tuple(a for a, _ in self.keys) + tuple(a for a, _ in self.aggs)

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class Sort(Plan):
    child: Plan
    orders: tuple[E.SortOrder, ...]

    @property
    def output(self):
        return self.child.output

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class Limit(Plan):
    child: Plan
    n: int

    @property
    def output(self):
        return self.child.output

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class Distinct(Plan):
    child: Plan

    @property
    def output(self):
        return self.child.output

    def children(self):
        return (self.child,)


@dataclass(frozen=True, eq=False)
class CountRows(Plan):
    """Internal: ``df.count()`` wraps the plan in this node."""

    child: Plan
    attr: E.Attribute = field(default=None)  # type: ignore[assignment]

    @property
    def output(self):
        return (self.attr,)

    def children(self):
        return (self.child,)


# --------------------------------------------------------------------------- cache key


def plan_key(plan: Plan) -> tuple:
    """Structural key: equal for plans that compile to the same SQL (compiled-SQL cache, §9.6)."""
    if isinstance(plan, TableScan):
        return ("scan", str(plan.table), tuple(a.id for a in plan.attrs))
    if isinstance(plan, Project):
        return ("project", plan_key(plan.child), tuple((a.id, a.name, e) for a, e in plan.items))
    if isinstance(plan, Filter):
        return ("filter", plan_key(plan.child), plan.condition)
    if isinstance(plan, Aggregate):
        return ("aggregate", plan_key(plan.child), tuple((a.id, a.name, e) for a, e in plan.keys),
                tuple((a.id, a.name, e) for a, e in plan.aggs))
    if isinstance(plan, Sort):
        return ("sort", plan_key(plan.child), plan.orders)
    if isinstance(plan, Limit):
        return ("limit", plan_key(plan.child), plan.n)
    if isinstance(plan, Distinct):
        return ("distinct", plan_key(plan.child))
    if isinstance(plan, CountRows):
        return ("count", plan_key(plan.child))  # the output attribute is new on every call
    raise TypeError(f"no cache key for {type(plan).__name__}")


# --------------------------------------------------------------------------- printing
def _e(expr: E.Expr) -> str:
    from .naming import expr_name

    if isinstance(expr, E.Literal) and isinstance(expr.value, str):
        return repr(expr.value)
    if isinstance(expr, E.Comparison):
        return f"({_e(expr.left)} {expr.op} {_e(expr.right)})"
    if isinstance(expr, E.And):
        return f"({_e(expr.left)} AND {_e(expr.right)})"
    if isinstance(expr, E.Or):
        return f"({_e(expr.left)} OR {_e(expr.right)})"
    if isinstance(expr, E.SortOrder):
        text = f"{_e(expr.child)} {'ASC' if expr.ascending else 'DESC'}"
        if expr.nulls_first is not None:
            text += " NULLS FIRST" if expr.nulls_first else " NULLS LAST"
        return text
    return expr_name(expr)


def _named(items) -> str:
    out = []
    for attr, expr in items:
        text = _e(expr)
        out.append(text if text == attr.name else f"{text} AS {attr.name}")
    return ", ".join(out)


def describe(plan: Plan) -> str:
    if isinstance(plan, TableScan):
        return f"TableScan {plan.table} [{', '.join(a.name for a in plan.attrs)}]"
    if isinstance(plan, Project):
        return f"Project [{_named(plan.items)}]"
    if isinstance(plan, Filter):
        return f"Filter {_e(plan.condition)}"
    if isinstance(plan, Aggregate):
        return f"Aggregate [{_named(plan.keys)}] [{_named(plan.aggs)}]"
    if isinstance(plan, Sort):
        return f"Sort [{', '.join(_e(o) for o in plan.orders)}]"
    if isinstance(plan, Limit):
        return f"Limit {plan.n}"
    if isinstance(plan, Distinct):
        return "Distinct"
    if isinstance(plan, CountRows):
        return "Count"
    return type(plan).__name__


def tree_string(plan: Plan) -> str:
    lines: list[str] = []

    def visit(p: Plan, depth: int) -> None:
        prefix = "" if depth == 0 else "   " * (depth - 1) + "+- "
        lines.append(prefix + describe(p))
        for c in p.children():
            visit(c, depth + 1)

    visit(plan, 0)
    return "\n".join(lines)
