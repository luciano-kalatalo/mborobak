"""Default output names of unaliased expressions (API contract §6.5)."""

from __future__ import annotations

import datetime

from . import expressions as E


def _lit_text(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):  # Spark: TIMESTAMP '2026-01-01 10:00:00.5'
        text = value.isoformat(sep=" ")
        return f"TIMESTAMP '{text.rstrip('0') if '.' in text else text}'"
    if isinstance(value, datetime.date):
        return f"DATE '{value.isoformat()}'"
    return str(value)


def expr_name(expr: E.Expr) -> str:
    """The name PySpark would give ``expr``."""
    if isinstance(expr, E.Alias):
        return expr.name
    if isinstance(expr, E.UnresolvedColumn):
        from .analyzer import _split_name

        return _split_name(expr.name)[1]
    if isinstance(expr, E.AttrRef):
        return expr.attr.name
    if isinstance(expr, E.Star):
        return "*"
    if isinstance(expr, E.Literal):
        return _lit_text(expr.value)
    if isinstance(expr, E.BinaryArithmetic):
        return f"({expr_name(expr.left)} {expr.op} {expr_name(expr.right)})"
    if isinstance(expr, E.Negate):
        return f"(- {expr_name(expr.child)})"
    if isinstance(expr, E.Comparison):
        if expr.op == "<>":  # Spark writes a != b as NOT (a = b)
            return f"(NOT ({expr_name(expr.left)} = {expr_name(expr.right)}))"
        return f"({expr_name(expr.left)} {expr.op} {expr_name(expr.right)})"
    if isinstance(expr, E.And):
        return f"({expr_name(expr.left)} AND {expr_name(expr.right)})"
    if isinstance(expr, E.Or):
        return f"({expr_name(expr.left)} OR {expr_name(expr.right)})"
    if isinstance(expr, E.Not):
        return f"(NOT {expr_name(expr.child)})"
    if isinstance(expr, E.IsNull):
        return f"({expr_name(expr.child)} IS {'NOT ' if expr.negated else ''}NULL)"
    if isinstance(expr, E.In):
        return f"({expr_name(expr.child)} IN ({', '.join(expr_name(v) for v in expr.values)}))"
    if isinstance(expr, E.Like):
        return f"{expr_name(expr.child)} LIKE {expr_name(expr.pattern)}"
    if isinstance(expr, E.Cast):
        child = expr.child
        if expr.lenient:
            return f"TRY_CAST({expr_name(child)} AS {expr.to.simpleString().upper()})"
        if isinstance(child, (E.UnresolvedColumn, E.AttrRef)):
            return expr_name(child)  # plain cast of a column keeps its name (API contract A1)
        return f"CAST({expr_name(child)} AS {expr.to.simpleString().upper()})"
    if isinstance(expr, E.CaseWhen):
        parts = " ".join(f"WHEN {expr_name(c)} THEN {expr_name(v)}" for c, v in expr.branches)
        tail = f" ELSE {expr_name(expr.otherwise)}" if expr.otherwise is not None else ""
        return f"CASE {parts}{tail} END"
    if isinstance(expr, E.AggregateCall):
        if isinstance(expr.child, E.Star):
            return "count(1)"
        inner = expr_name(expr.child)
        if ":" in expr.function:  # percentile:0.25 → percentile(x, 0.25, 1)
            fn, p = expr.function.split(":")
            return f"{fn}({inner}, {p}, {'10000' if fn == 'percentile_approx' else '1'})"
        return f"{expr.function}({'DISTINCT ' if expr.distinct else ''}{inner})"
    if isinstance(expr, E.SortOrder):
        return expr_name(expr.child)
    if isinstance(expr, E.Coalesce):
        return f"coalesce({', '.join(expr_name(v) for v in expr.values)})"
    if isinstance(expr, E.Func):
        from . import scalar

        return scalar.name(expr, expr_name)
    if isinstance(expr, E.WindowExpr):
        from . import windows

        return windows.name(expr, expr_name)
    if isinstance(expr, E.WindowFunc):
        from . import windows

        return windows.function_name(expr, expr_name)
    raise TypeError(f"no name for {type(expr).__name__}")
