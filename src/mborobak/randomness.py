"""Random values per row: ``rand``, ``randn`` and the sampling built on them (compiler spec §7.20).

IRIS SQL has no RAND, and a per-call random number would not do anyway: ``randomSplit`` runs one query per
split, so a row must get the same value in every query, or splits overlap and lose rows. Spark gets that from
the seed and the row's position in its partition. Mborobak hashes the seed with the row itself:

    CRC32(MD5('<seed>' || CHAR(1) || <column 1> || CHAR(1) || …)) / 2**32

MD5 makes values of different seeds independent (CRC32 alone is linear: two seeds' values were strongly
related in the probe); CRC32 turns the hash into a number. Both are native in IRIS SQL. Identical rows would
get identical values, so where the rows can't be shown to be distinct, a ``ROW_NUMBER()`` over all columns
numbers the copies, and the number is part of the hash. That costs about 3 s per million rows (probe
2026-11-21); rows are known distinct after ``distinct()``, ``groupBy()``, or over a table with a primary key
whose columns are still there.

Before compiling, ``bind`` rewrites every ``rand``/``randn`` to ``rand_row``, whose arguments are the seed,
the kind, and the input row's columns.
"""

from __future__ import annotations

import dataclasses
import random

from . import expressions as E
from . import plan as P
from .errors import UnsupportedOperationError
from .sql.types import LongType, StringType

RANDOM = ("rand", "randn")


def new_seed() -> int:
    """Spark's default seed for ``rand()``: a random long, shown in the column name."""
    return random.randint(-(2**63), 2**63 - 1)


def _has_random(expr: E.Expr) -> bool:
    return any(isinstance(e, E.Func) and e.name in RANDOM for e in E.walk(expr))


# --------------------------------------------------------------------------- distinct rows

def unique_keys(plan: P.Plan) -> list[frozenset[int]]:
    """Sets of output column ids, each unique across the plan's rows (an empty set: at most one row)."""
    if isinstance(plan, P.TableScan):
        return [frozenset(plan.key)] if plan.key else []
    if isinstance(plan, (P.Filter, P.Sort, P.Limit, P.SubqueryAlias)):
        return unique_keys(plan.child)
    if isinstance(plan, P.Distinct):
        return [frozenset(a.id for a in plan.output)]
    if isinstance(plan, P.Aggregate):
        return [frozenset(a.id for a, _ in plan.keys)]
    if isinstance(plan, P.Project):
        renamed: dict[int, int] = {}
        for a, e in plan.items:
            if isinstance(e, E.AttrRef):
                renamed.setdefault(e.attr.id, a.id)
        return [frozenset(renamed[i] for i in k) for k in unique_keys(plan.child) if k <= renamed.keys()]
    if isinstance(plan, P.Join):
        left = unique_keys(plan.left)
        if plan.join_type in ("semi", "anti"):
            return left
        if {a.id for a in plan.left.output} & {a.id for a in plan.right.output}:
            return []  # a self-join: both sides share the scan's ids
        return [k1 | k2 for k1 in left for k2 in unique_keys(plan.right)]
    return []


def distinct_rows(plan: P.Plan) -> bool:
    out = {a.id for a in plan.output}
    return any(k <= out for k in unique_keys(plan))


# --------------------------------------------------------------------------- binding

def bind(plan: P.Plan) -> P.Plan:
    """``plan`` with every random function bound to the columns of the row it is computed for."""
    children = plan.children()
    if not children:
        return plan
    if isinstance(plan, P.Join):
        new = dataclasses.replace(plan, left=bind(plan.left), right=bind(plan.right))
        if plan.condition is not None and _has_random(plan.condition):
            raise UnsupportedOperationError("rand() and randn() can't be used in a join condition")
        return new
    if isinstance(plan, P.Union):
        return dataclasses.replace(plan, inputs=tuple(bind(c) for c in plan.inputs))
    child = bind(plan.child)
    plan = dataclasses.replace(plan, child=child) if child is not plan.child else plan
    exprs = _expressions(plan)
    if not any(_has_random(e) for e in exprs):
        return plan

    keys = [E.AttrRef(a) for a in child.output]
    source = child
    if not distinct_rows(child):
        number = E.Attribute(E.new_id(), "mborobak_rn", LongType(), False)
        window = E.WindowExpr(E.WindowFunc("copy_number"), tuple(keys))  # ROW_NUMBER without an order
        source = P.Project(child, tuple((a, E.AttrRef(a)) for a in child.output) + ((number, window),))
        keys.append(E.AttrRef(number))

    def rule(e: E.Expr) -> E.Expr:
        if isinstance(e, E.Func) and e.name in RANDOM:
            return E.Func("rand_row", (e.args[0], E.Literal(e.name, StringType()), *keys))
        return e

    bound = _with_expressions(plan, [E.transform(e, rule) for e in exprs], source)
    if source is not child and not isinstance(plan, (P.Project, P.Aggregate)):
        bound = P.Project(bound, tuple((a, E.AttrRef(a)) for a in plan.output))  # drop the row number
    return bound


def _expressions(plan: P.Plan) -> list[E.Expr]:
    if isinstance(plan, P.Project):
        return [e for _, e in plan.items]
    if isinstance(plan, P.Filter):
        return [plan.condition]
    if isinstance(plan, P.Sort):
        return list(plan.orders)
    if isinstance(plan, P.Aggregate):
        return [e for _, e in plan.keys] + [e for _, e in plan.aggs]
    return []


def _with_expressions(plan: P.Plan, exprs: list[E.Expr], child: P.Plan) -> P.Plan:
    if isinstance(plan, P.Project):
        return P.Project(child, tuple((a, e) for (a, _), e in zip(plan.items, exprs)))
    if isinstance(plan, P.Filter):
        return P.Filter(child, exprs[0])
    if isinstance(plan, P.Sort):
        return P.Sort(child, tuple(exprs))  # type: ignore[arg-type]
    if isinstance(plan, P.Aggregate):
        n = len(plan.keys)
        return P.Aggregate(child, tuple((a, e) for (a, _), e in zip(plan.keys, exprs[:n])),
                           tuple((a, e) for (a, _), e in zip(plan.aggs, exprs[n:])))
    raise AssertionError(type(plan).__name__)
