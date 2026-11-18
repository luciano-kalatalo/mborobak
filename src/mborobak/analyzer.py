"""Name resolution, typing and checks (compiler spec §6, typing rules §3)."""

from __future__ import annotations

import difflib

from . import expressions as E
from .errors import (
    AmbiguousColumnError,
    AnalysisError,
    ColumnNotFoundError,
    UnsupportedOperationError,
)
from .naming import expr_name
from .plan import Plan
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
)

# --------------------------------------------------------------------------- resolution


def qualifiers(plan: Plan) -> dict[int, set[str]]:
    """Names usable before a dot for each output column: table names and ``alias()`` names."""
    from . import plan as P

    if isinstance(plan, P.TableScan):
        return {a.id: {plan.table.name.lower()} for a in plan.attrs}
    if isinstance(plan, P.SubqueryAlias):
        return {a.id: {plan.alias.lower()} for a in plan.output}
    if isinstance(plan, P.Join):
        out = qualifiers(plan.left)
        for k, v in qualifiers(plan.right).items():
            out.setdefault(k, set()).update(v)
        return out
    if isinstance(plan, P.Project):
        child = qualifiers(plan.child)
        out = {}
        for attr, expr in plan.items:
            if isinstance(expr, E.AttrRef) and expr.attr.id in child:
                out[attr.id] = child[expr.attr.id]
        return out
    if isinstance(plan, (P.Filter, P.Sort, P.Limit, P.Distinct)):
        return qualifiers(plan.children()[0])
    return {}


def _split_name(name: str) -> tuple[str | None, str]:
    """``a.x`` → ("a", "x"); backticks protect dots: ```a.b``` → (None, "a.b")."""
    if "`" in name:
        parts = [p for p in name.split("`") if p not in ("", ".")]
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, name.replace("`", "")
    if "." in name:
        q, _, rest = name.rpartition(".")
        return q, rest
    return None, name


def find_attr(name: str, attrs: tuple[E.Attribute, ...], operation: str | None,
              quals: dict[int, set[str]] | None = None) -> E.Attribute:
    qualifier, clean = _split_name(name)
    candidates = attrs
    if qualifier is not None:
        q = qualifier.lower()
        scoped = [a for a in attrs if q in (quals or {}).get(a.id, set())]
        if scoped:
            candidates = tuple(scoped)
        else:
            clean = name.replace("`", "")  # a column whose name contains a dot
    matches = [a for a in candidates if a.name.lower() == clean.lower()]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        names = [a.name for a in attrs]
        close = difflib.get_close_matches(clean, names, n=3)
        hint = f" Did you mean: {', '.join(close)}?" if close else ""
        raise ColumnNotFoundError(
            f"column {name!r} not found among [{', '.join(names)}].{hint}", operation=operation
        )
    raise AmbiguousColumnError(
        f"column {name!r} is ambiguous: {len(matches)} columns have this name. Qualify it with "
        "df.alias('a') and col('a.name'), or use df['name'] of the DataFrame you mean.",
        operation=operation,
    )


def resolve(expr: E.Expr, plan: Plan, operation: str | None, allow_star: bool = False) -> E.Expr:
    """Replace names with attribute references against ``plan.output``."""
    attrs = plan.output
    ids = {a.id: a for a in attrs}
    quals = qualifiers(plan)

    def go(e: E.Expr) -> E.Expr:
        if isinstance(e, E.UnresolvedColumn):
            return E.AttrRef(find_attr(e.name, attrs, operation, quals))
        if isinstance(e, E.AttrRef):
            if e.attr.id in ids:
                return E.AttrRef(ids[e.attr.id])  # current nullability (outer joins)
            if e.attr.id not in ids:
                raise ColumnNotFoundError(
                    f"column {e.attr.name!r} (from df[...]) is not available here; it was removed "
                    "or belongs to another DataFrame",
                    operation=operation,
                )
            return e
        if isinstance(e, E.Star):
            if allow_star:
                return e
            raise AnalysisError("'*' is only allowed in select() and count('*')", operation=operation)
        if isinstance(e, E.Literal):
            return e
        if isinstance(e, E.AggregateCall):
            return E.AggregateCall(e.function, go(e.child) if not isinstance(e.child, E.Star) else e.child,
                                   e.distinct)
        if isinstance(e, E.Alias):
            return E.Alias(go(e.child), e.name)
        if isinstance(e, E.BinaryArithmetic):
            return E.BinaryArithmetic(e.op, go(e.left), go(e.right))
        if isinstance(e, E.Negate):
            return E.Negate(go(e.child))
        if isinstance(e, E.Comparison):
            return E.Comparison(e.op, go(e.left), go(e.right))
        if isinstance(e, E.And):
            return E.And(go(e.left), go(e.right))
        if isinstance(e, E.Or):
            return E.Or(go(e.left), go(e.right))
        if isinstance(e, E.Not):
            return E.Not(go(e.child))
        if isinstance(e, E.IsNull):
            return E.IsNull(go(e.child), e.negated)
        if isinstance(e, E.In):
            return E.In(go(e.child), tuple(go(v) for v in e.values))
        if isinstance(e, E.Like):
            return E.Like(go(e.child), go(e.pattern))
        if isinstance(e, E.Cast):
            return E.Cast(go(e.child), e.to)
        if isinstance(e, E.CaseWhen):
            return E.CaseWhen(tuple((go(c), go(v)) for c, v in e.branches),
                              go(e.otherwise) if e.otherwise is not None else None)
        if isinstance(e, E.SortOrder):
            return E.SortOrder(go(e.child), e.ascending, e.nulls_first)
        raise UnsupportedOperationError(f"unsupported expression {type(e).__name__}", operation=operation)

    resolved = go(expr)
    infer(resolved, operation)  # type check now, so the error points at this operation
    return resolved


# --------------------------------------------------------------------------- typing

_DEC_INT = DecimalType(10, 0)
_DEC_LONG = DecimalType(20, 0)


def _as_decimal(t: DataType) -> DecimalType:
    if isinstance(t, DecimalType):
        return t
    return _DEC_LONG if isinstance(t, LongType) else _DEC_INT


def _bounded_decimal(precision: int, scale: int) -> DecimalType:
    """Spark's adjustPrecisionScale: above 38 digits, keep the integer digits and cut the scale (min 6)."""
    if precision <= 38:
        return DecimalType(precision, scale)
    int_digits = precision - scale
    return DecimalType(38, max(38 - int_digits, min(scale, 6)))


def _literal_decimal(expr: E.Expr, t: DataType, other: DataType) -> DataType:
    """An integer literal next to a decimal is typed by its digits, as Spark does (``n / 2``)."""
    if isinstance(expr, E.Literal) and t.is_integral and isinstance(other, DecimalType) \
            and isinstance(expr.value, int):
        return DecimalType(max(1, len(str(abs(expr.value)))), 0)
    return t


def _numeric_result(op: str, lt: DataType, rt: DataType) -> DataType:
    if isinstance(lt, NullType):
        lt = rt
    if isinstance(rt, NullType):
        rt = lt
    if isinstance(lt, DoubleType) or isinstance(rt, DoubleType):
        return DoubleType()
    if isinstance(lt, DecimalType) or isinstance(rt, DecimalType):
        # Spark's DecimalPrecision: an integer side counts as decimal(10,0) (int) or decimal(20,0)
        # (long); an integer literal as decimal(<its digits>,0), see infer().
        a, b = _as_decimal(lt), _as_decimal(rt)
        if op == "/":
            scale = max(6, a.scale + b.precision + 1)
            precision = a.precision - a.scale + b.scale + scale
        elif op in "+-":
            scale = max(a.scale, b.scale)
            precision = max(a.precision - a.scale, b.precision - b.scale) + scale + 1
        elif op == "*":
            scale = a.scale + b.scale
            precision = a.precision + b.precision + 1
        else:  # %
            scale = max(a.scale, b.scale)
            precision = min(a.precision - a.scale, b.precision - b.scale) + scale
        return _bounded_decimal(precision, scale)
    if op == "/":
        return DoubleType()
    if isinstance(lt, LongType) or isinstance(rt, LongType):
        return LongType()
    return IntegerType()


def wider(a: DataType, b: DataType) -> DataType | None:
    """Common type for CASE branches and comparisons; None if incompatible."""
    if a == b:
        return a
    if isinstance(a, NullType):
        return b
    if isinstance(b, NullType):
        return a
    if a.is_numeric and b.is_numeric:
        if isinstance(a, DoubleType) or isinstance(b, DoubleType):
            return DoubleType()
        if isinstance(a, DecimalType) or isinstance(b, DecimalType):
            # Spark's wider decimal: enough integer and fraction digits for both, no extra digit.
            x, y = _as_decimal(a), _as_decimal(b)
            scale = max(x.scale, y.scale)
            return DecimalType(min(38, max(x.precision - x.scale, y.precision - y.scale) + scale), scale)
        return LongType() if isinstance(a, LongType) or isinstance(b, LongType) else IntegerType()
    if {type(a), type(b)} == {DateType, TimestampNTZType}:
        return TimestampNTZType()
    return None


def infer(expr: E.Expr, operation: str | None = None) -> tuple[DataType, bool]:
    """Type and nullability of a resolved expression (compiler spec §3)."""
    if isinstance(expr, E.AttrRef):
        return expr.attr.dtype, expr.attr.nullable
    if isinstance(expr, E.SqlCol):  # compiler-level expressions
        return expr.dtype or NullType(), True
    if isinstance(expr, E.Exact):
        return infer(expr.child, operation)
    if isinstance(expr, E.Literal):
        return expr.dtype, expr.value is None
    if isinstance(expr, E.Alias):
        return infer(expr.child, operation)
    if isinstance(expr, E.BinaryArithmetic):
        lt, ln = infer(expr.left, operation)
        rt, rn = infer(expr.right, operation)
        for t in (lt, rt):
            if not (t.is_numeric or isinstance(t, NullType)):
                raise AnalysisError(
                    f"arithmetic '{expr.op}' needs numbers, got {lt.simpleString()} and {rt.simpleString()}",
                    operation=operation)
        lt, rt = _literal_decimal(expr.left, lt, rt), _literal_decimal(expr.right, rt, lt)
        return _numeric_result(expr.op, lt, rt), ln or rn or expr.op in "/%"
    if isinstance(expr, E.Negate):
        t, n = infer(expr.child, operation)
        if not t.is_numeric:
            raise AnalysisError(f"cannot negate {t.simpleString()}", operation=operation)
        return t, n
    if isinstance(expr, E.Comparison):
        lt, ln = infer(expr.left, operation)
        rt, rn = infer(expr.right, operation)
        if wider(lt, rt) is None:
            raise AnalysisError(f"cannot compare {lt.simpleString()} with {rt.simpleString()}",
                                operation=operation)
        return BooleanType(), (ln or rn) and expr.op != "<=>"
    if isinstance(expr, (E.And, E.Or)):
        nullable = False
        for c in expr.children():
            t, n = infer(c, operation)
            if not isinstance(t, (BooleanType, NullType)):
                raise AnalysisError(f"AND/OR needs booleans, got {t.simpleString()}", operation=operation)
            nullable |= n
        return BooleanType(), nullable
    if isinstance(expr, E.Not):
        t, n = infer(expr.child, operation)
        if not isinstance(t, (BooleanType, NullType)):
            raise AnalysisError(f"NOT needs a boolean, got {t.simpleString()}", operation=operation)
        return BooleanType(), n
    if isinstance(expr, E.IsNull):
        infer(expr.child, operation)
        return BooleanType(), False
    if isinstance(expr, E.In):
        ct, cn = infer(expr.child, operation)
        for v in expr.values:
            vt, _ = infer(v, operation)
            if wider(ct, vt) is None:
                raise AnalysisError(f"isin(): cannot compare {ct.simpleString()} with {vt.simpleString()}",
                                    operation=operation)
        return BooleanType(), cn
    if isinstance(expr, E.Like):
        ct, cn = infer(expr.child, operation)
        if not isinstance(ct, (StringType, NullType)):
            raise AnalysisError(f"like() needs a string column, got {ct.simpleString()}", operation=operation)
        return BooleanType(), cn
    if isinstance(expr, E.Cast):
        ct, cn = infer(expr.child, operation)
        to = expr.to
        allowed = (
            ct == to
            or isinstance(ct, NullType)
            or (ct.is_numeric and (to.is_numeric or isinstance(to, (StringType, BooleanType))))
            or (isinstance(ct, StringType) and (to.is_numeric or isinstance(to, (DateType, TimestampNTZType,
                                                                                  StringType))))
            or (isinstance(ct, (DateType, TimestampNTZType)) and isinstance(to, (StringType, DateType,
                                                                                  TimestampNTZType)))
            or (isinstance(ct, BooleanType) and (to.is_numeric or isinstance(to, StringType)))
        )
        if not allowed:
            raise UnsupportedOperationError(
                f"cast from {ct.simpleString()} to {to.simpleString()} is not supported", operation=operation)
        if isinstance(ct, StringType) and isinstance(to, BooleanType):
            raise UnsupportedOperationError("cast from string to boolean is not in P0", operation=operation)
        return to, cn
    if isinstance(expr, E.CaseWhen):
        result: DataType = NullType()
        nullable = expr.otherwise is None
        for cond, value in expr.branches:
            t, _ = infer(cond, operation)
            if not isinstance(t, (BooleanType, NullType)):
                raise AnalysisError("when() condition must be boolean", operation=operation)
            vt, vn = infer(value, operation)
            w = wider(result, vt)
            if w is None:
                raise AnalysisError(f"when() branches have different types: {result.simpleString()} and "
                                    f"{vt.simpleString()}", operation=operation)
            result, nullable = w, nullable or vn
        if expr.otherwise is not None:
            ot, on = infer(expr.otherwise, operation)
            w = wider(result, ot)
            if w is None:
                raise AnalysisError(f"otherwise() type {ot.simpleString()} differs from "
                                    f"{result.simpleString()}", operation=operation)
            result, nullable = w, nullable or on
        return result, nullable
    if isinstance(expr, E.AggregateCall):
        if any(isinstance(c, E.AggregateCall) for c in E.walk(expr.child)):
            raise AnalysisError("aggregate functions cannot be nested", operation=operation)
        if expr.function == "count":
            if not isinstance(expr.child, E.Star):
                infer(expr.child, operation)
            return LongType(), False
        ct, _ = infer(expr.child, operation)
        if expr.function == "sum":
            if ct.is_integral:
                return LongType(), True
            if isinstance(ct, DecimalType):
                return DecimalType(min(38, ct.precision + 10), ct.scale), True
            if isinstance(ct, DoubleType):
                return DoubleType(), True
            raise AnalysisError(f"sum() needs a number, got {ct.simpleString()}", operation=operation)
        if expr.function == "avg":
            if ct.is_integral or isinstance(ct, DoubleType):
                return DoubleType(), True
            if isinstance(ct, DecimalType):
                return DecimalType(min(38, ct.precision + 4), min(38, ct.scale + 4)), True
            raise AnalysisError(f"avg() needs a number, got {ct.simpleString()}", operation=operation)
        return ct, True  # min, max
    if isinstance(expr, E.SortOrder):
        return infer(expr.child, operation)
    if isinstance(expr, E.Coalesce):
        result: DataType = NullType()
        nullable = True
        for v in expr.values:
            t, n = infer(v, operation)
            result = wider(result, t) or result
            nullable = nullable and n
        return result, nullable
    if isinstance(expr, E.Star):
        raise AnalysisError("'*' is not an expression here", operation=operation)
    raise UnsupportedOperationError(f"cannot type {type(expr).__name__}", operation=operation)


# --------------------------------------------------------------------------- output attributes


def output_name(original: E.Expr) -> str:
    return expr_name(original)


def unique_ids(items: list[tuple[E.Attribute, E.Expr]]) -> list[tuple[E.Attribute, E.Expr]]:
    """Every output column of one node gets its own id: a column listed twice (``select("i", col("i")
    .alias("j"))``) keeps the input's id the first time and gets a new one after. Compilation keys
    subquery names, casts and substitutions by id, so a shared id would merge the two columns."""
    seen: set[int] = set()
    out = []
    for attr, expr in items:
        if attr.id in seen:
            attr = E.Attribute(E.new_id(), attr.name, attr.dtype, attr.nullable, attr.collation)
        seen.add(attr.id)
        out.append((attr, expr))
    return out


def make_attr(name: str, resolved: E.Expr, operation: str | None) -> tuple[E.Attribute, E.Expr]:
    """Output attribute for a projected expression; plain column refs keep their id."""
    expr = resolved.child if isinstance(resolved, E.Alias) else resolved
    if isinstance(expr, E.AttrRef):
        return expr.attr.renamed(name), expr
    dtype, nullable = infer(expr, operation)
    if isinstance(dtype, NullType):
        dtype = StringType()  # lit(None) without a cast: shown as a NULL string column
    return E.Attribute(E.new_id(), name, dtype, nullable), expr


def check_no_aggregate(expr: E.Expr, operation: str | None, where: str) -> None:
    if E.contains_aggregate(expr):
        raise AnalysisError(f"aggregate functions are not allowed in {where}; use groupBy().agg() "
                            "or agg()", operation=operation)


def check_grouped(expr: E.Expr, keys: list[E.Expr], operation: str | None) -> None:
    """Every column outside an aggregate must be a grouping expression (§6.2)."""

    def go(e: E.Expr) -> None:
        if isinstance(e, E.AggregateCall):
            return
        if any(e == k for k in keys):
            return
        if isinstance(e, E.AttrRef):
            raise AnalysisError(
                f"column {e.attr.name!r} is neither grouped nor aggregated; add it to groupBy() or wrap it "
                "in an aggregate function", operation=operation)
        for c in e.children():
            go(c)

    go(expr)

