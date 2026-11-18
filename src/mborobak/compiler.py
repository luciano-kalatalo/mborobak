"""Plan → IRIS SQL (compiler spec §7 required translations, §9 SQL generation).

The generator walks the plan bottom-up and builds ``Block``s (one SELECT
each). A plan node either merges into the current block or wraps it as a
derived table (§9.1–9.2). Expressions are kept as trees until the very end;
``Renderer`` turns them into text and collects parameters in text order.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from . import expressions as E
from . import plan as P
from .analyzer import infer
from .errors import PlanTooLargeError, UnsupportedOperationError
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
    StructField,
    StructType,
    TimestampNTZType,
)

DOUBLE_MAX = "1.7976931348623157E308"
INTEGER_PATTERN = "0.1(1\"-\",1\"+\")1.N"  # compiler spec §7.7


@dataclass(frozen=True)
class CompileOptions:
    string_semantics: str = "pyspark"  # D10
    max_parameters: int = 900


@dataclass
class SqlStatement:
    text: str
    params: list
    schema: StructType
    translations: list[str]
    fingerprint: str


# --------------------------------------------------------------------------- blocks


@dataclass
class Block:
    source: object  # TableRef or Block
    alias: str | None = None
    env: dict[int, E.Expr] = field(default_factory=dict)
    select: list[E.Attribute] = field(default_factory=list)
    where: list[E.Expr] = field(default_factory=list)
    group_by: list[E.Expr] | None = None
    having: list[E.Expr] = field(default_factory=list)
    order: list[E.SortOrder] = field(default_factory=list)  # plan-level keys, rendered through env
    top: int | None = None
    distinct: bool = False

    @property
    def grouped(self) -> bool:
        return self.group_by is not None


class Compiler:
    def __init__(self, options: CompileOptions):
        self.options = options
        self.translations: list[str] = []
        self._alias_counter = 0

    # ---------------------------------------------------------------- D10 collation
    def needs_exact(self, expr: E.Expr) -> bool:
        if self.options.string_semantics != "pyspark":
            return False
        if isinstance(expr, (E.Literal, E.Exact)):
            return False
        dtype, _ = infer(expr)
        if not isinstance(dtype, StringType):
            return False
        if isinstance(expr, E.AttrRef) and expr.attr.collation == "EXACT":
            return False
        return True

    def exact(self, expr: E.Expr) -> E.Expr:
        if self.needs_exact(expr):
            self._note_exact(expr)
            return E.Exact(expr)
        return expr

    def _note_exact(self, expr: E.Expr) -> None:
        if isinstance(expr, E.AttrRef):
            note = f"collation: %EXACT on {expr.attr.name} (string, {expr.attr.collation or 'not EXACT'})"
        else:
            note = "collation: %EXACT on a string expression"
        if note not in self.translations:
            self.translations.append(note)

    def translate(self, expr: E.Expr) -> E.Expr:
        """Insert ``Exact`` where collation changes the result (§7.1)."""
        t = self.translate
        if isinstance(expr, E.Comparison):
            return E.Comparison(expr.op, self.exact(t(expr.left)), self.exact(t(expr.right)))
        if isinstance(expr, E.In):
            return E.In(self.exact(t(expr.child)), tuple(t(v) for v in expr.values))
        if isinstance(expr, E.Like):
            return E.Like(self.exact(t(expr.child)), expr.pattern)
        if isinstance(expr, E.AggregateCall):
            child = expr.child if isinstance(expr.child, E.Star) else t(expr.child)
            if expr.function in ("min", "max") or expr.distinct:
                child = self.exact(child) if not isinstance(child, E.Star) else child
            return E.AggregateCall(expr.function, child, expr.distinct)
        if isinstance(expr, E.Alias):
            return E.Alias(t(expr.child), expr.name)
        if isinstance(expr, E.BinaryArithmetic):
            return E.BinaryArithmetic(expr.op, t(expr.left), t(expr.right))
        if isinstance(expr, E.Negate):
            return E.Negate(t(expr.child))
        if isinstance(expr, E.And):
            return E.And(t(expr.left), t(expr.right))
        if isinstance(expr, E.Or):
            return E.Or(t(expr.left), t(expr.right))
        if isinstance(expr, E.Not):
            return E.Not(t(expr.child))
        if isinstance(expr, E.IsNull):
            return E.IsNull(t(expr.child), expr.negated)
        if isinstance(expr, E.Cast):
            return E.Cast(t(expr.child), expr.to)
        if isinstance(expr, E.CaseWhen):
            return E.CaseWhen(tuple((t(c), t(v)) for c, v in expr.branches),
                              t(expr.otherwise) if expr.otherwise is not None else None)
        return expr

    # ---------------------------------------------------------------- substitution
    @staticmethod
    def substitute(expr: E.Expr, env: dict[int, E.Expr]) -> E.Expr:
        def go(e: E.Expr) -> E.Expr:
            if isinstance(e, E.AttrRef):
                return env[e.attr.id]
            if isinstance(e, E.Alias):
                return go(e.child)
            if isinstance(e, E.Exact):
                inner = go(e.child)
                return inner if isinstance(inner, E.Exact) else E.Exact(inner)
            if isinstance(e, (E.Literal, E.SqlCol, E.Star)):
                return e
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
            if isinstance(e, E.AggregateCall):
                return E.AggregateCall(e.function, go(e.child), e.distinct)
            raise TypeError(f"cannot substitute {type(e).__name__}")

        return go(expr)

    def sql(self, expr: E.Expr, block: Block) -> E.Expr:
        """Plan-level expression → SQL-level expression of ``block``."""
        return self.substitute(self.translate(expr), block.env)

    # ---------------------------------------------------------------- blocks
    def _new_alias(self) -> str:
        self._alias_counter += 1
        return f"t{self._alias_counter}"

    def scan(self, node: P.TableScan) -> Block:
        b = Block(source=node.table)
        for a in node.attrs:
            b.env[a.id] = E.SqlCol(a.name, None, a.dtype, a.collation)
        b.select = list(node.attrs)
        return b

    def wrap(self, inner: Block, keep_order: bool) -> Block:
        names = [a.name.lower() for a in inner.select]
        if len(set(names)) != len(names):
            raise UnsupportedOperationError(
                "this plan needs a subquery, and its columns have duplicate names; rename them first "
                "(prototype limitation)")
        alias = self._new_alias()
        outer = Block(source=inner, alias=alias)
        for a in inner.select:
            outer.env[a.id] = E.SqlCol(a.name, alias, a.dtype, a.collation)
        outer.select = list(inner.select)
        exposed = {a.id for a in inner.select}
        if inner.order:
            movable = all(r.attr.id in exposed for o in inner.order for r in E.walk(o.child)
                          if isinstance(r, E.AttrRef))
            if keep_order and movable:
                outer.order = list(inner.order)  # re-apply the order outside (§7.2)
            if inner.top is None:
                inner.order = []  # ORDER BY without TOP is rejected in a subquery (§7.2)
        return outer

    def build(self, node: P.Plan) -> Block:
        if isinstance(node, P.TableScan):
            return self.scan(node)

        if isinstance(node, P.Filter):
            b = self.build(node.child)
            if b.top is not None or b.distinct:
                b = self.wrap(b, keep_order=True)
            cond = self.sql(node.condition, b)
            (b.having if b.grouped else b.where).append(cond)
            return b

        if isinstance(node, P.Project):
            b = self.build(node.child)
            if b.distinct:
                b = self.wrap(b, keep_order=True)
            for attr, expr in node.items:
                b.env[attr.id] = self.sql(expr, b)
            b.select = [a for a, _ in node.items]
            return b

        if isinstance(node, P.Aggregate):
            b = self.build(node.child)
            if b.grouped or b.top is not None or b.distinct:
                b = self.wrap(b, keep_order=False)
            b.order = []  # aggregation destroys order (§7.2)
            keys = []
            for attr, expr in node.keys:
                k = self.substitute(self.exact(self.translate(expr)), b.env)
                keys.append(k)
            new_env = dict(b.env)
            for (attr, _), k in zip(node.keys, keys):
                new_env[attr.id] = k
            for attr, expr in node.aggs:
                new_env[attr.id] = self.sql(expr, b)
            b.env = new_env
            b.group_by = keys
            b.select = [a for a, _ in node.keys] + [a for a, _ in node.aggs]
            return b

        if isinstance(node, P.Sort):
            b = self.build(node.child)
            if b.top is not None:
                b = self.wrap(b, keep_order=False)
            b.order = list(node.orders)
            return b

        if isinstance(node, P.Limit):
            b = self.build(node.child)
            b.top = node.n if b.top is None else min(b.top, node.n)
            return b

        if isinstance(node, P.Distinct):
            b = self.build(node.child)
            if b.top is not None or b.distinct:
                b = self.wrap(b, keep_order=False)
            b.order = [] if b.top is None else b.order
            b.distinct = True
            return b

        if isinstance(node, P.CountRows):
            inner = self.build(node.child)
            outer = self.wrap(inner, keep_order=False)
            outer.order = []
            outer.env[node.attr.id] = E.AggregateCall("count", E.Star())
            outer.group_by = []
            outer.select = [node.attr]
            return outer

        raise UnsupportedOperationError(f"cannot compile {type(node).__name__}")

    # ---------------------------------------------------------------- entry point
    def compile(self, plan: P.Plan) -> SqlStatement:
        block = self.build(plan)
        renderer = Renderer(self)
        text = renderer.block(block, top_level=True)
        if len(renderer.params) > self.options.max_parameters:
            raise PlanTooLargeError(
                f"the query needs {len(renderer.params)} parameters; the limit is "
                f"{self.options.max_parameters} (IRIS fails near 999). Shorten long isin() lists.")
        schema = StructType([StructField(a.name, a.dtype, a.nullable) for a in plan.output])
        fingerprint = hashlib.sha256(text.encode()).hexdigest()[:16]
        return SqlStatement(text, renderer.params, schema, list(self.translations), fingerprint)


# --------------------------------------------------------------------------- rendering


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_type(t: DataType) -> str:
    if isinstance(t, IntegerType):
        return "INTEGER"
    if isinstance(t, LongType):
        return "BIGINT"
    if isinstance(t, DoubleType):
        return "DOUBLE"
    if isinstance(t, DecimalType):
        return f"NUMERIC({t.precision},{t.scale})"
    if isinstance(t, StringType):
        return "VARCHAR(32767)"
    if isinstance(t, BooleanType):
        return "BIT"
    if isinstance(t, DateType):
        return "DATE"
    if isinstance(t, TimestampNTZType):
        return "TIMESTAMP"
    raise UnsupportedOperationError(f"no IRIS type for {t.simpleString()}")


class Renderer:
    def __init__(self, compiler: Compiler):
        self.c = compiler
        self.params: list = []

    # ---------------------------------------------------------------- statements
    def block(self, b: Block, top_level: bool, indent: str = "") -> str:
        head = "SELECT "
        if b.distinct:
            head += "DISTINCT "
        if b.top is not None:
            head += "TOP ? "
            self.params.append(b.top)
        items = []
        for a in b.select:
            expr = b.env[a.id]
            if b.distinct and self.c.needs_exact(E.AttrRef(a)) and not isinstance(expr, E.Exact):
                self.c._note_exact(E.AttrRef(a))
                expr = E.Exact(expr)
            text = self.value(expr)
            plain = isinstance(expr, E.SqlCol) and expr.name == a.name
            items.append(text if plain else f"{text} AS {quote(a.name)}")
        lines = [head + ", ".join(items)]
        if isinstance(b.source, Block):
            inner = self.block(b.source, top_level=False, indent=indent + "  ")
            lines.append(f"FROM (\n{indent}  {inner}\n{indent}) AS {b.alias}")
        else:
            lines.append(f"FROM {b.source.quoted()}")
        if b.where:
            lines.append("WHERE " + " AND ".join(self.pred(w) for w in b.where))
        if b.group_by:
            lines.append("GROUP BY " + ", ".join(self.value(k) for k in b.group_by))
        if b.having:
            lines.append("HAVING " + " AND ".join(self.pred(h) for h in b.having))
        if b.order and (top_level or b.top is not None):
            lines.append("ORDER BY " + ", ".join(self.order_key(o, b) for o in b.order))
        return f"\n{indent}".join(lines)

    def order_key(self, order: E.SortOrder, b: Block) -> str:
        key_plan = order.child
        dtype, _ = infer(key_plan)
        key = self.c.substitute(self.c.exact(self.c.translate(key_plan)), b.env)
        direction = "" if order.ascending else " DESC"
        default_nulls = order.nulls_first is None or order.nulls_first == order.ascending

        def k() -> str:  # each use renders the key again, so parameters stay in text order
            return self.value(key)

        if isinstance(dtype, DoubleType):
            # §7.9: bucket key restores Spark's order around IRIS's -1E19 NULL marker.
            null_bucket = 0 if default_nulls else 5
            name = key_plan.attr.name if isinstance(key_plan, E.AttrRef) else "an expression"
            note = f"double sort: bucket key on {name}"
            if note not in self.c.translations:
                self.c.translations.append(note)
            bucket = (f"CASE WHEN {k()} IS NULL THEN {null_bucket} WHEN {k()} < -{DOUBLE_MAX} THEN 1 "
                      f"WHEN {k()} <> {k()} THEN 4 WHEN {k()} > {DOUBLE_MAX} THEN 3 ELSE 2 END")
            return f"{bucket}{direction}, {k()} + 0{direction}"
        if not default_nulls:
            first = order.nulls_first
            flag = f"CASE WHEN {k()} IS NULL THEN {0 if first else 1} ELSE {1 if first else 0} END"
            return f"{flag}, {k()}{direction}"
        return k() + direction

    # ---------------------------------------------------------------- expressions
    def lit(self, e: E.Literal) -> str:
        if e.value is None:
            return "NULL"
        value = e.value
        if isinstance(value, bool):
            value = int(value)
        self.params.append(value)
        return "?"

    def value(self, e: E.Expr) -> str:
        if isinstance(e, E.SqlCol):
            return f"{e.qualifier}.{quote(e.name)}" if e.qualifier else quote(e.name)
        if isinstance(e, E.Literal):
            return self.lit(e)
        if isinstance(e, E.Exact):
            return f"%EXACT({self.value(e.child)})"
        if isinstance(e, E.BinaryArithmetic):
            lt, _ = infer(e.left)
            rt, _ = infer(e.right)
            if e.op == "%":
                return f"MOD({self.value(e.left)}, {self.value(e.right)})"
            if e.op == "/" and not (isinstance(lt, DecimalType) and isinstance(rt, DecimalType)):
                return f"(CAST({self.value(e.left)} AS DOUBLE) / {self.value(e.right)})"
            return f"({self.value(e.left)} {e.op} {self.value(e.right)})"
        if isinstance(e, E.Negate):
            return f"(-{self.value(e.child)})"
        if isinstance(e, E.Cast):
            return self.cast(e)
        if isinstance(e, E.CaseWhen):
            parts = " ".join(f"WHEN {self.pred(c)} THEN {self.value(v)}" for c, v in e.branches)
            tail = f" ELSE {self.value(e.otherwise)}" if e.otherwise is not None else ""
            return f"CASE {parts}{tail} END"
        if isinstance(e, E.AggregateCall):
            fn = e.function.upper()
            if isinstance(e.child, E.Star):
                return f"{fn}(*)"
            return f"{fn}({'DISTINCT ' if e.distinct else ''}{self.value(e.child)})"
        if isinstance(e, (E.Comparison, E.And, E.Or, E.Not, E.IsNull, E.In, E.Like)):
            # §7.8: booleans in the SELECT list become CASE; NULL stays NULL.
            p = self.pred(e)
            n = self.pred(E.Not(e))
            return f"CASE WHEN {p} THEN 1 WHEN {n} THEN 0 END"
        raise UnsupportedOperationError(f"cannot render {type(e).__name__}")

    def pred(self, e: E.Expr) -> str:
        if isinstance(e, E.Comparison):
            if e.op == "<=>":
                l1, r1 = self.value(e.left), self.value(e.right)
                l2, r2 = self.value(e.left), self.value(e.right)
                return f"(CASE WHEN {l1} = {r1} OR ({l2} IS NULL AND {r2} IS NULL) THEN 1 ELSE 0 END = 1)"
            return f"{self.value(e.left)} {e.op} {self.value(e.right)}"
        if isinstance(e, E.And):
            return f"({self.pred(e.left)} AND {self.pred(e.right)})"
        if isinstance(e, E.Or):
            return f"({self.pred(e.left)} OR {self.pred(e.right)})"
        if isinstance(e, E.Not):
            return f"NOT ({self.pred(e.child)})"
        if isinstance(e, E.IsNull):
            return f"{self.value(e.child)} IS {'NOT ' if e.negated else ''}NULL"
        if isinstance(e, E.In):
            if not e.values:
                return "1 = 0"
            return f"{self.value(e.child)} IN ({', '.join(self.value(v) for v in e.values)})"
        if isinstance(e, E.Like):
            return f"{self.value(e.child)} LIKE {self.value(e.pattern)}"
        if isinstance(e, E.Literal) and e.value is None:
            return "1 = 0"
        # A boolean value (BIT column, CASE, literal) used as a condition.
        return f"{self.value(e)} = 1"

    def cast(self, e: E.Cast) -> str:
        src, _ = infer(e.child)
        to = e.to
        if isinstance(src, NullType):
            return f"CAST(NULL AS {sql_type(to)})"
        if src == to:
            return self.value(e.child)
        if isinstance(src, StringType) and to.is_numeric:
            # §7.7: validity check; LOG(0) on a row value raises <ILLEGAL VALUE> only when reached.
            s = lambda: self.value(e.child)  # noqa: E731 - each use renders its own parameters
            check = f"ISNUMERIC(TRIM({s()})) = 1"
            if to.is_integral:
                check += f" AND TRIM({s()}) %PATTERN '{INTEGER_PATTERN}'"
            return (f"CASE WHEN {s()} IS NULL THEN NULL WHEN {check} "
                    f"THEN CAST(TRIM({s()}) AS {sql_type(to)}) "
                    f"ELSE LOG(LENGTH({s()}) - LENGTH({s()})) END")
        if src.is_numeric and isinstance(to, BooleanType):
            v1, v2 = self.value(e.child), self.value(e.child)
            return f"CASE WHEN {v1} IS NULL THEN NULL WHEN {v2} = 0 THEN 0 ELSE 1 END"
        return f"CAST({self.value(e.child)} AS {sql_type(to)})"
