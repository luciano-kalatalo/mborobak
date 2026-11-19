"""Plan → IRIS SQL (compiler spec §7 required translations, §9 SQL generation).

The generator walks the plan bottom-up and builds ``Block``s (one SELECT
each). A plan node either merges into the current block or wraps it as a
derived table (§9.1–9.2). Expressions are kept as trees until the very end;
``Renderer`` turns them into text and collects parameters in text order.
"""

from __future__ import annotations

import decimal
import hashlib
import math
import re
from dataclasses import dataclass, field

from . import expressions as E
from . import plan as P
from . import scalar
from .analyzer import infer
from .errors import AnalysisError, PlanTooLargeError, UnsupportedOperationError
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
class FromItem:
    source: object  # TableRef or Block
    alias: str


@dataclass
class JoinSource:
    first: FromItem
    joins: list[tuple[str, FromItem, E.Expr | None]] = field(default_factory=list)


@dataclass
class UnionSource:
    arms: list["Block"]


@dataclass
class Block:
    source: object  # TableRef, Block, JoinSource or UnionSource
    alias: str | None = None
    out_names: dict[int, str] = field(default_factory=dict)  # select aliases when used as a subquery
    casts: dict[int, DataType] = field(default_factory=dict)  # union arms: widen to the union's type
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
        self.ctes: list[tuple[str, tuple]] = []  # (WITH text, its parameters) from sql() sources

    # ---------------------------------------------------------------- D10 collation
    def needs_exact(self, expr: E.Expr) -> bool:
        if self.options.string_semantics != "pyspark":
            return False
        if isinstance(expr, (E.Literal, E.Exact, E.TextOrder)):
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

    def order_text(self, expr: E.Expr) -> E.Expr:
        """A string in an ordering context (<, >, ORDER BY, MIN, MAX): Spark's code-point order (§7.1).
        Applies to literals and EXACT columns too: their IRIS order puts numeric-looking strings first."""
        if self.options.string_semantics != "pyspark":
            return expr
        dtype, _ = infer(expr)
        if not isinstance(dtype, StringType):
            return self.exact(expr)
        if isinstance(expr, E.Exact):
            expr = expr.child
        note = "string order: %EXACT(' ' || x), so numeric-looking strings sort as text"
        if note not in self.translations:
            self.translations.append(note)
        return E.TextOrder(expr)

    def _note_exact(self, expr: E.Expr) -> None:
        if isinstance(expr, E.AttrRef):
            note = f"collation: %EXACT on {expr.attr.name} (string, {expr.attr.collation or 'not EXACT'})"
        else:
            note = "collation: %EXACT on a string expression"
        if note not in self.translations:
            self.translations.append(note)

    def translate(self, expr: E.Expr) -> E.Expr:
        """Insert ``Exact`` where collation changes the result (§7.1)."""

        def rule(e: E.Expr) -> E.Expr:  # children are already translated (bottom-up)
            if isinstance(e, E.Comparison):
                if e.op in ("<", "<=", ">", ">="):
                    return E.Comparison(e.op, self.order_text(e.left), self.order_text(e.right))
                return E.Comparison(e.op, self.exact(e.left), self.exact(e.right))
            if isinstance(e, E.In):
                return E.In(self.exact(e.child), e.values)
            if isinstance(e, E.Like):
                return E.Like(self.exact(e.child), e.pattern)
            if isinstance(e, E.AggregateCall) and not isinstance(e.child, E.Star) \
                    and (e.function in ("min", "max") or e.distinct):
                child = self.order_text(e.child) if e.function in ("min", "max") else self.exact(e.child)
                return E.AggregateCall(e.function, child, e.distinct)
            if isinstance(e, E.Func) and scalar.spec(e.name).exact_args:
                return E.Func(e.name, tuple(self.exact(a) for a in e.args))  # NULLIF, GREATEST compare
            return e

        return E.transform(expr, rule)

    # ---------------------------------------------------------------- substitution
    @staticmethod
    def substitute(expr: E.Expr, env: dict[int, E.Expr]) -> E.Expr:
        """Plan-level column references → this block's SQL expressions."""

        def rule(e: E.Expr) -> E.Expr:
            if isinstance(e, E.AttrRef):
                return env[e.attr.id]
            if isinstance(e, E.Alias):
                return e.child
            if isinstance(e, E.Exact) and isinstance(e.child, E.Exact):
                return e.child
            return e

        return E.transform(expr, rule)

    def sql(self, expr: E.Expr, block: Block) -> E.Expr:
        """Plan-level expression → SQL-level expression of ``block``."""
        return self.substitute(self.translate(expr), block.env)

    # ---------------------------------------------------------------- guarded casts
    @staticmethod
    def _is_guarded_cast(e: E.Expr) -> bool:
        """A cast rendered with its operand repeated (§7.7), over an operand worth computing once."""
        if not isinstance(e, E.Cast) or isinstance(e.child, (E.AttrRef, E.Literal, E.SqlCol)):
            return False
        if E.contains_aggregate(e.child):
            return False  # the operand must stay after the GROUP BY
        src, _ = infer(e.child)
        to = e.to
        return ((isinstance(src, StringType) and to.is_numeric)
                or (isinstance(src, (DoubleType, DecimalType)) and to.is_integral)
                or (isinstance(src, BooleanType) and isinstance(to, StringType)))

    def hoist(self, b: Block, exprs: list[E.Expr]) -> tuple[Block, list[E.Expr]]:
        """Compute the operands of guarded casts once, as hidden columns of a subquery.

        A guarded cast repeats its operand up to 13 times; nested casts would multiply that. Each
        complex operand becomes a hidden column of ``b``, ``b`` becomes a subquery, and the cast
        refers to the column instead. Operands are hoisted recursively, innermost first.
        """
        pre: list[tuple[E.Attribute, E.Expr]] = []

        def replace(e: E.Expr):
            if self._is_guarded_cast(e):
                dtype, nullable = infer(e.child)
                attr = E.Attribute(E.new_id(), f"mborobak_h{E.new_id()}", dtype, nullable)
                pre.append((attr, e.child))
                return E.Cast(E.AttrRef(attr), e.to)
            return None

        rewritten = [E.transform_down(x, replace) for x in exprs]
        if not pre:
            return b, exprs
        b, operands = self.hoist(b, [child for _, child in pre])
        if b.distinct:
            b = self.wrap(b, keep_order=True)  # hidden columns would change which rows are distinct
        visible = list(b.select)
        for (attr, _), operand in zip(pre, operands):
            b.env[attr.id] = self.sql(operand, b)
        b.select = visible + [attr for attr, _ in pre]
        outer = self.wrap(b, keep_order=True)
        hidden = {attr.id for attr, _ in pre}
        outer.select = [a for a in outer.select if a.id not in hidden]
        return outer, rewritten

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

    @staticmethod
    def unique_names(attrs: list[E.Attribute]) -> dict[int, str]:
        """Subquery column names; duplicates (after a join) get a numeric suffix."""
        seen: set[str] = set()
        out: dict[int, str] = {}
        for a in attrs:
            if a.id in out:
                continue
            base = sql_alias(a.name)
            name, n = base, 1
            while name.lower() in seen:
                n += 1
                name = f"{base}_{n}"
            seen.add(name.lower())
            out[a.id] = name
        return out

    def wrap(self, inner: Block, keep_order: bool) -> Block:
        visible = list(inner.select)
        if inner.order and keep_order and not inner.distinct:
            # Sort keys a select dropped travel up as hidden columns, so the order can be re-applied
            # outside (§7.2). Not under DISTINCT: an extra column would change which rows are distinct.
            exposed = {a.id for a in inner.select}
            hidden = {r.attr.id: r.attr for o in inner.order for r in E.walk(o.child)
                      if isinstance(r, E.AttrRef) and r.attr.id not in exposed}
            inner.select = inner.select + list(hidden.values())
        alias = self._new_alias()
        inner.out_names = self.unique_names(inner.select)
        outer = Block(source=inner, alias=alias)
        for a in inner.select:
            outer.env[a.id] = E.SqlCol(inner.out_names[a.id], alias, a.dtype, a.collation)
        outer.select = visible
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
            b, (condition,) = self.hoist(b, [node.condition])
            cond = self.sql(condition, b)
            (b.having if b.grouped else b.where).append(cond)
            return b

        if isinstance(node, P.Project):
            b = self.build(node.child)
            if b.distinct:
                b = self.wrap(b, keep_order=True)
            b, exprs = self.hoist(b, [expr for _, expr in node.items])
            for (attr, _), expr in zip(node.items, exprs):
                b.env[attr.id] = self.sql(expr, b)
            b.select = [a for a, _ in node.items]
            return b

        if isinstance(node, P.Aggregate):
            b = self.build(node.child)
            if b.grouped or b.top is not None or b.distinct:
                b = self.wrap(b, keep_order=False)
            b.order = []  # aggregation destroys order (§7.2)
            b, exprs = self.hoist(b, [e for _, e in node.keys] + [e for _, e in node.aggs])
            b.order = []
            key_exprs, agg_exprs = exprs[:len(node.keys)], exprs[len(node.keys):]
            keys = [self.substitute(self.exact(self.translate(expr)), b.env) for expr in key_exprs]
            new_env = dict(b.env)
            for (attr, _), k in zip(node.keys, keys):
                new_env[attr.id] = k
            for (attr, _), expr in zip(node.aggs, agg_exprs):
                new_env[attr.id] = self.sql(expr, b)
            b.env = new_env
            b.group_by = keys
            b.select = [a for a, _ in node.keys] + [a for a, _ in node.aggs]
            return b

        if isinstance(node, P.Sort):
            b = self.build(node.child)
            if b.top is not None:
                b = self.wrap(b, keep_order=False)
            b, orders = self.hoist(b, list(node.orders))
            b.order = list(orders)
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

        if isinstance(node, P.SubqueryAlias):
            return self.build(node.child)

        if isinstance(node, P.SqlSource):
            names = [a.name.lower() for a in node.attrs]
            if len(set(names)) != len(names):
                raise AnalysisError(
                    "the sql() query has duplicate column names; give them distinct aliases in the SQL "
                    "to transform its result")
            if node.query.cte:
                entry = (node.query.cte, node.args[:node.query.cte_params])
                if entry not in self.ctes:
                    self.ctes.append(entry)
            alias = self._new_alias()
            b = Block(source=node, alias=alias)
            for a in node.attrs:
                b.env[a.id] = E.SqlCol(a.name, alias, a.dtype, a.collation)
            b.select = list(node.attrs)
            return b

        if isinstance(node, P.Join):
            return self.join(node)

        if isinstance(node, P.Union):
            return self.union(node)

        if isinstance(node, P.CountRows):
            inner = self.build(node.child)
            outer = self.wrap(inner, keep_order=False)
            outer.order = []
            outer.env[node.attr.id] = E.AggregateCall("count", E.Star())
            outer.group_by = []
            outer.select = [node.attr]
            return outer

        raise UnsupportedOperationError(f"cannot compile {type(node).__name__}")

    # ---------------------------------------------------------------- joins and unions
    @staticmethod
    def _is_plain(b: Block) -> bool:
        return not (b.where or b.grouped or b.having or b.top is not None or b.distinct or b.order)

    def from_item(self, b: Block) -> tuple[FromItem, dict[int, E.Expr]]:
        """One side of a join: the table itself when nothing else is applied, else a subquery."""
        alias = self._new_alias()
        if isinstance(b.source, P.TableRef) and self._is_plain(b):
            def requalify(e: E.Expr) -> E.Expr:
                if isinstance(e, E.SqlCol) and e.qualifier is None:
                    return E.SqlCol(e.name, alias, e.dtype, e.collation)
                return e

            # Export only this side's output columns: in a self-join both sides share the scan's ids.
            env = {a.id: E.transform(b.env[a.id], requalify) for a in b.select}
            return FromItem(b.source, alias), env
        b.order = b.order if b.top is not None else []
        b.out_names = self.unique_names(b.select)
        env = {a.id: E.SqlCol(b.out_names[a.id], alias, a.dtype, a.collation) for a in b.select}
        return FromItem(b, alias), env

    def join(self, node: P.Join) -> Block:
        left = self.build(node.left)
        right = self.build(node.right)
        if isinstance(left.source, JoinSource) and self._is_plain(left):
            source = JoinSource(left.source.first, list(left.source.joins))  # extend the join tree
            env = dict(left.env)
        else:
            item, env = self.from_item(left)
            source = JoinSource(item)
        right_item, right_env = self.from_item(right)
        env.update(right_env)
        cond = None
        if node.condition is not None:
            cond = self.substitute(self.translate(node.condition), env)
        source.joins.append((node.join_type, right_item, cond))
        return Block(source=source, env=env, select=list(node.output))

    def union(self, node: P.Union) -> Block:
        arms = []
        for child in node.inputs:
            b = self.build(child)
            if b.top is not None and b.order:
                b = self.wrap(b, keep_order=False)  # ORDER BY is rejected inside a UNION arm
            b.order = []
            arms.append(b)
        names = self.unique_names(list(node.attrs))
        for arm, child in zip(arms, node.inputs):
            arm.out_names = {}
            for target, a in zip(node.attrs, child.output):
                arm.out_names[a.id] = names[target.id]
                if a.dtype != target.dtype:
                    arm.casts[a.id] = target.dtype
            arm.select = list(child.output)
        alias = self._new_alias()
        outer = Block(source=UnionSource(arms), alias=alias)
        for a in node.attrs:
            outer.env[a.id] = E.SqlCol(names[a.id], alias, a.dtype, a.collation)
        outer.select = list(node.attrs)
        return outer

    # ---------------------------------------------------------------- entry point
    def compile(self, plan: P.Plan) -> SqlStatement:
        schema = StructType([StructField(a.name, a.dtype, a.nullable) for a in plan.output])
        if isinstance(plan, P.SqlSource):
            # The user's query on its own runs as written, so its ORDER BY keeps its meaning.
            text = plan.query.original
            fingerprint = hashlib.sha256(text.encode()).hexdigest()[:16]
            return SqlStatement(text, list(plan.args), schema, [], fingerprint)
        block = self.build(plan)
        renderer = Renderer(self)
        text = renderer.block(block, top_level=True)
        params = renderer.params
        if self.ctes:
            if len(self.ctes) > 1:  # entries are (text, args): the same text with other args differs too
                raise UnsupportedOperationError(
                    "combining sql() queries that use WITH is only supported when they are the same query "
                    "with the same args: the WITH clause is moved to the front of the statement, where two "
                    "of them would clash")
            cte, cte_args = self.ctes[0]
            text = f"{cte}\n{text}"
            params = list(cte_args) + params
            renderer.params = params
        if len(renderer.params) > self.options.max_parameters:
            raise PlanTooLargeError(
                f"the query needs {len(renderer.params)} parameters; the limit is "
                f"{self.options.max_parameters} (IRIS fails near 999). Shorten long isin() lists.")
        fingerprint = hashlib.sha256(text.encode()).hexdigest()[:16]
        return SqlStatement(text, renderer.params, schema, list(self.translations), fingerprint)


# --------------------------------------------------------------------------- constant casts

_INT_TEXT = re.compile(r"^[+-]?[0-9]+$")
_NUM_TEXT = re.compile(r"^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$")
_SPECIAL = {"nan": math.nan, "inf": math.inf, "+inf": math.inf, "infinity": math.inf, "+infinity": math.inf,
            "-inf": -math.inf, "-infinity": -math.inf}
_RANGES = {IntegerType: (-(2**31), 2**31 - 1), LongType: (-(2**63), 2**63 - 1)}


def fold_cast(value, to: DataType):
    """Spark's (ANSI) cast of one constant; raises MborobakCastError/MborobakArithmeticError."""
    from .errors import MborobakArithmeticError, MborobakCastError

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if isinstance(to, DoubleType):
            if text.lower() in _SPECIAL:
                return _SPECIAL[text.lower()]
            if _NUM_TEXT.match(text):
                return float(text)
        elif to.is_integral and _INT_TEXT.match(text):
            number = int(text)
            low, high = _RANGES[type(to)]
            if not low <= number <= high:
                raise MborobakCastError(f"invalid input for cast: {value!r} is out of range for "
                                        f"{to.simpleString()}")
            return number
        elif isinstance(to, DecimalType) and _NUM_TEXT.match(text):
            value = decimal.Decimal(text)
        else:
            raise MborobakCastError(f"invalid input for cast: {value!r} is not a valid {to.simpleString()}")
        if not isinstance(to, DecimalType):
            raise MborobakCastError(f"invalid input for cast: {value!r} is not a valid {to.simpleString()}")
    if isinstance(to, DecimalType):
        quantized = decimal.Decimal(value).quantize(decimal.Decimal(1).scaleb(-to.scale),
                                                    rounding=decimal.ROUND_HALF_UP)
        if len(quantized.as_tuple().digits) - to.scale > to.precision - to.scale and quantized != 0:
            raise MborobakArithmeticError(f"numeric overflow: {value!r} doesn't fit {to.simpleString()}")
        return quantized
    # double or decimal → integral: truncate toward zero, fail on NaN, infinity and overflow
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        raise MborobakArithmeticError(f"numeric overflow: {value!r} can't be cast to {to.simpleString()}")
    number = int(value)
    low, high = _RANGES[type(to)]
    if not low <= number <= high:
        raise MborobakArithmeticError(f"numeric overflow: {value!r} doesn't fit {to.simpleString()}")
    return number


# --------------------------------------------------------------------------- rendering


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_alias(name: str) -> str:
    """IRIS rejects '.', ',' and '^' in a delimited identifier (probed on 2026.2), which unaliased names
    such as "CAST(1.5 AS DECIMAL(10,2))" contain. Result columns are named from the DataFrame schema,
    not from the cursor, so the SQL alias may differ."""
    return name.translate(_ALIAS_FIX)


_ALIAS_FIX = str.maketrans({".": "_", ",": ";", "^": "~"})


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


_PREDICATES = (E.Comparison, E.And, E.Or, E.Not, E.IsNull, E.In, E.Like)


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
            if a.id in b.casts:
                expr = E.Cast(expr, b.casts[a.id])
            name = sql_alias(a.name) if top_level else b.out_names.get(a.id, sql_alias(a.name))
            text = self.value(expr)
            plain = isinstance(expr, E.SqlCol) and expr.name == name
            items.append(text if plain else f"{text} AS {quote(name)}")
        lines = [head + ", ".join(items)]
        lines.append("FROM " + self.source(b.source, b.alias, indent))
        if b.where:
            lines.append("WHERE " + " AND ".join(self.pred(w) for w in b.where))
        if b.group_by:
            lines.append("GROUP BY " + ", ".join(self.value(k) for k in b.group_by))
        if b.having:
            lines.append("HAVING " + " AND ".join(self.pred(h) for h in b.having))
        if b.order and (top_level or b.top is not None):
            lines.append("ORDER BY " + ", ".join(self.order_key(o, b) for o in b.order))
        return f"\n{indent}".join(lines)

    def subquery(self, inner: Block, alias: str, indent: str) -> str:
        text = self.block(inner, top_level=False, indent=indent + "  ")
        return f"(\n{indent}  {text}\n{indent}) AS {alias}"

    def from_item(self, item: FromItem, indent: str) -> str:
        if isinstance(item.source, Block):
            return self.subquery(item.source, item.alias, indent)
        return f"{item.source.quoted()} AS {item.alias}"

    def source(self, src: object, alias: str | None, indent: str) -> str:
        if isinstance(src, P.TableRef):
            return src.quoted()
        if isinstance(src, Block):
            return self.subquery(src, alias, indent)
        if isinstance(src, JoinSource):
            parts = [self.from_item(src.first, indent)]
            for join_type, item, cond in src.joins:
                keyword = {"inner": "JOIN", "left": "LEFT OUTER JOIN", "right": "RIGHT OUTER JOIN",
                           "full": "FULL OUTER JOIN", "cross": "CROSS JOIN"}[join_type]
                text = f"{keyword} {self.from_item(item, indent)}"
                if join_type != "cross":
                    text += f" ON {self.pred(cond) if cond is not None else '1 = 1'}"
                parts.append(text)
            return f"\n{indent}".join(parts)
        if isinstance(src, P.SqlSource):
            q = src.query
            self.params.extend(src.args[q.cte_params:])
            # The user's text is kept byte for byte (no re-indenting: it may contain multi-line strings).
            return f"(\n{q.body}\n{indent}) AS {alias}"
        if isinstance(src, UnionSource):
            arms = [self.block(arm, top_level=False, indent=indent + "  ") for arm in src.arms]
            body = f"\n{indent}  UNION ALL\n{indent}  ".join(arms)
            return f"(\n{indent}  {body}\n{indent}) AS {alias}"
        raise UnsupportedOperationError(f"cannot render source {type(src).__name__}")

    def order_key(self, order: E.SortOrder, b: Block) -> str:
        key_plan = order.child
        dtype, _ = infer(key_plan)
        key = self.c.substitute(self.c.order_text(self.c.translate(key_plan)), b.env)
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
        if isinstance(e, E.TextOrder):
            return f"%EXACT(' ' || {self.value(e.child)})"
        if isinstance(e, E.BinaryArithmetic):
            if e.op == "%":
                return f"MOD({self.value(e.left)}, {self.value(e.right)})"
            if e.op == "/" and isinstance(infer(e)[0], DoubleType):
                # Integer division must not truncate: Spark's result is a double (decimal stays decimal).
                return f"(CAST({self.value(e.left)} AS DOUBLE) / {self.value(e.right)})"
            return f"({self.value(e.left)} {e.op} {self.value(e.right)})"
        if isinstance(e, E.Negate):
            return f"(-{self.value(e.child)})"
        if isinstance(e, E.Cast):
            return self.cast(e)
        if isinstance(e, E.Coalesce):
            return f"COALESCE({', '.join(self.value(v) for v in e.values)})"
        if isinstance(e, E.CaseWhen):
            parts = " ".join(f"WHEN {self.pred(c)} THEN {self.value(v)}" for c, v in e.branches)
            tail = f" ELSE {self.value(e.otherwise)}" if e.otherwise is not None else ""
            return f"CASE {parts}{tail} END"
        if isinstance(e, E.Func):
            s = scalar.spec(e.name)
            if s.predicate:
                p = s.render(self, e)
                n = s.render(self, e)
                return f"CASE WHEN {p} THEN 1 WHEN NOT ({n}) THEN 0 END"
            return s.render(self, e)
        if isinstance(e, E.AggregateCall):
            fn = e.function.upper()
            if isinstance(e.child, E.Star):
                return f"{fn}(*)"
            if isinstance(e.child, E.TextOrder):  # MIN/MAX by code point, then drop the leading space
                return f"SUBSTRING({fn}({self.value(e.child)}), 2)"
            return f"{fn}({'DISTINCT ' if e.distinct else ''}{self.value(e.child)})"
        if isinstance(e, _PREDICATES):
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
        if isinstance(e, E.Func) and scalar.spec(e.name).predicate:
            return scalar.spec(e.name).render(self, e)
        # A boolean value (BIT column, CASE, literal) used as a condition.
        return f"{self.value(e)} = 1"

    def cast(self, e: E.Cast) -> str:
        src, _ = infer(e.child)
        to = e.to
        if isinstance(src, NullType):
            return f"CAST(NULL AS {sql_type(to)})"
        if src == to:
            return self.value(e.child)
        if isinstance(e.child, E.Literal) and (isinstance(src, StringType) and to.is_numeric or
                                               isinstance(src, (DoubleType, DecimalType)) and to.is_integral):
            # A constant has no row to make the error trigger depend on, so the guarded forms below don't
            # apply (§7.5). Cast it here with Spark's rules; invalid input fails when the action runs.
            return self.lit(E.Literal(fold_cast(e.child.value, to), to))
        if isinstance(src, StringType) and to.is_numeric:
            # §7.7: validity check. $LISTGET at position -5 raises <RANGE>, only when reached (it
            # depends on the row). <ILLEGAL VALUE> is not used: IRIS raises it for DOUBLE / 0 too.
            # Each s() renders the operand again, so build the text strictly left to right: its
            # parameters must be appended in the order they appear.
            s = lambda: self.value(e.child)  # noqa: E731
            text = f"CASE WHEN {s()} IS NULL THEN NULL"
            if isinstance(to, DoubleType):
                # CAST-07: Spark reads NaN and infinity text; ISNUMERIC rejects it. The values are sent as
                # parameters, since IRIS has no NaN or infinity literals (§9.5).
                for words, value in (("'NAN'", math.nan), ("'INF', '+INF', 'INFINITY', '+INFINITY'", math.inf),
                                     ("'-INF', '-INFINITY'", -math.inf)):
                    text += f" WHEN UPPER(TRIM({s()})) IN ({words}) THEN "
                    text += self.lit(E.Literal(value, DoubleType()))
            text += f" WHEN ISNUMERIC(TRIM({s()})) = 1"
            if to.is_integral:
                text += f" AND TRIM({s()}) %PATTERN '{INTEGER_PATTERN}'"
            text += f" THEN CAST(TRIM({s()}) AS {sql_type(to)})"
            text += f" ELSE $LISTGET({s()}, LENGTH({s()}) - LENGTH({s()}) - 5) END"
            return text
        if isinstance(src, (DoubleType, DecimalType)) and to.is_integral:
            # §7.7: IRIS turns NaN into 0, returns ±Infinity as text and ignores range overflow; Spark
            # (ANSI) fails. Out-of-range values (NaN included, via x <> x) reach EXP(100000), which
            # raises <MAXNUMBER> ("numeric overflow"), only when reached.
            low, high = ("-2147483649", "2147483648") if isinstance(to, IntegerType) else \
                ("-9223372036854775809", "9223372036854775808")
            x = lambda: self.value(e.child)  # noqa: E731
            return (f"CASE WHEN {x()} IS NULL THEN NULL "
                    f"WHEN {x()} <> {x()} OR {x()} >= {high} OR {x()} <= {low} "
                    f"THEN EXP(100000 + LENGTH(CAST({x()} AS VARCHAR(60))) - LENGTH(CAST({x()} AS VARCHAR(60)))) "
                    f"ELSE CAST({x()} AS {sql_type(to)}) END")
        if isinstance(src, BooleanType) and isinstance(to, StringType):
            # Spark writes 'true'/'false'; a plain CAST of IRIS's BIT (or of a 1/0 CASE) gives '1'/'0'.
            if isinstance(e.child, _PREDICATES):
                p, n = self.pred(e.child), self.pred(E.Not(e.child))
                return f"CASE WHEN {p} THEN 'true' WHEN {n} THEN 'false' END"
            v1, v2 = self.value(e.child), self.value(e.child)
            return f"CASE WHEN {v1} = 1 THEN 'true' WHEN {v2} = 0 THEN 'false' END"
        if src.is_numeric and isinstance(to, BooleanType):
            v1, v2 = self.value(e.child), self.value(e.child)
            return f"CASE WHEN {v1} IS NULL THEN NULL WHEN {v2} = 0 THEN 0 ELSE 1 END"
        return f"CAST({self.value(e.child)} AS {sql_type(to)})"
