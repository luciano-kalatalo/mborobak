"""Spark SQL statements in ``session.sql()`` (compiler spec §7.17, API contract §3.5).

With ``mborobak.sql.dialect = "spark"``, a ``SELECT`` statement is parsed here and built from DataFrame
operations, so it gets Spark's names, types and semantics (string collation, NaN, casts …) exactly as the
DataFrame API does, and compiles to one IRIS statement like any other DataFrame. Expressions use the
parser of ``F.expr`` (``sqlexpr.py``); this module adds the statement around them:

    [WITH name AS (query), …] select [UNION [ALL | DISTINCT] select …] [ORDER BY …] [LIMIT n]
    select := SELECT [DISTINCT | ALL] items [FROM relations] [WHERE …] [GROUP BY … | GROUP BY ALL] [HAVING …]

``GROUP BY`` takes expressions, select aliases and ordinals, as Spark does; ``ORDER BY`` also sees the
columns of ``FROM`` that the select drops. ``?`` and ``:name`` markers take the values of ``args``.
"""

from __future__ import annotations

import string
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from . import expressions as E
from .analyzer import find_attr, qualifiers
from .column import Column, literal
from .errors import AnalysisError, ColumnNotFoundError, UnsupportedOperationError
from .naming import expr_name
from .sql.types import IntegerType
from .sqlexpr import Parser, _RESERVED

if TYPE_CHECKING:
    from .dataframe import DataFrame
    from .session import MborobakSession

# Words that end an expression or a name in a statement; a table alias can't be one of them.
_CLAUSE = {"SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "UNION", "EXCEPT",
           "INTERSECT", "MINUS", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "SEMI", "ANTI",
           "ON", "USING", "NATURAL", "WITH", "WINDOW", "QUALIFY", "LATERAL", "AS", "BY", "ALL", "DISTINCT",
           "CLUSTER", "DISTRIBUTE", "SORT", "TABLESAMPLE", "PIVOT", "UNPIVOT"}
_HIDDEN = "__mborobak_"


@dataclass(frozen=True)
class QualifiedStar(E.Expr):
    """``t.*`` in a select list."""

    qualifier: str


@dataclass
class _Select:
    distinct: bool
    items: list[E.Expr]
    df: "DataFrame | None"
    where: E.Expr | None = None
    group: list[E.Expr] | str | None = None  # "ALL" for GROUP BY ALL
    having: E.Expr | None = None


@dataclass
class _Scope:
    ctes: dict[str, "DataFrame"] = field(default_factory=dict)
    parent: "_Scope | None" = None

    def lookup(self, name: str) -> "DataFrame | None":
        scope: _Scope | None = self
        while scope is not None:
            if name.lower() in scope.ctes:
                return scope.ctes[name.lower()]
            scope = scope.parent
        return None


# --------------------------------------------------------------------------- entry point
def run(session: "MborobakSession", text: str, args: Any, kwargs: dict[str, Any]) -> "DataFrame":
    scope = _Scope()
    if kwargs:
        text = _format(text, kwargs, scope)
    parser = StatementParser(text, args, session)
    df = parser.statement(scope)
    return df


def _format(text: str, kwargs: dict[str, Any], scope: _Scope) -> str:
    """PySpark's ``spark.sql("… {df} …", df=df)``: a DataFrame becomes a relation, other values text."""
    from .dataframe import DataFrame

    out = []
    for literal_text, name, spec, conversion in string.Formatter().parse(text):
        out.append(literal_text)
        if name is None:
            continue
        if name not in kwargs:
            raise KeyError(f"sql(): no value for {{{name}}}")
        value = kwargs[name]
        if isinstance(value, DataFrame):
            alias = f"{_HIDDEN}{name}"
            scope.ctes[alias.lower()] = value
            out.append(alias)
        else:
            out.append(format(value, spec or ""))
    return "".join(out)


# --------------------------------------------------------------------------- parser
class StatementParser(Parser):
    def __init__(self, text: str, args: Any, session: "MborobakSession"):
        super().__init__(text)
        self.session = session
        self.args = args
        self.position = 0
        if args is not None and not isinstance(args, (list, tuple, dict)):
            raise TypeError("sql(args=...) takes a list for ? markers or a dict for :name markers")

    # ---------------------------------------------------------------- parameters
    def primary(self) -> E.Expr:
        if self.op("?"):
            self.i += 1
            if not isinstance(self.args, (list, tuple)):
                self.fail("a ? marker needs args as a list")
            if self.position >= len(self.args):
                raise ValueError(f"sql(): the query has more ? markers than the {len(self.args)} args")
            self.position += 1
            return literal(self.args[self.position - 1])
        if self.op(":") and self.toks[self.i + 1].kind == "ident":
            name = self.toks[self.i + 1].text
            self.i += 2
            if not isinstance(self.args, dict) or name not in self.args:
                raise ValueError(f"sql(): no value for parameter :{name}")
            return literal(self.args[name])
        return super().primary()

    def kw_any(self) -> bool:
        return self.tok.kind == "ident" and self.tok.text.upper() in _CLAUSE

    # ---------------------------------------------------------------- statement
    def statement(self, scope: _Scope) -> "DataFrame":
        if not self.kw("SELECT", "WITH") and not self.op("("):
            word = self.tok.text.upper() or "an empty statement"
            raise UnsupportedOperationError(
                f"sql() with the Spark dialect runs SELECT statements (and WITH … SELECT); {word} is not "
                "supported. Write tables with df.write.saveAsTable()")
        df = self.query(scope)
        self.take_op(";")
        self.done()
        if isinstance(self.args, (list, tuple)) and self.position != len(self.args):
            raise ValueError(f"sql(): the query has {self.position} ? markers but {len(self.args)} args were given")
        return df

    def query(self, outer: _Scope) -> "DataFrame":
        scope = _Scope(parent=outer)
        if self.take_kw("WITH"):
            if self.take_kw("RECURSIVE"):
                raise UnsupportedOperationError("WITH RECURSIVE is not supported")
            while True:
                name = self.identifier()
                if self.op("("):
                    raise UnsupportedOperationError("column lists after a WITH name are not supported")
                self.expect_kw("AS")
                self.expect_op("(")
                scope.ctes[name.lower()] = self.query(scope)
                self.expect_op(")")
                if not self.take_op(","):
                    break
        first, select = self.query_term(scope)
        df = first
        while self.kw("UNION", "EXCEPT", "INTERSECT", "MINUS"):
            if not self.kw("UNION"):
                raise UnsupportedOperationError(f"{self.tok.text.upper()} is not supported; UNION is")
            self.i += 1
            distinct = not self.take_kw("ALL")
            self.take_kw("DISTINCT")
            right, right_select = self.query_term(scope)
            if select is not None:
                df, select = self.finish(select, [], None), None
            if right_select is not None:
                right = self.finish(right_select, [], None)
            df = df.union(right)
            if distinct:
                df = df.distinct()
        order = self.order_by()
        limit = self.limit()
        if select is not None:  # one SELECT: ORDER BY may use its FROM columns and aggregates
            return self.finish(select, order, limit)
        if order:
            df = df.orderBy(*[Column(o) for o in self.output_orders(df, order, None)])
        if limit is not None:
            df = df.limit(limit)
        return df

    def query_term(self, scope: _Scope) -> tuple["DataFrame | None", _Select | None]:
        if self.take_op("("):
            df = self.query(scope)
            self.expect_op(")")
            return df, None
        return None, self.select(scope)

    def order_by(self) -> list[E.SortOrder]:
        if not self.take_kw("ORDER"):
            return []
        self.expect_kw("BY")
        keys = [self.sort_key()]
        while self.take_op(","):
            keys.append(self.sort_key())
        return keys

    def limit(self) -> int | None:
        if not self.take_kw("LIMIT"):
            if self.kw("OFFSET"):
                raise UnsupportedOperationError("OFFSET is not supported")
            return None
        if self.take_kw("ALL"):
            return None
        value = self.value()
        if not isinstance(value, E.Literal) or not isinstance(value.value, int) or value.value < 0:
            self.fail("LIMIT takes a non-negative integer")
        if self.kw("OFFSET"):
            raise UnsupportedOperationError("OFFSET is not supported")
        return value.value

    # ---------------------------------------------------------------- SELECT
    def select(self, scope: _Scope) -> _Select:
        self.expect_kw("SELECT")
        distinct = self.take_kw("DISTINCT")
        if not distinct:
            self.take_kw("ALL")
        items = [self.item()]
        while self.take_op(","):
            items.append(self.item())
        if not self.take_kw("FROM"):
            raise UnsupportedOperationError("SELECT without FROM is not supported; select from a table, or use "
                                            "session.createDataFrame() for constants")
        sel = _Select(distinct, items, self.relations(scope))
        if self.take_kw("WHERE"):
            sel.where = self.expr()
        if self.take_kw("GROUP"):
            self.expect_kw("BY")
            if self.take_kw("ALL"):
                sel.group = "ALL"
            else:
                if self.kw("ROLLUP", "CUBE", "GROUPING"):
                    raise UnsupportedOperationError(f"GROUP BY {self.tok.text.upper()} is not supported")
                sel.group = [self.expr()]
                while self.take_op(","):
                    sel.group.append(self.expr())
        if self.take_kw("HAVING"):
            sel.having = self.expr()
        for word in ("WINDOW", "QUALIFY", "LATERAL", "CLUSTER", "DISTRIBUTE", "SORT"):
            if self.kw(word):
                raise UnsupportedOperationError(f"{word} is not supported")
        return sel

    def item(self) -> E.Expr:
        if self.op("*"):
            self.i += 1
            return E.Star()
        t, nxt = self.tok, self.toks[self.i + 1]
        if t.kind in ("ident", "qident") and nxt.kind == "op" and nxt.text == "." and \
                self.toks[self.i + 2].kind == "op" and self.toks[self.i + 2].text == "*":
            self.i += 3
            return QualifiedStar(t.value)
        expr = self.expr()
        if self.take_kw("AS"):
            return E.Alias(expr, self.identifier())
        if self.tok.kind == "qident" or (self.tok.kind == "ident" and not self.kw_any()
                                          and self.tok.text.upper() not in _RESERVED):
            return E.Alias(expr, self.identifier())
        return expr

    # ---------------------------------------------------------------- FROM
    def relations(self, scope: _Scope) -> "DataFrame":
        df = self.joined(scope)
        while self.take_op(","):
            df = df.crossJoin(self.joined(scope))
        return df

    def joined(self, scope: _Scope) -> "DataFrame":
        df = self.relation(scope)
        while True:
            how = self.join_type()
            if how is None:
                return df
            right = self.relation(scope)
            if how == "cross":
                df = df.crossJoin(right)
            elif self.take_kw("ON"):
                df = df.join(right, Column(self.expr()), how)
            elif self.take_kw("USING"):
                self.expect_op("(")
                names = [self.identifier()]
                while self.take_op(","):
                    names.append(self.identifier())
                self.expect_op(")")
                df = df.join(right, names, how)
            else:
                df = df.join(right, how=how)  # no condition: every pair (Spark 3+)

    def join_type(self) -> str | None:
        if self.kw("NATURAL"):
            raise UnsupportedOperationError("NATURAL JOIN is not supported; use USING (…)")
        start = self.i
        words = []
        while self.kw("INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "SEMI", "ANTI"):
            words.append(self.tok.text.upper())
            self.i += 1
        if not self.take_kw("JOIN"):
            self.i = start
            return None
        kind = " ".join(words)
        how = {"": "inner", "INNER": "inner", "CROSS": "cross", "LEFT": "left", "LEFT OUTER": "left",
               "RIGHT": "right", "RIGHT OUTER": "right", "FULL": "full", "FULL OUTER": "full",
               "LEFT SEMI": "left_semi", "SEMI": "left_semi", "LEFT ANTI": "left_anti", "ANTI": "left_anti"}
        if kind not in how:
            self.fail(f"unknown join type {kind} JOIN")
        return how[kind]

    def relation(self, scope: _Scope) -> "DataFrame":
        if self.take_op("("):
            df = self.query(scope)
            self.expect_op(")")
        else:
            parts = [self.identifier()]
            while self.op(".") and self.toks[self.i + 1].kind in ("ident", "qident"):
                self.i += 1
                parts.append(self.identifier())
            if len(parts) > 2:
                raise UnsupportedOperationError(
                    f"{'.'.join(parts)}: three-part names (catalog.schema.table) are not supported; IRIS tables "
                    "are schema.table")
            cte = scope.lookup(parts[0]) if len(parts) == 1 else None
            if cte is not None:
                df = cte.alias(parts[0])
            else:
                df = self.session.table(".".join(f"`{p}`" if "." in p else p for p in parts))
        if self.take_kw("AS") or self.tok.kind == "qident" or (self.tok.kind == "ident" and not self.kw_any()
                                                                and self.tok.text.upper() not in _RESERVED):
            df = df.alias(self.identifier())
        return df

    # ---------------------------------------------------------------- building
    def expand(self, df: "DataFrame", items: list[E.Expr]) -> list[E.Expr]:
        out: list[E.Expr] = []
        quals = qualifiers(df._plan)
        for item in items:
            if isinstance(item, E.Star):
                out.extend(E.AttrRef(a) for a in df._plan.output)
            elif isinstance(item, QualifiedStar):
                q = item.qualifier.lower()
                found = [E.AttrRef(a) for a in df._plan.output if q in quals.get(a.id, set())]
                if not found:
                    raise AnalysisError(f"{item.qualifier}.* matches no table or alias in FROM")
                out.extend(found)
            else:
                out.append(item)
        return out

    def finish(self, sel: _Select, order: list[E.SortOrder], limit: int | None) -> "DataFrame":
        df = sel.df
        if sel.where is not None:
            df = df.filter(Column(sel.where))
        items = self.expand(df, sel.items)
        aggregated = sel.group is not None or sel.having is not None or \
            any(E.contains_aggregate(e) for e in items) or any(E.contains_aggregate(o.child) for o in order)
        if aggregated:
            out = self.aggregate(df, sel, items, order)
        elif sel.distinct:
            out = df.select(*[Column(e) for e in items]).distinct()
            if order:
                out = out.orderBy(*[Column(o) for o in self.output_orders(out, order, items)])
        else:
            if order:
                df = df.orderBy(*[Column(o) for o in self.input_orders(df, order, items)])
            out = df.select(*[Column(e) for e in items])
        if limit is not None:
            out = out.limit(limit)
        return out

    @staticmethod
    def _ordinal(e: E.Expr, items: list[E.Expr], clause: str) -> E.Expr | None:
        if isinstance(e, E.Literal) and isinstance(e.value, int) and not isinstance(e.value, bool) \
                and isinstance(e.dtype, IntegerType):
            if not 1 <= e.value <= len(items):
                raise AnalysisError(f"{clause} position {e.value} is not in the select list (1 to {len(items)})")
            return items[e.value - 1]
        return None

    @staticmethod
    def _aliases(items: list[E.Expr]) -> dict[str, E.Expr]:
        return {i.name.lower(): i.child for i in items if isinstance(i, E.Alias)}

    @staticmethod
    def _is_input(df: "DataFrame", e: E.Expr) -> bool:
        if not isinstance(e, E.UnresolvedColumn):
            return False
        try:
            find_attr(e.name, df._plan.output, None, qualifiers(df._plan))
            return True
        except AnalysisError:
            return False

    def input_orders(self, df: "DataFrame", order: list[E.SortOrder], items: list[E.Expr]) -> list[E.SortOrder]:
        """ORDER BY of a plain select: ordinals and select aliases first (Spark), then FROM's columns."""
        aliases = self._aliases(items)
        out = []
        for o in order:
            key = self._ordinal(o.child, items, "ORDER BY")
            if key is None and isinstance(o.child, E.UnresolvedColumn) and "." not in o.child.name \
                    and o.child.name.lower() in aliases:
                key = aliases[o.child.name.lower()]
            key = key if key is not None else o.child
            out.append(E.SortOrder(_strip(key), o.ascending, o.nulls_first))
        return out

    def output_orders(self, df: "DataFrame", order: list[E.SortOrder], items: list[E.Expr] | None) -> list[E.SortOrder]:
        """ORDER BY over a result (after DISTINCT or UNION): ordinals and output names."""
        names = df.columns
        out = []
        for o in order:
            key = o.child
            if isinstance(key, E.Literal) and isinstance(key.value, int) and isinstance(key.dtype, IntegerType):
                if not 1 <= key.value <= len(names):
                    raise AnalysisError(f"ORDER BY position {key.value} is not in the select list")
                key = E.AttrRef(df._plan.output[key.value - 1])
            elif items is not None and not isinstance(key, E.UnresolvedColumn):
                match = [a for a, i in zip(df._plan.output, items) if _canon(_strip(i)) == _canon(key)]
                if match:
                    key = E.AttrRef(match[0])
            out.append(E.SortOrder(key, o.ascending, o.nulls_first))
        return out

    def aggregate(self, df: "DataFrame", sel: _Select, items: list[E.Expr],
                  order: list[E.SortOrder]) -> "DataFrame":
        """GROUP BY, HAVING and ORDER BY over aggregates: group by the keys and compute each aggregate once
        under a hidden name, then rewrite the select list, HAVING and ORDER BY over those names."""
        if any(E.contains_window(e) for e in items):
            raise UnsupportedOperationError("window functions in a query with GROUP BY or aggregates are not "
                                            "supported; group in a subquery first")
        aliases = self._aliases(items)
        if sel.group == "ALL":
            keys = [_strip(i) for i in items if not E.contains_aggregate(i)]
        else:
            keys = []
            for g in sel.group or []:
                key = self._ordinal(g, items, "GROUP BY")
                if key is None and isinstance(g, E.UnresolvedColumn) and not self._is_input(df, g) \
                        and g.name.lower() in aliases:
                    key = aliases[g.name.lower()]  # Spark's groupByAliases
                key = _strip(key if key is not None else g)
                if E.contains_aggregate(key):
                    raise AnalysisError(f"GROUP BY {expr_name(key)}: an aggregate can't be a grouping key")
                keys.append(key)
        key_map: dict[Any, str] = {}
        unique_keys = []
        for k in keys:
            if _canon(k) not in key_map:
                key_map[_canon(k)] = f"{_HIDDEN}g{len(unique_keys)}"
                unique_keys.append(k)

        orders = []
        for o in order:
            key = self._ordinal(o.child, items, "ORDER BY")
            if key is None and isinstance(o.child, E.UnresolvedColumn) and o.child.name.lower() in aliases:
                key = aliases[o.child.name.lower()]
            orders.append(E.SortOrder(_strip(key if key is not None else o.child), o.ascending, o.nulls_first))

        aggregates: dict[Any, tuple[str, E.Expr]] = {}

        def collect(e: E.Expr) -> None:
            for node in E.walk(e):
                if isinstance(node, E.AggregateCall) and _canon(node) not in aggregates:
                    aggregates[_canon(node)] = (f"{_HIDDEN}a{len(aggregates)}", node)

        for e in [*items, *([sel.having] if sel.having is not None else []), *(o.child for o in orders)]:
            collect(e)

        def rewrite(e: E.Expr) -> E.Expr:
            def fn(node: E.Expr):
                c = _canon(_strip(node))
                if c in key_map:
                    return E.UnresolvedColumn(key_map[c])
                if isinstance(node, E.AggregateCall):
                    return E.UnresolvedColumn(aggregates[c][0])
                return None

            return E.transform_down(e, fn)

        key_columns = [Column(k).alias(key_map[_canon(k)]) for k in unique_keys]
        if aggregates:
            g = df.groupBy(*key_columns).agg(*[Column(a).alias(n) for n, a in aggregates.values()])
        else:  # GROUP BY without aggregates: the distinct keys
            g = df.select(*key_columns).distinct()
        try:
            if sel.having is not None:
                g = g.filter(Column(rewrite(sel.having)))
            if orders and not sel.distinct:
                g = g.orderBy(*[Column(E.SortOrder(rewrite(o.child), o.ascending, o.nulls_first)) for o in orders])
            out = g.select(*[Column(E.Alias(rewrite(_strip(i)), expr_name(i))) for i in items])
        except ColumnNotFoundError as exc:
            raise AnalysisError("an expression of the select list, HAVING or ORDER BY is neither grouped nor an "
                                f"aggregate ({exc.message.split(' not found')[0]})") from None
        if sel.distinct:
            out = out.distinct()
            if orders:
                out = out.orderBy(*[Column(o) for o in self.output_orders(out, order, items)])
        return out


def _strip(e: E.Expr) -> E.Expr:
    return e.child if isinstance(e, E.Alias) else e


def _canon(e: E.Expr) -> Any:
    """A comparable form: names case-insensitive, aliases ignored."""
    def fn(node: E.Expr):
        if isinstance(node, E.UnresolvedColumn):
            return E.UnresolvedColumn(node.name.lower())
        if isinstance(node, E.Alias):
            return node.child
        return node

    return E.transform(_strip(e), fn)
