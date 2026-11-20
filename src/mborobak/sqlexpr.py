"""Spark SQL expression strings: ``F.expr``, ``selectExpr``, ``filter("…")`` (API contract §4.1, §6.4).

A parser for the subset of Spark SQL's expression grammar that the DataFrame API covers. It builds the
same expression nodes as the Column API, so typing, IRIS translation and results are the same; where
Spark names a SQL expression differently from its Column-API twin (``pow``, ``log(x)``, ``BETWEEN``,
``IF``), the parsed node carries Spark's name.

Grammar, from Spark's ``SqlBaseParser`` (lowest precedence first):

    named      := expr [[AS] identifier]
    expr       := or
    or         := and (OR and)*
    and        := not (AND not)*
    not        := NOT not | predicate
    predicate  := value [ [NOT] BETWEEN value AND value | [NOT] IN ( expr, … ) | [NOT] LIKE value
                        | IS [NOT] NULL ]
    value      := concat ((= | == | <> | != | < | <= | > | >= | <=>) concat)*
    concat     := term ((+ | - | ||) term)*
    term       := unary ((* | / | %) unary)*
    unary      := (- | +) unary | primary
    primary    := literal | identifier[.identifier] | * | ( expr ) | CASE … END | CAST(expr AS type)
                | function ( [DISTINCT] args | * ) [OVER ( window )]

Not supported (``UnsupportedOperationError``): RLIKE/REGEXP (no regular expressions in IRIS SQL),
ILIKE, DIV, bitwise operators, subqueries, INTERVAL, lambda functions, array and map access.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import re
from dataclasses import dataclass
from typing import Any

from . import expressions as E
from .errors import AnalysisError, UnsupportedOperationError
from .sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampNTZType,
    parse_type,
)

__all__ = ["parse_expression", "parse_named", "ParseError"]


class ParseError(AnalysisError):
    """A SQL expression string that can't be parsed (Spark's ParseException)."""


# --------------------------------------------------------------------------- tokens

@dataclass(frozen=True)
class Tok:
    kind: str     # num, str, ident, qident (backticks), op, end
    text: str
    pos: int
    value: Any = None


_NUMBER = re.compile(r"(\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?)(BD|bd|[LlDdSsYyFf])?(?![A-Za-z_0-9])")
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_OPS = ("<=>", "==", "!=", "<>", "<=", ">=", "||", "=", "<", ">", "+", "-", "*", "/", "%", "(", ")", ",", ".",
        "~", "&", "|", "^", "[", ":", "?", ";")
_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\0", "b": "\b", "Z": "\x1a", "\\": "\\", "'": "'", '"': '"',
            "%": "\\%", "_": "\\_"}


def tokenize(text: str) -> list[Tok]:
    toks: list[Tok] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "'\"":
            j, out = i + 1, []
            while True:
                if j >= n:
                    raise ParseError(f"unterminated string literal at position {i} in {text!r}")
                c = text[j]
                if c == "\\" and j + 1 < n:
                    out.append(_ESCAPES.get(text[j + 1], text[j + 1]))
                    j += 2
                    continue
                if c == ch:
                    break
                out.append(c)
                j += 1
            value = "".join(out)
            if toks and toks[-1].kind == "str":  # adjacent literals concatenate: 'a' 'b' is 'ab'
                prev = toks.pop()
                value = prev.value + value
                i = prev.pos
            toks.append(Tok("str", text[i:j + 1], i, value))
            i = j + 1
            continue
        if ch == "`":
            j = text.find("`", i + 1)
            while j != -1 and j + 1 < n and text[j + 1] == "`":  # `` inside quotes
                j = text.find("`", j + 2)
            if j == -1:
                raise ParseError(f"unterminated quoted identifier at position {i} in {text!r}")
            toks.append(Tok("qident", text[i:j + 1], i, text[i + 1:j].replace("``", "`")))
            i = j + 1
            continue
        if ch.isdigit() or (ch == "." and i + 1 < n and text[i + 1].isdigit()):
            m = _NUMBER.match(text, i)
            if not m:
                raise ParseError(f"invalid number at position {i} in {text!r}")
            toks.append(Tok("num", m.group(0), i, (m.group(1), (m.group(2) or "").upper())))
            i = m.end()
            continue
        m = _IDENT.match(text, i)
        if m:
            toks.append(Tok("ident", m.group(0), i, m.group(0)))
            i = m.end()
            continue
        for op in _OPS:
            if text.startswith(op, i):
                toks.append(Tok("op", op, i))
                i += len(op)
                break
        else:
            raise ParseError(f"unexpected character {ch!r} at position {i} in {text!r}")
    toks.append(Tok("end", "", n))
    return toks


def number_literal(digits: str, suffix: str) -> E.Literal:
    """Spark SQL's literal types: 1 int (bigint, decimal when larger), 1.5 decimal, 1e3 double, 1L, 1D, 1BD."""
    if suffix in ("S", "Y", "F"):
        raise UnsupportedOperationError(f"{digits}{suffix}: smallint, tinyint and float literals are not supported")
    if suffix == "D" or ("e" in digits.lower() and suffix != "BD"):
        return E.Literal(float(digits), DoubleType())
    if suffix == "L":
        return E.Literal(int(decimal.Decimal(digits)), LongType())
    if "." in digits or suffix == "BD":
        value = decimal.Decimal(digits)
        sign, ds, exp = value.as_tuple()
        scale = max(0, -exp)
        return E.Literal(value, DecimalType(max(len(ds), scale, 1), scale))
    value = int(digits)
    if value <= 2**31 - 1:
        return E.Literal(value, IntegerType())
    if value <= 2**63 - 1:
        return E.Literal(value, LongType())
    return E.Literal(decimal.Decimal(value), DecimalType(min(len(digits), 38), 0))


# --------------------------------------------------------------------------- parser

_RESERVED = {"AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END",
             "AS", "OVER", "DISTINCT", "TRUE", "FALSE", "CAST", "RLIKE", "REGEXP", "ILIKE", "DIV", "FROM",
             "PARTITION", "ORDER", "BY", "ROWS", "RANGE", "ASC", "DESC", "NULLS", "FIRST", "LAST"}
_NO_PAREN_FUNCTIONS = {"current_date", "current_timestamp"}


def _named(expr: E.Expr, name: str) -> E.Expr:
    """``expr`` under Spark's SQL name (an alias that only sets the default column name)."""
    return E.Alias(expr, name)


class Parser:
    def __init__(self, text: str):
        if not isinstance(text, str):
            raise TypeError("a SQL expression must be a string")
        self.text = text
        self.toks = tokenize(text)
        self.i = 0

    # helpers
    @property
    def tok(self) -> Tok:
        return self.toks[self.i]

    def fail(self, message: str, tok: Tok | None = None):
        tok = tok or self.tok
        where = f"at position {tok.pos}" if tok.kind != "end" else "at the end"
        raise ParseError(f"cannot parse {self.text!r}: {message} {where}")

    def kw(self, *words: str) -> bool:
        t = self.tok
        return t.kind == "ident" and t.text.upper() in words

    def take_kw(self, *words: str) -> bool:
        if self.kw(*words):
            self.i += 1
            return True
        return False

    def expect_kw(self, word: str) -> None:
        if not self.take_kw(word):
            self.fail(f"expected {word}")

    def op(self, *ops: str) -> bool:
        return self.tok.kind == "op" and self.tok.text in ops

    def take_op(self, *ops: str) -> str | None:
        if self.op(*ops):
            t = self.tok.text
            self.i += 1
            return t
        return None

    def expect_op(self, o: str) -> None:
        if not self.take_op(o):
            self.fail(f"expected '{o}'")

    def done(self) -> None:
        if self.tok.kind != "end":
            self.fail(f"unexpected {self.tok.text!r}")

    # entry points
    def named(self) -> E.Expr:
        if self.op("*") and self.toks[self.i + 1].kind == "end":
            self.i += 1
            return E.Star()
        expr = self.expr()
        if self.take_kw("AS"):
            return E.Alias(expr, self.identifier())
        if self.tok.kind in ("ident", "qident") and not self.kw(*_RESERVED):
            return E.Alias(expr, self.identifier())
        return expr

    def identifier(self) -> str:
        t = self.tok
        if t.kind == "qident" or (t.kind == "ident" and t.text.upper() not in _RESERVED):
            self.i += 1
            return t.value
        self.fail("expected a name")

    # boolean levels
    def expr(self) -> E.Expr:
        left = self.and_()
        while self.take_kw("OR"):
            left = E.Or(left, self.and_())
        return left

    def and_(self) -> E.Expr:
        left = self.not_()
        while self.take_kw("AND"):
            left = E.And(left, self.not_())
        return left

    def not_(self) -> E.Expr:
        if self.take_kw("NOT"):
            return E.Not(self.not_())
        return self.predicate()

    def predicate(self) -> E.Expr:
        left = self.value()
        while True:
            negated = False
            start = self.i
            if self.kw("NOT") and self.toks[self.i + 1].kind == "ident" and \
                    self.toks[self.i + 1].text.upper() in ("BETWEEN", "IN", "LIKE", "RLIKE", "REGEXP", "ILIKE"):
                self.i += 1
                negated = True
            if self.take_kw("BETWEEN"):
                low = self.value()
                self.expect_kw("AND")
                high = self.value()
                from .naming import expr_name

                between = E.And(E.Comparison(">=", left, low), E.Comparison("<=", left, high))
                name = f"between({expr_name(left)}, {expr_name(low)}, {expr_name(high)})"
                result: E.Expr = _named(between, name)
            elif self.take_kw("IN"):
                self.expect_op("(")
                if self.kw("SELECT"):
                    raise UnsupportedOperationError("IN with a subquery is not supported")
                values = [self.expr()]
                while self.take_op(","):
                    values.append(self.expr())
                self.expect_op(")")
                result = E.In(left, tuple(values))
            elif self.take_kw("LIKE"):
                result = E.Like(left, self.value())
            elif self.take_kw("RLIKE", "REGEXP"):  # Spark names both RLIKE(s, p)
                result = E.Func("rlike", (left, self.value()))
            elif self.kw("ILIKE"):
                raise UnsupportedOperationError("ILIKE is not supported; use lower(x) LIKE …")
            elif not negated and self.take_kw("IS"):
                neg = self.take_kw("NOT")
                if not self.take_kw("NULL"):
                    self.fail("only IS [NOT] NULL is supported")
                left = E.IsNull(left, negated=neg)
                continue
            else:
                self.i = start
                return left
            left = E.Not(result) if negated else result

    def value(self) -> E.Expr:
        left = self.concat()
        while True:
            o = self.take_op("=", "==", "<>", "!=", "<", "<=", ">", ">=", "<=>")
            if o is None:
                return left
            right = self.concat()
            if o in ("<>", "!="):
                left = E.Not(E.Comparison("=", left, right))  # Spark: (NOT (a = b))
            else:
                left = E.Comparison("=" if o == "==" else o, left, right)

    def concat(self) -> E.Expr:
        left = self.term()
        while True:
            o = self.take_op("+", "-", "||")
            if o is None:
                if self.op("&", "|", "^"):
                    raise UnsupportedOperationError(f"bitwise operator {self.tok.text!r} is not supported")
                return left
            right = self.term()
            left = E.Func("concat", (left, right)) if o == "||" else E.BinaryArithmetic(o, left, right)

    def term(self) -> E.Expr:
        left = self.unary()
        while True:
            if self.kw("DIV"):
                raise UnsupportedOperationError("DIV (integer division) is not supported; use floor(a / b)")
            o = self.take_op("*", "/", "%")
            if o is None:
                return left
            left = E.BinaryArithmetic(o, left, self.unary())

    def unary(self) -> E.Expr:
        if self.take_op("-"):
            operand = self.unary()
            if isinstance(operand, E.Literal) and isinstance(operand.value, (int, float, decimal.Decimal)) \
                    and not isinstance(operand.value, bool):
                return E.Literal(-operand.value, operand.dtype)  # -1 is a literal in Spark
            return E.Negate(operand)
        if self.take_op("+"):
            from .naming import expr_name

            operand = self.unary()
            return _named(operand, f"(+ {expr_name(operand)})")
        if self.op("~"):
            raise UnsupportedOperationError("bitwise operator '~' is not supported")
        return self.primary()

    # primaries
    def primary(self) -> E.Expr:
        t = self.tok
        if t.kind == "num":
            self.i += 1
            return number_literal(*t.value)
        if t.kind == "str":
            self.i += 1
            return E.Literal(t.value, StringType())
        if self.take_op("("):
            if self.kw("SELECT"):
                raise UnsupportedOperationError("subqueries are not supported")
            inner = self.expr()
            self.expect_op(")")
            return inner
        if self.op("*"):
            self.fail("'*' is only allowed alone or in count(*)")
        if t.kind == "qident":
            return self.column()
        if t.kind != "ident":
            self.fail(f"unexpected {t.text!r}")
        word = t.text.upper()
        if word == "NULL":
            self.i += 1
            from .column import literal

            return literal(None)
        if word in ("TRUE", "FALSE"):
            self.i += 1
            return E.Literal(word == "TRUE", BooleanType())
        if word == "CASE":
            return self.case()
        if word == "CAST":
            return self.cast()
        if word in ("DATE", "TIMESTAMP", "TIMESTAMP_NTZ") and self.toks[self.i + 1].kind == "str":
            self.i += 1
            return self.typed_literal(word, self.tok)
        if word == "INTERVAL":
            raise UnsupportedOperationError("INTERVAL literals are not supported")
        if self.toks[self.i + 1].kind == "op" and self.toks[self.i + 1].text == "(":
            return self.call()
        if t.text.lower() in _NO_PAREN_FUNCTIONS:
            self.i += 1
            return self.function(t.text.lower(), [], False, t)
        if word in _RESERVED:
            self.fail(f"unexpected keyword {t.text}")
        return self.column()

    def column(self) -> E.Expr:
        parts = [self.identifier_part()]
        while self.op(".") and self.toks[self.i + 1].kind in ("ident", "qident"):
            self.i += 1
            parts.append(self.identifier_part())
        if self.op("["):
            raise UnsupportedOperationError("array and map access is not supported")
        name = ".".join(f"`{p}`" if ("." in p or "`" in p) else p for p in parts)
        return E.UnresolvedColumn(name)

    def identifier_part(self) -> str:
        t = self.tok
        self.i += 1
        return t.value

    def typed_literal(self, word: str, t: Tok) -> E.Literal:
        self.i += 1
        try:
            if word == "DATE":
                return E.Literal(datetime.date.fromisoformat(t.value.strip()), DateType())
            return E.Literal(datetime.datetime.fromisoformat(t.value.strip()), TimestampNTZType())
        except ValueError:
            self.fail(f"invalid {word} literal {t.value!r}", t)

    def case(self) -> E.Expr:
        self.expect_kw("CASE")
        subject = None if self.kw("WHEN") else self.expr()
        branches = []
        while self.take_kw("WHEN"):
            cond = self.expr()
            self.expect_kw("THEN")
            value = self.expr()
            branches.append((E.Comparison("=", subject, cond) if subject is not None else cond, value))
        if not branches:
            self.fail("CASE needs at least one WHEN")
        otherwise = self.expr() if self.take_kw("ELSE") else None
        self.expect_kw("END")
        return E.CaseWhen(tuple(branches), otherwise)

    def cast(self) -> E.Expr:
        self.expect_kw("CAST")
        self.expect_op("(")
        child = self.expr()
        self.expect_kw("AS")
        start = self.tok.pos
        depth = 0
        while not (self.op(")") and depth == 0):
            if self.tok.kind == "end":
                self.fail("expected ')' after the type")
            if self.op("("):
                depth += 1
            elif self.op(")"):
                depth -= 1
            self.i += 1
        type_text = self.text[start:self.tok.pos].strip()
        self.expect_op(")")
        return E.Cast(child, sql_type(type_text))

    def call(self) -> E.Expr:
        t = self.tok
        name = t.text.lower()
        self.i += 1
        self.expect_op("(")
        distinct = self.take_kw("DISTINCT")
        args: list[E.Expr] = []
        if self.take_op("*"):
            args.append(E.Star())
        elif not self.op(")"):
            args.append(self.expr())
            while self.take_op(","):
                args.append(self.expr())
        self.expect_op(")")
        return self.function(name, args, distinct, t)

    def window(self):
        from .column import Column
        from .sql.window import Window

        self.expect_op("(")
        spec = Window.partitionBy()
        if self.take_kw("PARTITION"):
            self.expect_kw("BY")
            keys = [self.expr()]
            while self.take_op(","):
                keys.append(self.expr())
            spec = spec.partitionBy(*[Column(k) for k in keys])
        if self.take_kw("ORDER"):
            self.expect_kw("BY")
            orders = [self.sort_key()]
            while self.take_op(","):
                orders.append(self.sort_key())
            spec = spec.orderBy(*[Column(o) for o in orders])
        if self.kw("ROWS", "RANGE"):
            kind = self.tok.text.upper()
            self.i += 1
            if self.take_kw("BETWEEN"):
                start = self.bound()
                self.expect_kw("AND")
                end = self.bound()
            else:
                start, end = self.bound(), 0
            spec = spec.rowsBetween(start, end) if kind == "ROWS" else spec.rangeBetween(start, end)
            spec = type(spec)(spec._partition, spec._order, dataclasses.replace(spec._frame, written=True))
        self.expect_op(")")
        return spec

    def sort_key(self) -> E.SortOrder:
        key = self.expr()
        ascending = not self.take_kw("DESC")
        if ascending:
            self.take_kw("ASC")
        nulls = None
        if self.take_kw("NULLS"):
            if self.take_kw("FIRST"):
                nulls = True
            else:
                self.expect_kw("LAST")
                nulls = False
        return E.SortOrder(key, ascending, nulls)

    def bound(self) -> int:
        from .sql.window import Window

        if self.take_kw("UNBOUNDED"):
            if self.take_kw("PRECEDING"):
                return Window.unboundedPreceding
            self.expect_kw("FOLLOWING")
            return Window.unboundedFollowing
        if self.take_kw("CURRENT"):
            self.expect_kw("ROW")
            return 0
        t = self.tok
        if t.kind != "num":
            self.fail("expected a frame bound")
        self.i += 1
        n = int(t.value[0])
        if self.take_kw("PRECEDING"):
            return -n
        self.expect_kw("FOLLOWING")
        return n

    def function(self, name: str, args: list[E.Expr], distinct: bool, tok: Tok) -> E.Expr:
        call = build_call(name, args, distinct, self, tok)
        if self.take_kw("OVER"):
            from .column import Column

            return Column(call).over(self.window())._expr
        return call


# --------------------------------------------------------------------------- types and functions

def sql_type(text: str) -> DataType:
    t = re.sub(r"\s+", " ", text.strip().lower())
    if t in ("timestamp", "timestamp_ltz"):
        return TimestampNTZType()  # Mborobak's only timestamp type (matrix TIME-01)
    if re.fullmatch(r"(var)?char\s*\(\s*\d+\s*\)", t):
        return StringType()
    if t in ("smallint", "short", "tinyint", "byte", "float", "real", "binary"):
        raise UnsupportedOperationError(f"type {text} is not supported")
    try:
        return parse_type("decimal" + t[len("numeric"):] if t.startswith("numeric") else t)
    except ValueError:
        raise UnsupportedOperationError(f"type {text!r} is not supported") from None


# Positional arguments that PySpark's functions take as Python constants, not Columns.
_CONSTANT_ARGS = {"ntile": (0,), "lag": (1, 2), "lead": (1, 2), "months_between": (2,), "first": (1,),
                  "last": (1,), "regexp_extract": (1, 2), "regexp_instr": (2,)}
_SQL_ONLY = {"col", "column", "lit", "when", "expr", "asc", "desc", "asc_nulls_first", "asc_nulls_last",
             "desc_nulls_first", "desc_nulls_last", "count_distinct"}


def build_call(name: str, args: list[E.Expr], distinct: bool, parser: Parser, tok: Tok) -> E.Expr:
    from .column import Column
    from .naming import expr_name
    from .sql import functions as F

    if distinct:
        if name != "count":
            raise UnsupportedOperationError(f"{name}(DISTINCT …) is not supported; only count(DISTINCT …)")
        if len(args) != 1:
            raise UnsupportedOperationError("count(DISTINCT …) over several columns is not supported")
        return F.countDistinct(Column(args[0]))._expr
    if name == "count":
        if len(args) != 1:
            parser.fail("count() takes one argument", tok)
        return F.count("*" if isinstance(args[0], E.Star) else Column(args[0]))._expr
    if any(isinstance(a, E.Star) for a in args):
        parser.fail("'*' is only allowed in count(*)", tok)
    if name == "pow" and len(args) == 2:  # SQL keeps the name as written: pow(i, 2), not POWER(i, 2)
        return E.Func("pow_sql", tuple(args))
    if name == "log" and len(args) == 1:  # Spark SQL's one-argument log is LOG(E(), x)
        return _named(E.Func("ln", tuple(args)), f"LOG(E(), {expr_name(args[0])})")
    if name == "if":
        if len(args) != 3:
            parser.fail("if() takes three arguments", tok)
        cond, a, b = args
        return _named(E.CaseWhen(((cond, a),), b),
                      f"(IF({expr_name(cond)}, {expr_name(a)}, {expr_name(b)}))")
    if name in ("substring", "substr") and len(args) == 2:
        return F.substr(Column(args[0]), Column(args[1]))._expr if name == "substr" else \
            E.Func("substring", (args[0], args[1], E.Literal(2147483647, IntegerType())))
    if name in ("split", "regexp_extract_all"):
        raise UnsupportedOperationError(f"{name}() returns an array, which Mborobak does not support")
    if name == "regexp_extract" and len(args) == 2:
        args = [*args, E.Literal(1, IntegerType())]  # Spark SQL's default group
    fn = getattr(F, name, None) if name not in _SQL_ONLY and not name.startswith("_") else None
    if fn is None or not callable(fn) or name not in F.__all__:
        raise UnsupportedOperationError(f"function {name}() is not supported in SQL expressions")
    call_args: list[Any] = [Column(a) for a in args]
    for i in _CONSTANT_ARGS.get(name, ()):
        if i < len(call_args):
            a = args[i]
            if not isinstance(a, E.Literal):
                parser.fail(f"{name}() takes a constant as argument {i + 1}", tok)
            call_args[i] = a.value
    try:
        return fn(*call_args)._expr
    except TypeError as exc:
        raise ParseError(f"cannot parse {parser.text!r}: {name}(): {exc}") from None


def parse_expression(text: str) -> E.Expr:
    """A boolean or value expression, without an alias (filter, where)."""
    p = Parser(text)
    expr = p.expr()
    p.done()
    return expr


def parse_named(text: str) -> E.Expr:
    """An expression with an optional alias, or ``*`` (selectExpr, F.expr)."""
    p = Parser(text)
    expr = p.named()
    p.done()
    return expr
