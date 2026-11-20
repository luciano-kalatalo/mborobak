"""Scalar functions: typing, default names and IRIS SQL for each ``Func`` (compiler spec §7.10).

Each function has a ``Spec``: how to type it from its arguments (Spark's result types, verified
against PySpark 4.3), how PySpark names it when it is not aliased, and how to render it in IRIS SQL.
The rendering follows the IRIS probe of 2026-11-19 (``probes/scalar_function_probe.py``):

* IRIS keeps empty strings distinct from NULL in function results, as Spark does.
* ``NULLIF`` and ``GREATEST``/``LEAST`` compare with the column's collation, so string arguments get
  ``%EXACT``; ``INSTR`` and ``REPLACE`` are case-sensitive already.
* ``GREATEST``/``LEAST`` return NULL if any argument is NULL; Spark skips NULLs.
* Math functions raise errors where Spark returns NaN, NULL or infinity (``SQRT(-1)``, ``LOG(0)``,
  ``EXP(1000)``, ``POWER(0, -1)``, ``ROUND`` of NaN or infinity), so the SQL routes those inputs first.

Renderers build their text strictly left to right and render an argument again at each use, so the
parameters of a repeated argument are collected in text order.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable

from . import analyzer as A
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
    NullType,
    StringType,
    TimestampNTZType,
)

Typed = tuple[DataType, bool]


@dataclass(frozen=True)
class Spec:
    typer: Callable[[E.Func, list[Typed], str | None], Typed]
    render: Callable[[object, E.Func], str]
    label: str | None = None              # PySpark's display name, if not the key (CEIL, POWER, ln …)
    predicate: bool = False               # renders as a condition (isnan)
    exact_args: bool = False              # compares its arguments: strings need %EXACT (D10)
    namer: Callable[[E.Func, Callable], str] | None = None


REGISTRY: dict[str, Spec] = {}


def spec(name: str) -> Spec:
    try:
        return REGISTRY[name]
    except KeyError:
        raise UnsupportedOperationError(f"unknown function {name}") from None


# --------------------------------------------------------------------------- typing and naming

def infer(expr: E.Func, operation: str | None) -> Typed:
    args = [A.infer(a, operation) for a in expr.args]
    return spec(expr.name).typer(expr, args, operation)


def name(expr: E.Func, expr_name: Callable) -> str:
    s = spec(expr.name)
    if s.namer is not None:
        return s.namer(expr, expr_name)
    return f"{s.label or expr.name}({', '.join(expr_name(a) for a in expr.args)})"


def _fail(fn: str, what: str, got: DataType, operation: str | None):
    raise AnalysisError(f"{fn}() needs {what}, got {got.simpleString()}", operation=operation)


def _string(fn: str, t: DataType, operation) -> None:
    if not isinstance(t, (StringType, NullType)):
        _fail(fn, "a string", t, operation)


def _number(fn: str, t: DataType, operation) -> None:
    if not (t.is_numeric or isinstance(t, NullType)):
        _fail(fn, "a number", t, operation)


def _common(fn: str, args: list[Typed], operation) -> DataType:
    result: DataType = NullType()
    for t, _ in args:
        w = A.wider(result, t)
        if w is None:
            raise AnalysisError(f"{fn}() arguments have different types: {result.simpleString()} and "
                                f"{t.simpleString()}", operation=operation)
        result = w
    return result


def _const(e: E.Expr):
    return e.value if isinstance(e, E.Literal) else None


# Types whose text form is the same in IRIS and Spark (compatibility matrix CAST-11); double and
# decimal are not (CAST-12, CAST-13).
_CONCAT_TYPES = (StringType, NullType, IntegerType, LongType, BooleanType, DateType, TimestampNTZType)


# --------------------------------------------------------------------------- render helpers

def _x(r, arg: E.Expr, to: DataType | None = None) -> Callable[[], str]:
    """A renderer of ``arg`` (cast to ``to`` if needed), to call once per occurrence in the text."""
    if to is not None:
        t, _ = A.infer(arg)
        if t != to:
            arg = E.Cast(arg, to)
    return lambda: r.value(arg)


def _double(r, arg: E.Expr) -> Callable[[], str]:
    return _x(r, arg, DoubleType())


def _special(r, value: float) -> str:
    """NaN and infinity have no IRIS literal: they are parameters (§9.5)."""
    return r.lit(E.Literal(value, DoubleType()))


def _sql_type(t: DataType) -> str:
    from .compiler import sql_type

    return sql_type(t)


def _power10(n: int) -> str:
    """10**n as SQL number text, n may be negative."""
    return "1" + "0" * n if n >= 0 else "0." + "0" * (-n - 1) + "1"


# --------------------------------------------------------------------------- null functions

def _t_coalesce(e, args, op):
    if not args:
        raise AnalysisError(f"{e.name}() needs at least one argument", operation=op)
    return _common(e.name, args, op), all(n for _, n in args)


def _r_coalesce(r, e):
    t, _ = A.infer(e)
    parts = [_x(r, a, t) for a in e.args]
    return f"COALESCE({', '.join(p() for p in parts)})"


def _t_nvl2(e, args, op):
    return _common("nvl2", args[1:], op), args[1][1] or args[2][1]


def _r_nvl2(r, e):
    t, _ = A.infer(e)
    a, b, c = _x(r, e.args[0]), _x(r, e.args[1], t), _x(r, e.args[2], t)
    return f"CASE WHEN {a()} IS NOT NULL THEN {b()} ELSE {c()} END"


def _t_nullif(e, args, op):
    _common("nullif", args, op)
    return args[0][0], True


def _r_nullif(r, e):
    t = _common("nullif", [A.infer(a) for a in e.args], None)
    a, b = _x(r, e.args[0], t), _x(r, e.args[1], t)
    if isinstance(t, DoubleType):  # Spark treats NaN as equal to NaN
        return f"CASE WHEN {a()} = {b()} OR ({a()} <> {a()} AND {b()} <> {b()}) THEN NULL ELSE {a()} END"
    return f"NULLIF({a()}, {b()})"


def _t_isnan(e, args, op):
    _number("isnan", args[0][0], op)
    return BooleanType(), False


def _p_isnan(r, e):
    t, _ = A.infer(e.args[0])
    if not isinstance(t, DoubleType):
        return "1 = 0"  # only a double can be NaN
    # IRIS finds NaN with x <> x inside CASE only; in a WHERE clause the same test is never true.
    x = _x(r, e.args[0])
    return f"CASE WHEN {x()} <> {x()} THEN 1 ELSE 0 END = 1"


def _t_greatest(e, args, op):
    if len(args) < 2:
        raise AnalysisError(f"{e.name}() needs at least two arguments", operation=op)
    return _common(e.name, args, op), all(n for _, n in args)


def _r_greatest(r, e):
    # Spark skips NULLs; IRIS returns NULL if any argument is NULL. Each argument is replaced by the
    # first non-NULL of (itself, the others): that value is among the arguments, so the result is
    # unchanged, and it is NULL only when all are.
    t, _ = A.infer(e)
    text = isinstance(t, StringType) and r.c.options.string_semantics == "pyspark"
    n = len(e.args)
    items = []
    for i in range(n):
        order = [i] + [j for j in range(n) if j != i]
        item = "COALESCE(" + ", ".join(_x(r, e.args[j], t)() for j in order) + ")"
        # strings compare by code point (§7.1): with a leading space, then without it
        items.append(f"%EXACT(' ' || {item})" if text else item)
    call = f"{'GREATEST' if e.name == 'greatest' else 'LEAST'}({', '.join(items)})"
    return f"SUBSTRING({call}, 2)" if text else call


# --------------------------------------------------------------------------- string functions

def _t_string1(e, args, op):
    _string(e.name, args[0][0], op)
    return StringType(), args[0][1]


def _r_wrap(sql_name: str):
    return lambda r, e: f"{sql_name}({r.value(e.args[0])})"


def _t_length(e, args, op):
    _string(e.name, args[0][0], op)
    return IntegerType(), args[0][1]


def _t_substring(e, args, op):
    _string(e.name, args[0][0], op)
    return StringType(), args[0][1]


def _r_substring(r, e):
    # Spark's substringSQL: 1-based; 0 counts as 1; a negative position counts from the end; the
    # window [start, start + len) is clipped to the string. IRIS's SUBSTRING does not handle 0 or
    # negative positions that way.
    pos, length = _const(e.args[1]), _const(e.args[2])
    s = _x(r, e.args[0])
    if length <= 0:
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE '' END"
    # IRIS returns '' for a length beyond its string limit; substr(s, pos) uses 2**31 - 1. Lengths
    # past the longest IRIS string are read as "to the end".
    rest = length >= _MAX_STRING
    tail = "" if rest else f", {length}"
    if pos > 0:
        return f"SUBSTRING({s()}, {pos}{tail})"
    if pos == 0:
        return f"SUBSTRING({s()}, 1{tail})"
    if rest:
        return (f"CASE WHEN CHAR_LENGTH({s()}) + ({pos}) >= 0 THEN SUBSTRING({s()}, CHAR_LENGTH({s()}) + ({pos + 1})) "
                f"ELSE {s()} END")
    return (f"CASE WHEN {s()} IS NULL THEN NULL "
            f"WHEN CHAR_LENGTH({s()}) + ({pos}) >= 0 THEN SUBSTRING({s()}, CHAR_LENGTH({s()}) + ({pos + 1}), {length}) "
            f"WHEN CHAR_LENGTH({s()}) + ({pos + length}) > 0 THEN SUBSTRING({s()}, 1, CHAR_LENGTH({s()}) + ({pos + length})) "
            f"ELSE '' END")


_MAX_STRING = 3641144  # longest IRIS string


def _t_concat(e, args, op):
    if not args:
        raise AnalysisError("concat() needs at least one column", operation=op)
    for t, _ in args:
        if not isinstance(t, _CONCAT_TYPES):
            raise UnsupportedOperationError(
                f"concat() of a {t.simpleString()} column is not supported: IRIS and Spark write "
                f"{t.simpleString()} values as text differently (compatibility matrix CAST-12, CAST-13)",
                operation=op)
    return StringType(), any(n for _, n in args)


def _r_concat(r, e):
    return "(" + " || ".join(_x(r, a, StringType())() for a in e.args) + ")"


def _t_concat_ws(e, args, op):
    _t_concat(e, args[1:] or [(StringType(), False)], op)
    return StringType(), False


def _r_concat_ws(r, e):
    # Spark skips NULLs and returns '' when all are NULL. Each value contributes sep || value (NULL
    # when the value is NULL, then ''); the leading separator is cut off.
    sep = _const(e.args[0])
    parts = []
    for a in e.args[1:]:
        v = _x(r, a, StringType())
        parts.append(f"COALESCE({r.lit(E.Literal(sep, StringType()))} || {v()}, '')")
    if not parts:
        return "''"
    return f"SUBSTRING({' || '.join(parts)}, {len(sep) + 1})"


def _t_pad(e, args, op):
    _string(e.name, args[0][0], op)
    return StringType(), args[0][1]


def _r_pad(r, e):
    length, pad = _const(e.args[1]), _const(e.args[2])
    s = _x(r, e.args[0])
    if length <= 0:
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE '' END"
    if pad == "":
        # Spark only truncates; IRIS pads with NUL characters
        return f"SUBSTRING({s()}, 1, {length})"
    fn = "LPAD" if e.name == "lpad" else "RPAD"
    return f"{fn}({s()}, {length}, {r.lit(E.Literal(pad, StringType()))})"


def _t_instr(e, args, op):
    _string(e.name, args[0][0], op)
    _string(e.name, args[1][0], op)
    return IntegerType(), args[0][1] or args[1][1]


def _r_instr(r, e):
    s, sub = _x(r, e.args[0]), _x(r, e.args[1])
    if _const(e.args[1]) == "":
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE 1 END"
    return f"INSTR({s()}, {sub()})"


def _t_locate(e, args, op):
    _string("locate", args[0][0], op)
    _string("locate", args[1][0], op)
    return IntegerType(), args[0][1] or args[1][1]


def _r_locate(r, e):
    # locate(substr, str, pos): 0 when pos < 1; an empty substr is found at 1.
    sub, s, pos = _x(r, e.args[0]), _x(r, e.args[1]), _const(e.args[2])
    if pos < 1:
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE 0 END"
    if _const(e.args[0]) == "":  # Spark's indexOf of an empty string is 0, whatever the start
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE 1 END"
    return f"INSTR({s()}, {sub()}, {pos})"


def _t_replace(e, args, op):
    for t, _ in args:
        _string("replace", t, op)
    return StringType(), any(n for _, n in args)


def _r_replace(r, e):
    # Spark returns the source unchanged for an empty search string; IRIS's REPLACE('', '', 'x') is 'x'.
    src, search, repl = (_x(r, a) for a in e.args)
    found = _const(e.args[1])
    if found == "":
        return src()
    if isinstance(found, str):
        return f"REPLACE({src()}, {search()}, {repl()})"
    return f"CASE WHEN CHAR_LENGTH({search()}) = 0 THEN {src()} ELSE REPLACE({src()}, {search()}, {repl()}) END"


def _t_repeat(e, args, op):
    _string("repeat", args[0][0], op)
    return StringType(), args[0][1]


def _r_repeat(r, e):
    n = _const(e.args[1])
    s = _x(r, e.args[0])
    if n <= 0:
        return f"CASE WHEN {s()} IS NULL THEN NULL ELSE '' END"
    return f"REPEAT({s()}, {n})"


# --------------------------------------------------------------------------- math functions

def _t_abs(e, args, op):
    _number("abs", args[0][0], op)
    t = args[0][0]
    return (DoubleType() if isinstance(t, NullType) else t), args[0][1]


def _r_abs(r, e):
    t, _ = A.infer(e)
    return f"ABS({_x(r, e.args[0], t)()})"


def round_type(t: DataType, scale: int) -> DataType:
    """Spark's RoundBase.dataType."""
    if isinstance(t, DecimalType):
        integral = t.precision - t.scale + 1
        if scale < 0:
            return DecimalType(min(max(integral, -scale + 1), 38), 0)
        new_scale = min(t.scale, scale)
        return DecimalType(min(integral + new_scale, 38), new_scale)
    if isinstance(t, NullType):
        return DoubleType()
    return t


def _t_round(e, args, op):
    _number(e.name, args[0][0], op)
    return round_type(args[0][0], _const(e.args[1])), args[0][1]


_EXACT_DOUBLE = "9007199254740992"  # 2**53: every double at least this large is an integer


def _half_even(v: Callable[[], str], scale: int) -> str:
    m = _power10(scale)
    return (f"CASE WHEN ABS({v()} * {m} - TRUNCATE({v()} * {m}, 0)) = 0.5 AND MOD(TRUNCATE({v()} * {m}, 0), 2) = 0 "
            f"THEN TRUNCATE({v()}, {scale}) ELSE ROUND({v()}, {scale}) END")


def _r_round(r, e):
    scale = _const(e.args[1])
    t, _ = A.infer(e.args[0])
    result, _ = A.infer(e)
    even = e.name == "bround"
    if isinstance(t, DoubleType) or isinstance(t, NullType):
        # Spark rounds the double's shortest decimal form (0.285 → 0.29); IRIS rounds its 20-digit
        # binary expansion (0.28499999999999997557 → 0.28). Rounding first to 15 significant digits
        # recovers the shortest form for every double that has at most 15 of them. NaN, infinity and
        # values IRIS can't round (beyond its decimal range) are returned as they are.
        x = _double(r, e.args[0])
        big = _EXACT_DOUBLE if scale >= 0 else "1E100"
        pre = lambda: f"ROUND({x()}, 15 - FLOOR(LOG10(ABS({x()}))))"  # noqa: E731
        rounded = _half_even(pre, scale) if even else f"ROUND({pre()}, {scale})"
        return (f"CASE WHEN {x()} IS NULL THEN NULL WHEN {x()} <> {x()} OR ABS({x()}) >= {big} THEN {x()} "
                f"WHEN ABS({x()}) < 1E-100 THEN CAST(0 AS DOUBLE) "
                f"ELSE CAST({rounded} AS DOUBLE) END")
    x = _x(r, e.args[0])
    if t.is_integral and scale >= 0:
        return x()
    rounded = _half_even(x, scale) if even else f"ROUND({x()}, {scale})"
    return f"CAST({rounded} AS {_sql_type(result)})"


def _t_ceil(e, args, op):
    t = args[0][0]
    _number(e.name, t, op)
    if isinstance(t, DecimalType):
        if t.scale == 0:
            return t, args[0][1]
        return DecimalType(min(t.precision - t.scale + 1, 38), 0), args[0][1]
    return LongType(), args[0][1]


def _r_ceil(r, e):
    fn = "FLOOR" if e.name == "floor" else "CEILING"
    t, _ = A.infer(e.args[0])
    result, _ = A.infer(e)
    if isinstance(t, (DoubleType, NullType)):
        # Spark (ANSI): NaN → 0; infinity and results beyond bigint fail with an overflow. The failure is
        # EXP(100000), which raises <MAXNUMBER> only for the rows that reach it (as the casts of §7.7).
        x = _double(r, e.args[0])
        return (f"CASE WHEN {x()} IS NULL THEN NULL WHEN {x()} <> {x()} THEN 0 "
                f"WHEN {fn}({x()}) >= 9.223372036854775807E18 OR {fn}({x()}) < -9.223372036854775808E18 "
                f"THEN EXP(100000 + LENGTH(CAST({x()} AS VARCHAR(60))) - LENGTH(CAST({x()} AS VARCHAR(60)))) "
                f"ELSE CAST({fn}({x()}) AS BIGINT) END")
    x = _x(r, e.args[0])
    if t.is_integral:
        return f"CAST({x()} AS BIGINT)"
    if t.scale == 0:
        return x()
    return f"CAST({fn}({x()}) AS {_sql_type(result)})"


def _t_double(e, args, op):
    for t, _ in args:
        _number(e.name, t, op)
    return DoubleType(), any(n for _, n in args)


def _t_double_nullable(e, args, op):
    _t_double(e, args, op)
    return DoubleType(), True


def _r_pow(r, e):
    a, b = _double(r, e.args[0]), _double(r, e.args[1])
    return (f"CASE WHEN {a()} IS NULL OR {b()} IS NULL THEN NULL "
            f"WHEN {a()} = 0 AND {b()} < 0 THEN {_special(r, math.inf)} "
            f"WHEN {a()} < 0 AND {b()} <> FLOOR({b()}) THEN {_special(r, math.nan)} "
            f"ELSE POWER({a()}, {b()}) END")


def _r_sqrt(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} < 0 THEN {_special(r, math.nan)} ELSE SQRT({x()}) END"


def _r_exp(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} > 709.782712893384 THEN {_special(r, math.inf)} ELSE EXP({x()}) END"


# IRIS evaluates NaN <= 0 as true, so the log functions test for NaN (x <> x) first.


def _r_ln(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} <> {x()} THEN {x()} WHEN {x()} <= 0 THEN NULL ELSE LOG({x()}) END"


def _r_log10(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} <> {x()} THEN {x()} WHEN {x()} <= 0 THEN NULL ELSE LOG10({x()}) END"


def _r_log2(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} <> {x()} THEN {x()} WHEN {x()} <= 0 THEN NULL ELSE LOG({x()}) / LOG(2) END"


def _r_log1p(r, e):
    # LOG(1 + x) loses x when it is tiny; there the Taylor series x - x²/2 + x³/3 - x⁴/4 is exact to
    # double precision, and below 1E-150 log1p(x) is x itself (x² would underflow).
    x = _double(r, e.args[0])
    return (f"CASE WHEN {x()} <> {x()} THEN {x()} WHEN {x()} <= -1 THEN NULL WHEN ABS({x()}) < 1E-150 THEN {x()} "
            f"WHEN ABS({x()}) < 1E-4 THEN {x()} - {x()} * {x()} / 2 + {x()} * {x()} * {x()} / 3 "
            f"- {x()} * {x()} * {x()} * {x()} / 4 "
            f"ELSE LOG(1 + {x()}) END")


def _r_log_base(r, e):
    b, x = _double(r, e.args[0]), _double(r, e.args[1])
    return (f"CASE WHEN {b()} <> {b()} THEN {b()} WHEN {x()} <> {x()} THEN {x()} "
            f"WHEN {b()} <= 0 OR {x()} <= 0 THEN NULL ELSE LOG({x()}) / LOG({b()}) END")


def _r_signum(r, e):
    x = _double(r, e.args[0])
    return f"CASE WHEN {x()} IS NULL THEN NULL WHEN {x()} <> {x()} THEN {x()} ELSE CAST(SIGN({x()}) AS DOUBLE) END"


def _t_pmod(e, args, op):
    return A.infer(E.BinaryArithmetic("%", e.args[0], e.args[1]), op)


def _r_pmod(r, e):
    # Spark: r = a % n; if r < 0 then (r + n) % n else r
    rem = E.BinaryArithmetic("%", e.args[0], e.args[1])
    n = e.args[1]
    return (f"CASE WHEN {r.value(rem)} < 0 THEN MOD({r.value(rem)} + {r.value(n)}, {r.value(n)}) "
            f"ELSE {r.value(rem)} END")


# --------------------------------------------------------------------------- registry

def _register() -> None:
    R = REGISTRY
    for key in ("coalesce", "ifnull", "nvl"):
        R[key] = Spec(_t_coalesce, _r_coalesce)
    R["nvl2"] = Spec(_t_nvl2, _r_nvl2)
    R["nullif"] = Spec(_t_nullif, _r_nullif, exact_args=True)
    R["isnan"] = Spec(_t_isnan, _p_isnan, predicate=True)
    R["greatest"] = Spec(_t_greatest, _r_greatest, exact_args=True)
    R["least"] = Spec(_t_greatest, _r_greatest, exact_args=True)

    for key, sql in (("upper", "UPPER"), ("lower", "LOWER"), ("trim", "TRIM"), ("ltrim", "LTRIM"),
                     ("rtrim", "RTRIM"), ("initcap", "INITCAP"), ("reverse", "REVERSE")):
        R[key] = Spec(_t_string1, _r_wrap(sql))
    for key in ("length", "char_length", "character_length"):
        R[key] = Spec(_t_length, _r_wrap("CHAR_LENGTH"))  # LENGTH ignores trailing spaces in IRIS
    for key in ("substring", "substr"):
        R[key] = Spec(_t_substring, _r_substring)
    R["concat"] = Spec(_t_concat, _r_concat)
    R["concat_ws"] = Spec(_t_concat_ws, _r_concat_ws)
    R["lpad"] = Spec(_t_pad, _r_pad)
    R["rpad"] = Spec(_t_pad, _r_pad)
    R["instr"] = Spec(_t_instr, _r_instr)
    R["locate"] = Spec(_t_locate, _r_locate)
    R["replace"] = Spec(_t_replace, _r_replace)
    R["repeat"] = Spec(_t_repeat, _r_repeat)

    R["abs"] = Spec(_t_abs, _r_abs)
    R["round"] = Spec(_t_round, _r_round)
    R["bround"] = Spec(_t_round, _r_round)
    R["ceil"] = Spec(_t_ceil, _r_ceil, label="CEIL")
    R["ceiling"] = Spec(_t_ceil, _r_ceil)
    R["floor"] = Spec(_t_ceil, _r_ceil, label="FLOOR")
    R["pow"] = Spec(_t_double, _r_pow, label="POWER")
    R["sqrt"] = Spec(_t_double, _r_sqrt, label="SQRT")
    R["exp"] = Spec(_t_double, _r_exp, label="EXP")
    R["ln"] = Spec(_t_double_nullable, _r_ln)
    R["log10"] = Spec(_t_double_nullable, _r_log10, label="LOG10")
    R["log2"] = Spec(_t_double_nullable, _r_log2, label="LOG2")
    R["log1p"] = Spec(_t_double_nullable, _r_log1p, label="LOG1P")
    R["log"] = Spec(_t_double_nullable, _r_log_base, label="LOG")
    R["signum"] = Spec(_t_double, _r_signum, label="SIGNUM")
    R["sign"] = Spec(_t_double, _r_signum)
    R["pmod"] = Spec(_t_pmod, _r_pmod)


_register()


from . import dates as _dates  # noqa: E402,F401 - registers the date and time functions
