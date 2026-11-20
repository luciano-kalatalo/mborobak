"""Date and time functions (compiler spec §7.12), registered in the scalar-function registry.

From the IRIS probe of 2026-11-20 (``probes/date_probe.py``):

* ``DATEADD`` returns a timestamp, so date results are cast back to ``DATE``; its month arithmetic
  clamps to the month's last day, as Spark's ``add_months`` does.
* ``DATEDIFF('dd', a, b)`` takes its dates in the opposite order to Spark's ``datediff(end, start)``.
* ``SECOND()`` rounds the fraction (23:59:59.999999 gives 60); ``DATEPART('ss', …)`` truncates.
* ``HOUR`` of a ``DATE`` returns a wrong value: dates are cast to timestamps first, as Spark does.
* ``WEEK()`` is not ISO 8601; ``LAST_DAY(NULL)`` is a fatal error; there is no ``DATE_TRUNC`` or ``TRUNC``
  for dates, and ``TO_CHAR`` fails on timestamps. Truncation and ``date_format`` are built from parts.
* ``TO_DATE``/``TO_TIMESTAMP`` accept Oracle-style formats, are lenient about digit counts and trailing
  text, and fail on impossible dates; ``YY`` reads ``26`` as 1926. Spark's parsing is strict, so the
  text's shape is checked with ``%PATTERN`` first.

Spark's ``to_timestamp``, ``date_trunc`` and ``current_timestamp`` return ``timestamp`` (with a time
zone); Mborobak's timestamps are ``timestamp_ntz`` by design (compatibility matrix TIME-01, TIME-05).
"""

from __future__ import annotations

from typing import Callable

from . import analyzer as A
from . import expressions as E
from .errors import AnalysisError, UnsupportedOperationError
from .scalar import REGISTRY, Spec, _const, _x
from .sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    NullType,
    StringType,
    TimestampNTZType,
)

DATE, TS = DateType(), TimestampNTZType()


def _datelike(fn: str, t, operation) -> None:
    if not isinstance(t, (DateType, TimestampNTZType, StringType, NullType)):
        raise AnalysisError(f"{fn}() needs a date, a timestamp or date text, got {t.simpleString()}",
                            operation=operation)


def _integral(fn: str, t, operation) -> None:
    if not (t.is_integral or isinstance(t, NullType)):
        raise AnalysisError(f"{fn}() needs an integer number of days or months, got {t.simpleString()}",
                            operation=operation)


def _cast_trigger(s: Callable[[], str]) -> str:
    """The §7.7 cast trigger: raises <RANGE> (MborobakCastError) only for the rows that reach it."""
    return f"$LISTGET({s()}, LENGTH({s()}) - LENGTH({s()}) - 5)"


# --------------------------------------------------------------------------- current values

def _t_now(t):
    return lambda e, args, op: (t, False)


# --------------------------------------------------------------------------- parts

def _t_name(e, args, op):
    _datelike(e.name, args[0][0], op)
    return StringType(), args[0][1]


def _t_part(kind):
    def typer(e, args, op):
        _datelike(e.name, args[0][0], op)
        return IntegerType(), args[0][1]
    return typer


def _r_part(sql: str, to):
    return lambda r, e: sql.format(x=_x(r, e.args[0], to)())


def _r_weekofyear(r, e):
    # ISO 8601: the week of the Thursday of the date's week (Monday first), numbered from 1.
    d = _x(r, e.args[0], DATE)
    return f"CAST(FLOOR((DAYOFYEAR(DATEADD('dd', 3 - MOD(DAYOFWEEK({d()}) + 5, 7), {d()})) - 1) / 7) + 1 AS INTEGER)"


# --------------------------------------------------------------------------- arithmetic

def _t_shift(e, args, op):
    _datelike(e.name, args[0][0], op)
    _integral(e.name, args[1][0], op)
    return DateType(), args[0][1] or args[1][1]


def _r_shift(unit: str, sign: str = ""):
    def render(r, e):
        d, n = _x(r, e.args[0], DATE), _x(r, e.args[1])
        amount = f"0 - ({n()})" if sign == "-" else n()
        return f"CAST(DATEADD('{unit}', {amount}, {d()}) AS DATE)"
    return render


def _t_datediff(e, args, op):
    for t, _ in args:
        _datelike("datediff", t, op)
    return IntegerType(), args[0][1] or args[1][1]


def _r_datediff(r, e):
    end, start = _x(r, e.args[0], DATE), _x(r, e.args[1], DATE)
    return f"DATEDIFF('dd', {start()}, {end()})"  # IRIS: (start, end); Spark: datediff(end, start)


def _t_months_between(e, args, op):
    for t, _ in args[:2]:
        _datelike("months_between", t, op)
    return DoubleType(), args[0][1] or args[1][1]


def _r_months_between(r, e):
    # Spark's DateTimeUtils.monthsBetween: whole months when the days of month are equal or both are
    # month ends; otherwise months plus the difference in seconds over 31 days, rounded to 8 digits
    # (Math.round) unless roundOff is false.
    a, b = _x(r, e.args[0], TS), _x(r, e.args[1], TS)
    round_off = _const(e.args[2])

    def secs(t):
        return (f"(DATEPART('hh', {t()}) * 3600 + DATEPART('mi', {t()}) * 60 + DATEPART('ss', {t()}) "
                f"+ DATEPART('mcs', {t()}) / 1000000)")

    # Every piece is a function, rendered again at each use, so parameters stay in text order.
    def months():
        return f"((YEAR({a()}) - YEAR({b()})) * 12 + MONTH({a()}) - MONTH({b()}))"

    def same():
        return (f"DAYOFMONTH({a()}) = DAYOFMONTH({b()}) OR (DAYOFMONTH({a()}) = DAYOFMONTH(LAST_DAY({a()})) "
                f"AND DAYOFMONTH({b()}) = DAYOFMONTH(LAST_DAY({b()})))")

    def diff():
        return (f"({months()} + ((DAYOFMONTH({a()}) - DAYOFMONTH({b()})) * 86400 + {secs(a)} - {secs(b)}) "
                f"/ 2678400)")

    head = f"CASE WHEN {a()} IS NULL OR {b()} IS NULL THEN NULL WHEN {same()} THEN CAST({months()} AS DOUBLE) "
    value = f"FLOOR({diff()} * 100000000 + 0.5) / 100000000" if round_off else diff()
    return f"{head}ELSE CAST({value} AS DOUBLE) END"


def _t_last_day(e, args, op):
    _datelike("last_day", args[0][0], op)
    return DateType(), args[0][1]


def _r_last_day(r, e):
    d = _x(r, e.args[0], DATE)
    return f"CASE WHEN {d()} IS NULL THEN NULL ELSE CAST(LAST_DAY({d()}) AS DATE) END"  # LAST_DAY(NULL) fails


# --------------------------------------------------------------------------- truncation

_TRUNC_UNITS = {"year": "year", "yyyy": "year", "yy": "year", "quarter": "quarter", "month": "month",
                "mon": "month", "mm": "month", "week": "week"}
_DATE_TRUNC_UNITS = {**_TRUNC_UNITS, "day": "day", "dd": "day", "hour": "hour", "minute": "minute",
                     "second": "second", "millisecond": "millisecond", "microsecond": "microsecond"}


def _first_day(unit: str, d: Callable[[], str]) -> str:
    """The first day of d's year, quarter, month or (Monday) week, as an IRIS timestamp."""
    if unit == "year":
        return f"DATEADD('dd', 1 - DAYOFYEAR({d()}), {d()})"
    if unit == "month":
        return f"DATEADD('dd', 1 - DAYOFMONTH({d()}), {d()})"
    if unit == "quarter":
        return f"DATEADD('mm', 0 - MOD(MONTH({d()}) - 1, 3), DATEADD('dd', 1 - DAYOFMONTH({d()}), {d()}))"
    return f"DATEADD('dd', 0 - MOD(DAYOFWEEK({d()}) + 5, 7), {d()})"  # week


def _t_trunc(e, args, op):
    _datelike("trunc", args[0][0], op)
    return DateType(), True


def _r_trunc(r, e):
    unit = _TRUNC_UNITS.get(str(_const(e.args[1])).lower())
    if unit is None:
        return "CAST(NULL AS DATE)"  # Spark returns NULL for an unknown unit (and for 'day')
    return f"CAST({_first_day(unit, _x(r, e.args[0], DATE))} AS DATE)"


def _t_date_trunc(e, args, op):
    _datelike("date_trunc", args[1][0], op)
    return TimestampNTZType(), True


def _r_date_trunc(r, e):
    unit = _DATE_TRUNC_UNITS.get(str(_const(e.args[0])).lower())
    t = _x(r, e.args[1], TS)
    if unit is None:
        return "CAST(NULL AS TIMESTAMP)"
    if unit in ("year", "quarter", "month", "week"):
        d = _x(r, e.args[1], DATE)
        return f"CAST({_first_day(unit, d)} AS TIMESTAMP)"
    if unit == "microsecond":
        return t()
    day = lambda: f"CAST(CAST({t()} AS DATE) AS TIMESTAMP)"  # noqa: E731
    if unit == "day":
        return day()
    if unit == "hour":
        return f"DATEADD('hh', HOUR({t()}), {day()})"
    if unit == "minute":
        return f"DATEADD('mi', HOUR({t()}) * 60 + MINUTE({t()}), {day()})"
    second = lambda: (f"DATEADD('ss', HOUR({t()}) * 3600 + MINUTE({t()}) * 60 + DATEPART('ss', {t()}), "  # noqa: E731
                      f"{day()})")
    if unit == "second":
        return second()
    return f"DATEADD('ms', FLOOR(DATEPART('mcs', {t()}) / 1000), {second()})"  # millisecond


# --------------------------------------------------------------------------- patterns

def tokens(pattern: str) -> list[tuple[str, str]]:
    """Java DateTimeFormatter pattern → [("field", "yyyy") | ("text", "-")]."""
    out: list[tuple[str, str]] = []
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "'":
            j = i + 1
            text = ""
            while j < n:
                if pattern[j] == "'":
                    if j + 1 < n and pattern[j + 1] == "'":
                        text += "'"
                        j += 2
                        continue
                    break
                text += pattern[j]
                j += 1
            out.append(("text", text if j > i + 1 else "'"))
            i = j + 1
        elif ch.isascii() and ch.isalpha():
            j = i
            while j < n and pattern[j] == ch:
                j += 1
            out.append(("field", pattern[i:j]))
            i = j
        else:
            out.append(("text", ch))
            i += 1
    return out


_FORMAT_FIELDS = "yMdDHhmsSaEQq"


def _check_format(fn: str, pattern, operation) -> None:
    if not isinstance(pattern, str):
        raise AnalysisError(f"{fn}() takes a constant format string", operation=operation)
    for kind, f in tokens(pattern):
        if kind == "field" and (f[0] not in _FORMAT_FIELDS or _piece(f, None) is None):
            raise UnsupportedOperationError(
                f"{fn}(): the pattern letter {f!r} is not supported; supported: y, M, d, D, H, h, m, s, S, a, E, Q",
                operation=operation)


def _piece(f: str, t: Callable[[], str] | None) -> str | None:
    """SQL text for one date_format field of timestamp t (None: not supported). With t None, only checks."""
    c, n = f[0], len(f)
    x = t or (lambda: "")

    def pad(expr: str, width: int) -> str:
        return f"LPAD({expr}, {width}, '0')" if width > 1 else f"CAST({expr} AS VARCHAR(12))"

    if c == "y":
        if n == 2:
            return f"SUBSTRING(LPAD(YEAR({x()}), 4, '0'), 3, 2)"
        return pad(f"YEAR({x()})", n if n >= 4 else 1) if n != 3 else pad(f"YEAR({x()})", 1)
    if c == "M":
        if n <= 2:
            return pad(f"MONTH({x()})", n)
        if n == 3:
            return f"SUBSTRING(MONTHNAME({x()}), 1, 3)"
        return f"MONTHNAME({x()})" if n == 4 else None
    if c == "d":
        return pad(f"DAYOFMONTH({x()})", n) if n <= 2 else None
    if c == "D":
        return pad(f"DAYOFYEAR({x()})", n) if n <= 3 else None
    if c == "H":
        return pad(f"HOUR({x()})", n) if n <= 2 else None
    if c == "h":
        return pad(f"CASE MOD(HOUR({x()}), 12) WHEN 0 THEN 12 ELSE MOD(HOUR({x()}), 12) END", n) if n <= 2 else None
    if c == "m":
        return pad(f"MINUTE({x()})", n) if n <= 2 else None
    if c == "s":
        return pad(f"DATEPART('ss', {x()})", n) if n <= 2 else None
    if c == "S":
        if n > 9:
            return None
        digits = f"LPAD(DATEPART('mcs', {x()}), 6, '0')"
        return f"SUBSTRING({digits}, 1, {n})" if n <= 6 else f"{digits} || '{'0' * (n - 6)}'"
    if c == "a":
        return f"CASE WHEN HOUR({x()}) < 12 THEN 'AM' ELSE 'PM' END" if n == 1 else None
    if c == "E":
        if n <= 3:
            return f"SUBSTRING(DAYNAME({x()}), 1, 3)"
        return f"DAYNAME({x()})" if n == 4 else None
    if c in "Qq":
        return pad(f"QUARTER({x()})", n) if n <= 2 else None
    return None


def _t_date_format(e, args, op):
    _datelike("date_format", args[0][0], op)
    _check_format("date_format", _const(e.args[1]), op)
    return StringType(), args[0][1]


def _r_date_format(r, e):
    t = _x(r, e.args[0], TS)
    parts = []
    for kind, f in tokens(_const(e.args[1])):
        parts.append(r.lit(E.Literal(f, StringType())) if kind == "text" else _piece(f, t))
    if not parts:
        return "''"
    return f"CASE WHEN {t()} IS NULL THEN NULL ELSE {' || '.join(parts)} END"


# Parsing with a pattern: field → (%PATTERN code, IRIS TO_TIMESTAMP format code)
_PARSE = {
    "yyyy": ("4N", "YYYY"), "M": ("1.2N", "MM"), "MM": ("2N", "MM"), "MMM": ("3A", "MON"), "MMMM": ("3.9A", "MONTH"),
    "d": ("1.2N", "DD"), "dd": ("2N", "DD"), "H": ("1.2N", "HH24"), "HH": ("2N", "HH24"),
    "h": ("1.2N", "HH12"), "hh": ("2N", "HH12"), "m": ("1.2N", "MI"), "mm": ("2N", "MI"),
    "s": ("1.2N", "SS"), "ss": ("2N", "SS"), "a": ("2A", "AM"),
}


def parse_plan(fn: str, pattern, operation=None) -> tuple[str, str, list[str]]:
    """(%PATTERN, IRIS format, literal letter runs to blank out) for a Java parsing pattern."""
    if not isinstance(pattern, str):
        raise AnalysisError(f"{fn}() takes a constant format string", operation=operation)
    pat, fmt, blanks = [], [], []
    for kind, f in tokens(pattern):
        if kind == "text":
            for ch in f:
                pat.append('1""""' if ch == '"' else f'1"{ch}"')
            if any(ch.isalpha() for ch in f):
                blanks.append(f)  # IRIS would read letters in the format as codes: blank them in both
                fmt.append(" " * len(f))
            else:
                fmt.append(f)
        elif f in _PARSE:
            pat.append(_PARSE[f][0])
            fmt.append(_PARSE[f][1])
        elif f[0] == "S" and len(f) <= 9:
            pat.append(f"{len(f)}N")
            fmt.append("FF")
        else:
            hint = " (two-digit years are read by IRIS in the 1900s)" if f == "yy" else ""
            raise UnsupportedOperationError(f"{fn}(): the pattern {f!r} is not supported for parsing{hint}",
                                            operation=operation)
    return "".join(pat), "".join(fmt), blanks


def _sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _t_parse(to):
    def typer(e, args, op):
        t = args[0][0]
        if len(e.args) == 1:
            _datelike(e.name, t, op)
            return to, args[0][1]
        if not isinstance(t, (StringType, NullType)):
            raise AnalysisError(f"{e.name}() with a format needs a string, got {t.simpleString()}", operation=op)
        parse_plan(e.name, _const(e.args[1]), op)
        return to, args[0][1]
    return typer


def _r_parse(to):
    def render(r, e):
        if len(e.args) == 1:
            return _x(r, e.args[0], to)()  # a cast: §7.7 for text, CAST for dates and timestamps
        pattern, fmt, blanks = parse_plan(e.name, _const(e.args[1]))
        s = _x(r, e.args[0])
        text = s
        for b in blanks:
            text = (lambda inner, b=b: lambda: f"REPLACE({inner()}, {_sql_text(b)}, {_sql_text(' ' * len(b))})")(text)
        # The CASE yields text (the trigger is text too); TO_TIMESTAMP(NULL) is NULL.
        checked = (f"CASE WHEN {s()} IS NULL THEN NULL WHEN {s()} %PATTERN {_sql_text(pattern)} THEN {text()} "
                   f"ELSE {_cast_trigger(s)} END")
        parsed = f"TO_TIMESTAMP({checked}, {_sql_text(fmt)})"
        return f"CAST({parsed} AS DATE)" if isinstance(to, DateType) else parsed
    return render


# --------------------------------------------------------------------------- registry

def _names_first(e, nm):
    return f"{e.name}({', '.join(nm(a) for a in e.args)})"


def _register() -> None:
    R = REGISTRY
    R["current_date"] = Spec(_t_now(DATE), lambda r, e: "CURRENT_DATE")
    R["current_timestamp"] = Spec(_t_now(TS), lambda r, e: "CURRENT_TIMESTAMP(6)")
    R["now"] = Spec(_t_now(TS), lambda r, e: "CURRENT_TIMESTAMP(6)")
    for key, sql, to in (("year", "YEAR({x})", DATE), ("month", "MONTH({x})", DATE),
                         ("dayofmonth", "DAYOFMONTH({x})", DATE), ("day", "DAYOFMONTH({x})", DATE),
                         ("dayofweek", "DAYOFWEEK({x})", DATE), ("dayofyear", "DAYOFYEAR({x})", DATE),
                         ("quarter", "QUARTER({x})", DATE), ("hour", "HOUR({x})", TS),
                         ("minute", "MINUTE({x})", TS), ("second", "DATEPART('ss', {x})", TS)):
        R[key] = Spec(_t_part(key), _r_part(sql, to))
    R["weekofyear"] = Spec(_t_part("weekofyear"), _r_weekofyear)
    for key, sql in (("dayname", "DAYNAME"), ("monthname", "MONTHNAME")):  # Spark: 'Sat', 'Jan'
        R[key] = Spec(_t_name, (lambda fn: lambda r, e: f"SUBSTRING({fn}({_x(r, e.args[0], DATE)()}), 1, 3)")(sql))
    R["date_add"] = Spec(_t_shift, _r_shift("dd"))
    R["date_sub"] = Spec(_t_shift, _r_shift("dd", "-"))
    R["add_months"] = Spec(_t_shift, _r_shift("mm"))
    R["datediff"] = Spec(_t_datediff, _r_datediff)
    R["months_between"] = Spec(_t_months_between, _r_months_between)
    R["last_day"] = Spec(_t_last_day, _r_last_day)
    R["trunc"] = Spec(_t_trunc, _r_trunc)
    R["date_trunc"] = Spec(_t_date_trunc, _r_date_trunc)
    R["date_format"] = Spec(_t_date_format, _r_date_format)
    R["to_date"] = Spec(_t_parse(DATE), _r_parse(DATE))
    R["to_timestamp"] = Spec(_t_parse(TS), _r_parse(TS))


_register()
