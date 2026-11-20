"""Column functions (API contract §6.4). Import as ``F``."""

from __future__ import annotations

from typing import Any

from .. import expressions as E
from ..column import Column, literal, to_column_expr, to_expr

__all__ = [
    "col",
    "column",
    "lit",
    "when",
    "count",
    "countDistinct",
    "count_distinct",
    "sum",
    "avg",
    "mean",
    "min",
    "max",
    "asc",
    "desc",
    "asc_nulls_first",
    "asc_nulls_last",
    "desc_nulls_first",
    "desc_nulls_last",
]


def col(name: str) -> Column:
    if not isinstance(name, str):
        raise TypeError("col() takes a column name")
    return Column(E.UnresolvedColumn(name))


column = col


def lit(value: Any) -> Column:
    if isinstance(value, Column):
        return value
    return Column(literal(value))


def when(condition: Column, value: Any) -> Column:
    if not isinstance(condition, Column):
        raise TypeError("when() takes a Column condition")
    return Column(E.CaseWhen(((condition._expr, to_expr(value)),), None))


def _agg(function: str, c: Column | str, distinct: bool = False) -> Column:
    return Column(E.AggregateCall(function, to_column_expr(c), distinct))


def count(c: Column | str) -> Column:
    expr = to_column_expr(c)
    if isinstance(expr, E.Literal):
        expr = E.Star()  # count(lit(1)) counts rows
    return Column(E.AggregateCall("count", expr))


def countDistinct(c: Column | str, *cols: Column | str) -> Column:  # noqa: N802
    if cols:
        raise NotImplementedError("countDistinct over several columns is not in the prototype")
    return _agg("count", c, distinct=True)


count_distinct = countDistinct


def sum(c: Column | str) -> Column:  # noqa: A001 - PySpark name
    return _agg("sum", c)


def avg(c: Column | str) -> Column:
    return _agg("avg", c)


mean = avg


def min(c: Column | str) -> Column:  # noqa: A001
    return _agg("min", c)


def max(c: Column | str) -> Column:  # noqa: A001
    return _agg("max", c)


def asc(name: str) -> Column:
    return col(name).asc()


def desc(name: str) -> Column:
    return col(name).desc()


def asc_nulls_first(name: str) -> Column:
    return col(name).asc_nulls_first()


def asc_nulls_last(name: str) -> Column:
    return col(name).asc_nulls_last()


def desc_nulls_first(name: str) -> Column:
    return col(name).desc_nulls_first()


def desc_nulls_last(name: str) -> Column:
    return col(name).desc_nulls_last()


# --------------------------------------------------------------------------- scalar functions (0.2)
# Argument conventions follow PySpark: a ``str`` is a column name where PySpark takes "ColumnOrName",
# and a constant where PySpark takes ``int``/``str`` (a position, a pad string, a separator).


def _col(c: Column | str) -> E.Expr:
    expr = to_column_expr(c)
    if isinstance(expr, E.Star):
        raise TypeError("'*' is not a column here")
    return expr


def _col_or_value(c: Any) -> E.Expr:
    """ColumnOrName, or a number (pow, pmod, log base)."""
    if isinstance(c, (Column, str)):
        return _col(c)
    if isinstance(c, (int, float)) and not isinstance(c, bool):
        return literal(c)
    raise TypeError(f"expected a column, a column name or a number, got {type(c).__name__}")


def _int(value: Any, fn: str, what: str) -> E.Literal:
    if isinstance(value, Column) and isinstance(value._expr, E.Literal):
        value = value._expr.value
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{fn}() takes an integer {what}; a column {what} is not supported yet")
    return literal(value)


def _str(value: Any, fn: str, what: str) -> E.Literal:
    if isinstance(value, Column) and isinstance(value._expr, E.Literal):
        value = value._expr.value
    if not isinstance(value, str):
        raise TypeError(f"{fn}() takes a string {what}; a column {what} is not supported yet")
    return literal(value)


def _f(name: str, *args: E.Expr) -> Column:
    return Column(E.Func(name, tuple(args)))


# null handling

def coalesce(*cols: Column | str) -> Column:
    return _f("coalesce", *(_col(c) for c in cols))


def ifnull(col1: Column | str, col2: Column | str) -> Column:
    return _f("ifnull", _col(col1), _col(col2))


def nvl(col1: Column | str, col2: Column | str) -> Column:
    return _f("nvl", _col(col1), _col(col2))


def nvl2(col1: Column | str, col2: Column | str, col3: Column | str) -> Column:
    return _f("nvl2", _col(col1), _col(col2), _col(col3))


def nullif(col1: Column | str, col2: Column | str) -> Column:
    return _f("nullif", _col(col1), _col(col2))


def isnan(col: Column | str) -> Column:  # noqa: F811 - PySpark's parameter name
    return _f("isnan", _col(col))


def isnull(col: Column | str) -> Column:  # noqa: F811
    return Column(E.IsNull(_col(col)))


def isnotnull(col: Column | str) -> Column:  # noqa: F811
    return Column(E.IsNull(_col(col), negated=True))


def greatest(*cols: Column | str) -> Column:
    return _f("greatest", *(_col(c) for c in cols))


def least(*cols: Column | str) -> Column:
    return _f("least", *(_col(c) for c in cols))


# strings

def upper(col: Column | str) -> Column:  # noqa: F811
    return _f("upper", _col(col))


def lower(col: Column | str) -> Column:  # noqa: F811
    return _f("lower", _col(col))


def _trim(name: str, col, trim) -> Column:
    if trim is not None:
        raise NotImplementedError(f"{name}() with a trim string is not supported yet")
    return _f(name, _col(col))


def trim(col: Column | str, trim: Any = None) -> Column:  # noqa: F811
    return _trim("trim", col, trim)


def ltrim(col: Column | str, trim: Any = None) -> Column:  # noqa: F811
    return _trim("ltrim", col, trim)


def rtrim(col: Column | str, trim: Any = None) -> Column:  # noqa: F811
    return _trim("rtrim", col, trim)


def initcap(col: Column | str) -> Column:  # noqa: F811
    return _f("initcap", _col(col))


def reverse(col: Column | str) -> Column:  # noqa: F811
    return _f("reverse", _col(col))


def length(col: Column | str) -> Column:  # noqa: F811
    return _f("length", _col(col))


def char_length(str: Column | str) -> Column:  # noqa: A002 - PySpark's parameter name
    return _f("char_length", _col(str))


def character_length(str: Column | str) -> Column:  # noqa: A002
    return _f("character_length", _col(str))


def substring(str: Column | str, pos: int, len: int) -> Column:  # noqa: A002
    return _f("substring", _col(str), _int(pos, "substring", "position"), _int(len, "substring", "length"))


def substr(str: Column | str, pos: Any, len: Any = None) -> Column:  # noqa: A002
    length = _int(2147483647 if len is None else len, "substr", "length")
    return _f("substr", _col(str), _int(pos, "substr", "position"), length)


def concat(*cols: Column | str) -> Column:
    return _f("concat", *(_col(c) for c in cols))


def concat_ws(sep: str, *cols: Column | str) -> Column:
    return _f("concat_ws", _str(sep, "concat_ws", "separator"), *(_col(c) for c in cols))


def lpad(col: Column | str, len: int, pad: str) -> Column:  # noqa: A002
    return _f("lpad", _col(col), _int(len, "lpad", "length"), _str(pad, "lpad", "pad"))


def rpad(col: Column | str, len: int, pad: str) -> Column:  # noqa: A002
    return _f("rpad", _col(col), _int(len, "rpad", "length"), _str(pad, "rpad", "pad"))


def instr(str: Column | str, substr: str) -> Column:  # noqa: A002
    sub = substr._expr if isinstance(substr, Column) else _str(substr, "instr", "substring")
    return _f("instr", _col(str), sub)


def locate(substr: str, str: Column | str, pos: int = 1) -> Column:  # noqa: A002
    return _f("locate", _str(substr, "locate", "substring"), _col(str), _int(pos, "locate", "position"))


def replace(src: Column | str, search: Column | str, replace: Column | str | None = None) -> Column:  # noqa: F811
    repl = literal("") if replace is None else _col(replace)
    return _f("replace", _col(src), _col(search), repl)


def repeat(col: Column | str, n: int) -> Column:  # noqa: F811
    return _f("repeat", _col(col), _int(n, "repeat", "count"))


# math

def abs(col: Column | str) -> Column:  # noqa: A001
    return _f("abs", _col(col))


def round(col: Column | str, scale: int | None = None) -> Column:  # noqa: A001
    return _f("round", _col(col), _int(0 if scale is None else scale, "round", "scale"))


def bround(col: Column | str, scale: int | None = None) -> Column:
    return _f("bround", _col(col), _int(0 if scale is None else scale, "bround", "scale"))


def _no_scale(name: str, col, scale) -> Column:
    if scale is not None:
        raise NotImplementedError(f"{name}() with a scale is not supported yet")
    return _f(name, _col(col))


def ceil(col: Column | str, scale: Any = None) -> Column:
    return _no_scale("ceil", col, scale)


def ceiling(col: Column | str, scale: Any = None) -> Column:
    return _no_scale("ceiling", col, scale)


def floor(col: Column | str, scale: Any = None) -> Column:
    return _no_scale("floor", col, scale)


def pow(col1: Any, col2: Any) -> Column:  # noqa: A001
    return _f("pow", _col_or_value(col1), _col_or_value(col2))


power = pow


def sqrt(col: Column | str) -> Column:
    return _f("sqrt", _col(col))


def exp(col: Column | str) -> Column:
    return _f("exp", _col(col))


def ln(col: Column | str) -> Column:
    return _f("ln", _col(col))


def log(arg1: Any, arg2: Any = None) -> Column:
    if arg2 is None:
        return _f("ln", _col(arg1))
    return _f("log", _col_or_value(arg1), _col(arg2))


def log10(col: Column | str) -> Column:
    return _f("log10", _col(col))


def log2(col: Column | str) -> Column:
    return _f("log2", _col(col))


def log1p(col: Column | str) -> Column:
    return _f("log1p", _col(col))


def signum(col: Column | str) -> Column:
    return _f("signum", _col(col))


def sign(col: Column | str) -> Column:
    return _f("sign", _col(col))


def pmod(dividend: Any, divisor: Any) -> Column:
    return _f("pmod", _col_or_value(dividend), _col_or_value(divisor))


__all__ += [
    "coalesce", "ifnull", "nvl", "nvl2", "nullif", "isnan", "isnull", "isnotnull", "greatest", "least",
    "upper", "lower", "trim", "ltrim", "rtrim", "initcap", "reverse", "length", "char_length",
    "character_length", "substring", "substr", "concat", "concat_ws", "lpad", "rpad", "instr", "locate",
    "replace", "repeat",
    "abs", "round", "bround", "ceil", "ceiling", "floor", "pow", "power", "sqrt", "exp", "ln", "log",
    "log10", "log2", "log1p", "signum", "sign", "pmod",
]


# --------------------------------------------------------------------------- window functions (0.2)
# Each needs .over(window). Ranking functions and lag/lead need an ordered window.


def _w(name: str, *args: E.Expr) -> Column:
    return Column(E.WindowFunc(name, tuple(args)))


def row_number() -> Column:
    return _w("row_number")


def rank() -> Column:
    return _w("rank")


def dense_rank() -> Column:
    return _w("dense_rank")


def percent_rank() -> Column:
    return _w("percent_rank")


def cume_dist() -> Column:
    return _w("cume_dist")


def ntile(n: int) -> Column:
    if not isinstance(n, int) or isinstance(n, bool) or n <= 0:
        raise ValueError("ntile() takes a positive integer")
    return _w("ntile", literal(n))


def _offset(name: str, col: Column | str, offset: int, default: Any) -> Column:
    if not isinstance(offset, int) or isinstance(offset, bool):
        raise TypeError(f"{name}() takes an integer offset")
    if isinstance(default, Column):
        if not isinstance(default._expr, E.Literal):
            raise TypeError(f"{name}() takes a constant default; a column default is not supported yet")
        default = default._expr.value
    return _w(name, _col(col), literal(offset), literal(default))


def lag(col: Column | str, offset: int = 1, default: Any = None) -> Column:
    return _offset("lag", col, offset, default)


def lead(col: Column | str, offset: int = 1, default: Any = None) -> Column:
    return _offset("lead", col, offset, default)


def _first_last(name: str, col, ignorenulls: bool) -> Column:
    if ignorenulls:
        raise NotImplementedError(f"{name}(..., ignorenulls=True) is not supported: IRIS has no IGNORE NULLS")
    return _w(name, _col(col))


def first(col: Column | str, ignorenulls: bool = False) -> Column:
    return _first_last("first", col, ignorenulls)


def last(col: Column | str, ignorenulls: bool = False) -> Column:
    return _first_last("last", col, ignorenulls)


__all__ += ["row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead", "first", "last"]


# --------------------------------------------------------------------------- date and time functions (0.2)


def current_date() -> Column:
    return _f("current_date")


def current_timestamp() -> Column:
    return _f("current_timestamp")


def now() -> Column:
    return _f("now")


def year(col: Column | str) -> Column:
    return _f("year", _col(col))


def month(col: Column | str) -> Column:
    return _f("month", _col(col))


def dayofmonth(col: Column | str) -> Column:
    return _f("dayofmonth", _col(col))


def day(col: Column | str) -> Column:
    return _f("day", _col(col))


def dayofweek(col: Column | str) -> Column:
    return _f("dayofweek", _col(col))


def dayofyear(col: Column | str) -> Column:
    return _f("dayofyear", _col(col))


def weekofyear(col: Column | str) -> Column:
    return _f("weekofyear", _col(col))


def quarter(col: Column | str) -> Column:
    return _f("quarter", _col(col))


def hour(col: Column | str) -> Column:
    return _f("hour", _col(col))


def minute(col: Column | str) -> Column:
    return _f("minute", _col(col))


def second(col: Column | str) -> Column:
    return _f("second", _col(col))


def _days(value: Any) -> E.Expr:
    """An int, or a column (by Column; PySpark reads a str here as a column name too)."""
    if isinstance(value, (Column, str)):
        return _col(value)
    return _int(value, "date arithmetic", "count")


def date_add(start: Column | str, days: Any) -> Column:
    return _f("date_add", _col(start), _days(days))


def date_sub(start: Column | str, days: Any) -> Column:
    return _f("date_sub", _col(start), _days(days))


def add_months(start: Column | str, months: Any) -> Column:
    return _f("add_months", _col(start), _days(months))


def datediff(end: Column | str, start: Column | str) -> Column:
    return _f("datediff", _col(end), _col(start))


def months_between(date1: Column | str, date2: Column | str, roundOff: bool = True) -> Column:  # noqa: N803
    return _f("months_between", _col(date1), _col(date2), literal(bool(roundOff)))


def last_day(date: Column | str) -> Column:
    return _f("last_day", _col(date))


def trunc(date: Column | str, format: str) -> Column:  # noqa: A002
    return _f("trunc", _col(date), _str(format, "trunc", "format"))


def date_trunc(format: str, timestamp: Column | str) -> Column:  # noqa: A002
    return _f("date_trunc", _str(format, "date_trunc", "format"), _col(timestamp))


def date_format(date: Column | str, format: str) -> Column:  # noqa: A002
    return _f("date_format", _col(date), _str(format, "date_format", "format"))


def to_date(col: Column | str, format: str | None = None) -> Column:  # noqa: A002
    if format is None:
        return _f("to_date", _col(col))
    return _f("to_date", _col(col), _str(format, "to_date", "format"))


def to_timestamp(col: Column | str, format: str | None = None) -> Column:  # noqa: A002
    if format is None:
        return _f("to_timestamp", _col(col))
    return _f("to_timestamp", _col(col), _str(format, "to_timestamp", "format"))


__all__ += ["current_date", "current_timestamp", "now", "year", "month", "dayofmonth", "day", "dayofweek",
            "dayofyear", "weekofyear", "quarter", "hour", "minute", "second", "date_add", "date_sub",
            "add_months", "datediff", "months_between", "last_day", "trunc", "date_trunc", "date_format",
            "to_date", "to_timestamp"]


def expr(str: str) -> Column:  # noqa: A002 - PySpark's parameter name
    """A Spark SQL expression string, with an optional alias: ``F.expr("price * qty AS total")``."""
    from ..sqlexpr import parse_named

    return Column(parse_named(str))


__all__ += ["expr"]
