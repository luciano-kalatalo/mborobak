"""Window semantics shared by the analyzer, naming and the compiler (compiler spec §7.11).

Spark gives every window an effective frame: ranking functions use ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW (``cume_dist`` the RANGE form), ``lag``/``lead`` a one-row frame at their offset, and
aggregates without a frame RANGE … CURRENT ROW when ordered or the whole partition when not. The
frame shows in the default column name and decides what can be compiled: IRIS 2026.2 has ROWS frames
with constant offsets but no explicit RANGE frames (probe 2026-11-20).
"""

from __future__ import annotations

from . import expressions as E
from .errors import AnalysisError, UnsupportedOperationError

# key: PySpark's display name
RANKING = {"row_number": "row_number", "rank": "RANK", "dense_rank": "DENSE_RANK",
           "percent_rank": "PERCENT_RANK", "cume_dist": "cume_dist", "ntile": "ntile"}
OFFSET = ("lag", "lead")
VALUE = ("first", "last", "first_value", "last_value")

ROWS_TO_CURRENT = E.Frame("rows", None, 0)
RANGE_TO_CURRENT = E.Frame("range", None, 0)
WHOLE = E.Frame("rows", None, None)


def offset_of(w: E.WindowFunc) -> int:
    """Signed row offset of lag/lead: lag(x, 2) is -2, lead(x, 1) is 1."""
    n = w.args[1].value
    return -n if w.name == "lag" else n


def effective_frame(w: E.WindowExpr) -> E.Frame:
    f = w.function
    if isinstance(f, E.WindowFunc) and f.name in RANKING:
        return RANGE_TO_CURRENT if f.name == "cume_dist" else ROWS_TO_CURRENT
    if isinstance(f, E.WindowFunc) and f.name in OFFSET:
        n = offset_of(f)
        return E.Frame("rows", n, n)
    if w.frame is not None:
        return w.frame
    return RANGE_TO_CURRENT if w.order else WHOLE


def check(w: E.WindowExpr, operation: str | None) -> None:
    """Spark's analysis errors, then what IRIS can't compile."""
    f = w.function
    if any(E.contains_window(e) for e in (*f.children(), *w.partition, *w.order)):
        raise AnalysisError("window functions can't be nested; add the inner one with withColumn() first",
                            operation=operation)
    if isinstance(f, E.AggregateCall) and f.distinct:
        raise AnalysisError("distinct window functions are not supported (as in Spark): "
                            f"{f.function}(DISTINCT …).over(…)", operation=operation)
    if isinstance(f, E.WindowFunc) and f.name in (*RANKING, *OFFSET) and not w.order:
        raise AnalysisError(f"window function {f.name}() requires an ordered window: add orderBy() to the "
                            "Window", operation=operation)
    if isinstance(f, E.WindowFunc) and f.name in RANKING and w.frame is not None and w.frame != ROWS_TO_CURRENT:
        raise AnalysisError(f"{f.name}() takes no window frame other than rowsBetween(Window.unboundedPreceding, "
                            "Window.currentRow)", operation=operation)
    if isinstance(f, E.WindowFunc) and f.name in OFFSET and w.frame is not None:
        raise AnalysisError(f"cannot specify a window frame for {f.name}()", operation=operation)
    fr = w.frame
    if fr is None:
        return
    if fr.start is not None and fr.end is not None and fr.start > fr.end:
        raise AnalysisError("the lower bound of a window frame must be less than or equal to the upper bound",
                            operation=operation)
    if fr.kind == "range":
        if fr.start not in (None, 0) or fr.end not in (None, 0):
            raise UnsupportedOperationError(
                "rangeBetween() with value offsets is not supported: IRIS has no RANGE frames. Use "
                "rowsBetween() for a number of rows, or a self-join for a range of values", operation=operation)
        if fr.start == 0 and fr.end == 0:
            raise UnsupportedOperationError("rangeBetween(currentRow, currentRow) is not supported: IRIS has no "
                                            "RANGE frames", operation=operation)
    elif not w.order and fr != WHOLE:
        raise UnsupportedOperationError("rowsBetween() needs orderBy() in Mborobak: without an order the rows "
                                        "in a frame are arbitrary", operation=operation)


def _bound(value: int | None, unbounded: str, written: bool) -> str:
    if value is None:
        return f"UNBOUNDED {unbounded}"
    if value == 0:
        return "CURRENT ROW"
    if written and value < 0:
        return f"{-value} PRECEDING"  # a frame from a SQL expression keeps its words
    return f"{value} FOLLOWING"  # rowsBetween(-1, …): Spark writes every offset this way, negative ones too


def frame_name(fr: E.Frame) -> str:
    return (f"{fr.kind.upper()} BETWEEN {_bound(fr.start, 'PRECEDING', fr.written)} AND "
            f"{_bound(fr.end, 'FOLLOWING', fr.written)}")


def order_name(o: E.SortOrder, expr_name) -> str:
    return (f"{expr_name(o.child)} {'ASC' if o.ascending else 'DESC'} "
            f"NULLS {'FIRST' if o.effective_nulls_first else 'LAST'}")


def name(w: E.WindowExpr, expr_name) -> str:
    parts = []
    if w.partition:
        parts.append("PARTITION BY " + ", ".join(expr_name(p) for p in w.partition))
    if w.order:
        parts.append("ORDER BY " + ", ".join(order_name(o, expr_name) for o in w.order))
    f = w.function
    if isinstance(f, E.WindowFunc) and f.name in OFFSET:  # Spark writes even 0 as "0 FOLLOWING" here
        n = offset_of(f)
        parts.append(f"ROWS BETWEEN {n} FOLLOWING AND {n} FOLLOWING")
    else:
        parts.append(frame_name(effective_frame(w)))
    return f"{expr_name(w.function)} OVER ({' '.join(parts)})"


def function_name(f: E.WindowFunc, expr_name) -> str:
    if f.name in RANKING:
        args = ", ".join(expr_name(a) for a in f.args)
        return f"{RANKING[f.name]}({args})"
    return f"{f.name}({', '.join(expr_name(a) for a in f.args)})"
