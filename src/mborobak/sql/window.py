"""``Window`` and ``WindowSpec`` (API contract §6.6), as in ``pyspark.sql.window``."""

from __future__ import annotations

from typing import Any

from .. import expressions as E

__all__ = ["Window", "WindowSpec"]


def _cols(cols: tuple) -> list:
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        return list(cols[0])
    return list(cols)


def _order(c: Any) -> E.SortOrder:
    from ..column import to_column_expr

    expr = to_column_expr(c)
    if isinstance(expr, E.Star):
        raise TypeError("'*' is not a sort key")
    return expr if isinstance(expr, E.SortOrder) else E.SortOrder(expr, True, None)


def _bound(value: int, side: str) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"window frame {side} must be an integer")
    if value <= E.PRECEDING_LIMIT or value >= E.FOLLOWING_LIMIT:
        return None
    return value


class WindowSpec:
    """Partitioning, ordering and frame of a window. Immutable: each method returns a new spec."""

    __slots__ = ("_partition", "_order", "_frame")

    def __init__(self, partition: tuple = (), order: tuple = (), frame: E.Frame | None = None):
        self._partition = partition
        self._order = order
        self._frame = frame

    def partitionBy(self, *cols: Any) -> "WindowSpec":  # noqa: N802
        from ..column import to_column_expr

        exprs = tuple(to_column_expr(c) for c in _cols(cols))
        return WindowSpec(exprs, self._order, self._frame)

    def orderBy(self, *cols: Any) -> "WindowSpec":  # noqa: N802
        return WindowSpec(self._partition, tuple(_order(c) for c in _cols(cols)), self._frame)

    def _between(self, kind: str, start: int, end: int) -> "WindowSpec":
        frame = E.Frame(kind, _bound(start, "start"), _bound(end, "end"))
        if frame.start is None and start > 0:
            raise ValueError("the window frame start can't be unboundedFollowing")
        if frame.end is None and end < 0:
            raise ValueError("the window frame end can't be unboundedPreceding")
        return WindowSpec(self._partition, self._order, frame)

    def rowsBetween(self, start: int, end: int) -> "WindowSpec":  # noqa: N802
        return self._between("rows", start, end)

    def rangeBetween(self, start: int, end: int) -> "WindowSpec":  # noqa: N802
        return self._between("range", start, end)

    def __repr__(self) -> str:
        return f"WindowSpec(partition={len(self._partition)}, order={len(self._order)}, frame={self._frame})"


class Window:
    """Entry point: ``Window.partitionBy("g").orderBy("t").rowsBetween(-1, 0)``."""

    unboundedPreceding = E.UNBOUNDED_PRECEDING
    unboundedFollowing = E.UNBOUNDED_FOLLOWING
    currentRow = 0

    @staticmethod
    def partitionBy(*cols: Any) -> WindowSpec:  # noqa: N802
        return WindowSpec().partitionBy(*cols)

    @staticmethod
    def orderBy(*cols: Any) -> WindowSpec:  # noqa: N802
        return WindowSpec().orderBy(*cols)

    @staticmethod
    def rowsBetween(start: int, end: int) -> WindowSpec:  # noqa: N802
        return WindowSpec().rowsBetween(start, end)

    @staticmethod
    def rangeBetween(start: int, end: int) -> WindowSpec:  # noqa: N802
        return WindowSpec().rangeBetween(start, end)
