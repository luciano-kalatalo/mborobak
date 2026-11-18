"""``Row``: a tuple with named fields (API contract §5.1)."""

from __future__ import annotations

from typing import Any

__all__ = ["Row"]


class Row(tuple):
    """``Row(a=1, b=2)``, ``Row("a", "b")(1, 2)``, or built by Mborobak from results."""

    __fields__: tuple[str, ...]

    def __new__(cls, *args: Any, **kwargs: Any) -> "Row":
        if args and kwargs:
            raise ValueError("Row takes either positional or keyword arguments, not both")
        if kwargs:
            row = tuple.__new__(cls, kwargs.values())
            row.__fields__ = tuple(kwargs)
            return row
        # Row("a", "b") makes a row "class" that can be called with values.
        row = tuple.__new__(cls, args)
        row.__fields__ = ()
        return row

    @classmethod
    def _make(cls, names: tuple[str, ...], values: tuple[Any, ...]) -> "Row":
        row = tuple.__new__(cls, values)
        row.__fields__ = names
        return row

    def __call__(self, *values: Any) -> "Row":
        if self.__fields__:
            raise TypeError("only a Row of field names can be called")
        if len(values) != len(self):
            raise ValueError(f"expected {len(self)} values, got {len(values)}")
        return Row._make(tuple(self), values)

    def __getitem__(self, item: Any) -> Any:  # type: ignore[override]
        if isinstance(item, str):
            try:
                return tuple.__getitem__(self, self.__fields__.index(item))
            except ValueError:
                raise KeyError(item) from None
        return tuple.__getitem__(self, item)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        try:
            return tuple.__getitem__(self, self.__fields__.index(name))
        except ValueError:
            raise AttributeError(name) from None

    def __contains__(self, item: object) -> bool:
        return item in self.__fields__ if self.__fields__ else tuple.__contains__(self, item)

    def asDict(self) -> dict[str, Any]:  # noqa: N802 - PySpark name
        return dict(zip(self.__fields__, self))

    def __repr__(self) -> str:
        if self.__fields__:
            return "Row(" + ", ".join(f"{k}={v!r}" for k, v in zip(self.__fields__, self)) + ")"
        return "<Row(" + ", ".join(repr(v) for v in self) + ")>"

    def __reduce__(self):  # pickling keeps the field names
        return (Row._make, (self.__fields__, tuple(self)))
