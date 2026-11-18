"""Error classes (API contract §9)."""

from __future__ import annotations

import os
import sys
from typing import Any

__all__ = [
    "MborobakError",
    "MborobakConnectionError",
    "SessionClosedError",
    "AnalysisError",
    "TableNotFoundError",
    "ColumnNotFoundError",
    "AmbiguousColumnError",
    "UnsupportedTypeError",
    "UnsupportedOperationError",
    "PlanTooLargeError",
    "MborobakExecutionError",
    "MborobakArithmeticError",
    "MborobakCastError",
]

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def caller_location() -> str | None:
    """``file.py:42`` of the first stack frame outside the mborobak package."""
    frame = sys._getframe(1)
    while frame is not None:
        path = os.path.abspath(frame.f_code.co_filename)
        if not path.startswith(_PACKAGE_DIR):
            return f"{os.path.basename(path)}:{frame.f_lineno}"
        frame = frame.f_back
    return None


class MborobakError(Exception):
    """Base class. ``operation`` names the user call that caused the error."""

    def __init__(self, message: str, *, operation: str | None = None, **details: Any):
        self.message = message
        self.operation = operation
        for key, value in details.items():
            setattr(self, key, value)
        super().__init__(message)

    def __str__(self) -> str:
        text = self.message
        if self.operation:
            text += f"\n  in: {self.operation}"
        return text


class MborobakConnectionError(MborobakError):
    pass


class SessionClosedError(MborobakError):
    pass


class AnalysisError(MborobakError):
    pass


class TableNotFoundError(AnalysisError):
    pass


class ColumnNotFoundError(AnalysisError):
    pass


class AmbiguousColumnError(AnalysisError):
    pass


class UnsupportedTypeError(AnalysisError):
    pass


class UnsupportedOperationError(MborobakError, NotImplementedError):
    pass


class PlanTooLargeError(MborobakError):
    pass


class MborobakExecutionError(MborobakError):
    """Raised by actions. Keeps ``sqlcode``, ``iris_message`` and ``sql``."""

    def __str__(self) -> str:
        text = super().__str__()
        code = getattr(self, "iris_code", None)
        sqlcode = getattr(self, "sqlcode", None)
        if sqlcode is not None:
            text += f"\n  IRIS: SQLCODE {sqlcode}" + (f" {code}" if code else "")
        return text


class MborobakArithmeticError(MborobakExecutionError):
    pass


class MborobakCastError(MborobakExecutionError):
    pass
