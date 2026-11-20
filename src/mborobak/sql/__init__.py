"""PySpark-style import paths: ``from mborobak.sql import functions as F``.

The top-level classes are loaded lazily, because ``mborobak.sql.types`` is
imported by the core modules themselves.
"""

from importlib import import_module

__all__ = ["MborobakSession", "DataFrame", "Column", "Row", "GroupedData", "Window", "WindowSpec", "functions",
           "types", "window"]

_LAZY = {
    "MborobakSession": ("mborobak.session", "MborobakSession"),
    "DataFrame": ("mborobak.dataframe", "DataFrame"),
    "GroupedData": ("mborobak.dataframe", "GroupedData"),
    "Column": ("mborobak.column", "Column"),
    "Row": ("mborobak.row", "Row"),
    "Window": ("mborobak.sql.window", "Window"),
    "WindowSpec": ("mborobak.sql.window", "WindowSpec"),
    "functions": ("mborobak.sql.functions", None),
    "window": ("mborobak.sql.window", None),
    "types": ("mborobak.sql.types", None),
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module 'mborobak.sql' has no attribute {name!r}")
    module_name, attr = _LAZY[name]
    module = import_module(module_name)
    return module if attr is None else getattr(module, attr)
