"""PySpark-style import paths: ``from mborobak.sql import functions as F``.

The top-level classes are loaded lazily, because ``mborobak.sql.types`` is
imported by the core modules themselves.
"""

from importlib import import_module

__all__ = ["MborobakSession", "DataFrame", "Column", "Row", "GroupedData", "functions", "types"]

_LAZY = {
    "MborobakSession": ("mborobak.session", "MborobakSession"),
    "DataFrame": ("mborobak.dataframe", "DataFrame"),
    "GroupedData": ("mborobak.dataframe", "GroupedData"),
    "Column": ("mborobak.column", "Column"),
    "Row": ("mborobak.row", "Row"),
    "functions": ("mborobak.sql.functions", None),
    "types": ("mborobak.sql.types", None),
}


def __getattr__(name: str):
    if name not in _LAZY:
        raise AttributeError(f"module 'mborobak.sql' has no attribute {name!r}")
    module_name, attr = _LAZY[name]
    module = import_module(module_name)
    return module if attr is None else getattr(module, attr)
