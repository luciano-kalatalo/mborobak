"""Notebook compatibility scanner (POC readiness plan, tier 0).

``mborobak-scan notebook.ipynb`` lists every PySpark call a notebook or script makes and says, for each,
whether Mborobak supports it today, plans it (with the release), has not scheduled it yet, or keeps it
out of scope. It reads the code with Python's ``ast`` module and never runs it.

Classification is by name, not by type: the scanner cannot know whether ``x.count()`` is called on a
DataFrame or on a list. It counts attribute calls whose names belong to the PySpark API, and skips
calls on imported non-Spark modules (``os.path.join``), on string literals (``", ".join``) and on
variables assigned from pandas or ``toPandas()``. Treat the result as an estimate to start a POC from.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["Finding", "Report", "scan_source", "scan_notebook", "scan_paths", "main"]

SUPPORTED = "supported"
PLANNED = "planned"
NOT_SCHEDULED = "not scheduled"
OUT_OF_SCOPE = "out of scope"
_STATUS_ORDER = (SUPPORTED, PLANNED, NOT_SCHEDULED, OUT_OF_SCOPE)
_RELEASES = ("0.2", "0.2.x", "0.3")

# Kinds of finding.
FUNCTION = "function"          # pyspark.sql.functions.x(...)
METHOD = "method"              # DataFrame, Column or GroupedData method
WINDOW = "window"              # Window.partitionBy(...), Window.unboundedPreceding
SESSION = "session"            # spark.table(...), spark.createDataFrame(...)
READER = "reader"              # spark.read...
WRITER = "writer"              # df.write...
PLATFORM = "platform"          # display(), dbutils, %sql cells, MLlib imports

# ---------------------------------------------------------------- catalog
# Planned and out-of-scope entries follow docs/poc-readiness-plan.md. "Supported" is read from the
# installed Mborobak (see _supported), so it can never claim more than the code provides.

_PLANNED_FUNCTIONS = {
    "0.2": """coalesce nullif isnan isnull isnotnull greatest least ifnull nvl nvl2
        upper lower trim ltrim rtrim length char_length substring substr concat concat_ws lpad rpad
        instr locate replace initcap reverse
        round bround abs ceil ceiling floor pow power sqrt exp log log10 log2 ln mod pmod signum
        current_date current_timestamp now year month dayofmonth day dayofweek dayofyear weekofyear
        quarter hour minute second date_add date_sub datediff add_months months_between last_day
        to_date to_timestamp date_trunc trunc date_format
        row_number rank dense_rank percent_rank cume_dist ntile lag lead first last first_value
        last_value expr""",
    "0.3": "regexp_extract_all split",
}
_OUT_OF_SCOPE_FUNCTIONS = {
    "udf": "Python UDFs run in Spark workers; server-side UDFs are 0.3 research",
    "pandas_udf": "Python UDFs run in Spark workers; server-side UDFs are 0.3 research",
    "broadcast": "a Spark join hint; IRIS plans joins itself",
    "spark_partition_id": "Spark execution detail",
    "monotonically_increasing_id": "Spark execution detail",
    "input_file_name": "reads files",
}
_PLANNED_METHODS = {
    "0.2": "selectExpr dropDuplicates drop_duplicates over fillna dropna",
}
_OUT_OF_SCOPE_METHODS = {
    "cache": "IRIS keeps its own caches; drop the call",
    "persist": "IRIS keeps its own caches; drop the call",
    "unpersist": "IRIS keeps its own caches; drop the call",
    "checkpoint": "Spark execution detail",
    "localCheckpoint": "Spark execution detail",
    "repartition": "Spark partitioning; IRIS parallelizes queries itself",
    "coalesce": "Spark partitioning (DataFrame.coalesce); for the null function use F.coalesce",
    "sortWithinPartitions": "Spark partitioning",
    "foreach": "runs Python on Spark workers",
    "foreachPartition": "runs Python on Spark workers",
    "mapInPandas": "runs Python on Spark workers",
    "applyInPandas": "runs Python on Spark workers",
    "withWatermark": "streaming",
    "pandas_api": "pandas API on Spark; use toPandas() on the result",
}
# Spark API method names the scanner recognizes. Unknown attribute names are ignored, so that
# ordinary Python calls (str.split, dict.get) do not count. "groupby" and "replace" are left out on
# purpose: they are far more often pandas or str calls in notebooks.
_SPARK_METHODS = set("""
    select selectExpr filter where withColumn withColumns withColumnRenamed withColumnsRenamed drop
    dropDuplicates drop_duplicates dropna fillna orderBy sort sortWithinPartitions limit distinct
    groupBy agg alias join crossJoin union unionAll unionByName intersect intersectAll subtract
    exceptAll collect show count take first head tail toPandas cache persist unpersist checkpoint
    localCheckpoint repartition coalesce explain printSchema describe summary sample randomSplit toDF
    transform createOrReplaceTempView createTempView createGlobalTempView createOrReplaceGlobalTempView
    foreach foreachPartition mapInPandas applyInPandas pivot rollup cube withWatermark isEmpty toJSON
    hint observe melt unpivot offset toSQL pandas_api
    cast astype isNull isNotNull isin like ilike rlike between contains startswith endswith substr
    when otherwise over asc desc asc_nulls_first asc_nulls_last desc_nulls_first desc_nulls_last
    eqNullSafe getItem getField bitwiseAND bitwiseOR bitwiseXOR
    sum avg mean min max
""".split())
_NA_METHODS = {"na.fill", "na.drop", "na.replace"}

_PLANNED_SESSION = {"createDataFrame": "0.2.x"}
_OUT_OF_SCOPE_SESSION = {
    "range": "generates data in Spark; create a table in IRIS instead",
    "sparkContext": "Spark execution detail",
    "udf": "Python UDFs run in Spark workers",
    "readStream": "streaming",
}
_SESSION_METHODS = {"table", "sql", "createDataFrame", "range", "stop"}

_WINDOW_PLANNED = "0.2"
_WRITER_FILES = {"save", "parquet", "csv", "json", "orc", "text", "jdbc", "xml"}
_JOIN_LATER = {"semi", "leftsemi", "left_semi", "anti", "leftanti", "left_anti"}

_PLATFORM = {
    "display": (OUT_OF_SCOPE, None, "Databricks notebook display; use show() or toPandas()"),
    "displayHTML": (OUT_OF_SCOPE, None, "Databricks notebook display"),
    "dbutils": (OUT_OF_SCOPE, None, "Databricks utilities"),
    "%sql cell": (PLANNED, "0.3", "Spark SQL dialect; IRIS SQL works today through session.sql()"),
    "pyspark.ml": (OUT_OF_SCOPE, None, "MLlib"),
    "pyspark.mllib": (OUT_OF_SCOPE, None, "MLlib"),
    "pyspark.streaming": (OUT_OF_SCOPE, None, "streaming"),
    "pyspark.pandas": (OUT_OF_SCOPE, None, "pandas API on Spark"),
    "dlt": (OUT_OF_SCOPE, None, "Lakeflow declarative pipelines (Delta Live Tables)"),
}

_TO_PANDAS = ("toPandas", "pandas_api", "to_pandas_on_spark", "to_koalas")
# irispark: Mborobak's predecessor, whose notebooks use PySpark's API under its own module names
_SPARK_MODULES = ("pyspark", "mborobak", "delta", "databricks", "irispark")
_FUNCTION_MODULES = ("pyspark.sql.functions", "mborobak.sql.functions", "irispark.functions")
_PANDAS_MODULES = ("pandas", "numpy", "polars")


def _words(table: dict[str, str]) -> dict[str, str]:
    return {name: release for release, names in table.items() for name in names.split()}


_PLANNED_FUNCTION_RELEASE = _words(_PLANNED_FUNCTIONS)
_PLANNED_METHOD_RELEASE = _words(_PLANNED_METHODS)


def _dialects() -> set[str]:
    try:
        from . import sparksql  # noqa: F401

        return {"sql(spark)"}
    except ImportError:
        return set()


def _supported() -> dict[str, set[str]]:
    """What the installed Mborobak provides, by kind."""
    from . import Column, DataFrame, GroupedData, MborobakSession
    from .session import _CatalogAPI
    from .sql import functions as F
    from .sql.window import Window, WindowSpec
    from .dataframe import _JOIN_TYPES
    from .writer import DataFrameWriter

    def public(cls) -> set[str]:
        return {n for n in dir(cls) if not n.startswith("_")}

    functions = {n for n, f in vars(F).items()
                 if callable(f) and not n.startswith("_") and getattr(f, "__module__", "") == F.__name__}
    return {
        FUNCTION: functions,
        METHOD: public(DataFrame) | public(Column) | public(GroupedData) | {f"join(how={k!r})" for k in _JOIN_TYPES},
        SESSION: public(MborobakSession) | {"catalog." + n for n in public(_CatalogAPI)} | _dialects(),
        WINDOW: public(Window) | public(WindowSpec),
        WRITER: public(DataFrameWriter),
    }


# ---------------------------------------------------------------- findings
@dataclass(frozen=True)
class Location:
    file: str
    line: int
    cell: int | None = None   # 1-based cell number in a notebook, counting all cells

    def __str__(self) -> str:
        where = f"cell {self.cell}, line {self.line}" if self.cell is not None else f"line {self.line}"
        return f"{self.file}: {where}"


@dataclass
class Finding:
    kind: str
    name: str
    status: str
    release: str | None
    note: str | None
    locations: list[Location] = field(default_factory=list)

    @property
    def sorted_locations(self) -> list[Location]:
        return sorted(self.locations, key=lambda loc: (loc.file, loc.cell or 0, loc.line))

    @property
    def count(self) -> int:
        return len(self.locations)

    @property
    def label(self) -> str:
        if self.kind == FUNCTION:
            return f"F.{self.name}"
        if self.kind == WINDOW:
            return f"Window.{self.name}"
        if self.kind in (SESSION, READER):
            return f"spark.{self.name}"
        if self.kind == WRITER:
            return f"df.{self.name}"
        if self.kind == METHOD:
            return f".{self.name}()" if not self.name.endswith(")") else f".{self.name}"
        return self.name

    @property
    def status_text(self) -> str:
        return f"{self.status} ({self.release})" if self.status == PLANNED and self.release else self.status


@dataclass
class Report:
    findings: dict[tuple[str, str], Finding] = field(default_factory=dict)
    files: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add(self, kind: str, name: str, loc: Location, supported: dict[str, set[str]]) -> None:
        key = (kind, name)
        if key not in self.findings:
            status, release, note = classify(kind, name, supported)
            self.findings[key] = Finding(kind, name, status, release, note)
        self.findings[key].locations.append(loc)

    def merge(self, other: "Report") -> None:
        for key, f in other.findings.items():
            if key in self.findings:
                self.findings[key].locations.extend(f.locations)
            else:
                self.findings[key] = f
        self.files += other.files
        self.warnings += other.warnings

    def totals(self) -> dict[str, int]:
        out = dict.fromkeys(_STATUS_ORDER, 0)
        for f in self.findings.values():
            out[f.status] += f.count
        return out

    def coverage(self, in_scope: bool = False) -> dict[str, float]:
        """Share of calls supported today and after each planned release (cumulative).

        With ``in_scope``, out-of-scope calls (display(), spark.read, ...) are left out: a POC rewrites
        them anyway, so this is the share of the remaining code that runs.
        """
        findings = [f for f in self.findings.values() if not (in_scope and f.status == OUT_OF_SCOPE)]
        calls = sum(f.count for f in findings)
        if not calls:
            return {}
        done = sum(f.count for f in findings if f.status == SUPPORTED)
        out = {"today": done / calls}
        for release in _RELEASES:
            done += sum(f.count for f in findings if f.status == PLANNED and f.release == release)
            out[f"after {release}"] = done / calls
        return out

    def sorted_findings(self) -> list[Finding]:
        return sorted(self.findings.values(),
                      key=lambda f: (_STATUS_ORDER.index(f.status), _RELEASES.index(f.release)
                                     if f.release in _RELEASES else 9, -f.count, f.label.lower()))


def classify(kind: str, name: str, supported: dict[str, set[str]]) -> tuple[str, str | None, str | None]:
    """(status, release, note) for one PySpark API name."""
    if kind == FUNCTION:
        if name in supported[FUNCTION]:
            return SUPPORTED, None, None
        if name in _PLANNED_FUNCTION_RELEASE:
            note = "returns an array, which Mborobak has no type for" if _PLANNED_FUNCTION_RELEASE[name] == "0.3" else None
            return PLANNED, _PLANNED_FUNCTION_RELEASE[name], note
        if name in _OUT_OF_SCOPE_FUNCTIONS:
            return OUT_OF_SCOPE, None, _OUT_OF_SCOPE_FUNCTIONS[name]
        return NOT_SCHEDULED, None, None
    if kind == METHOD:
        if name == "filter(str)":
            if "selectExpr" in supported[METHOD]:
                return SUPPORTED, None, "Spark SQL expression"
            return PLANNED, "0.2", "SQL-string condition; use F.col(...) conditions today"
        if name.startswith("join(how="):
            if name in supported[METHOD]:
                return SUPPORTED, None, None
            return PLANNED, "0.2.x", "semi and anti joins"
        if name in ("na.fill", "na.drop"):
            if {"fillna", "dropna"} <= supported[METHOD]:
                return SUPPORTED, None, None
            return PLANNED, "0.2", None
        if name in _NA_METHODS:
            return NOT_SCHEDULED, None, None
        if name in supported[METHOD]:
            return SUPPORTED, None, None
        if name in _PLANNED_METHOD_RELEASE:
            return PLANNED, _PLANNED_METHOD_RELEASE[name], "window functions" if name == "over" else None
        if name in _OUT_OF_SCOPE_METHODS:
            return OUT_OF_SCOPE, None, _OUT_OF_SCOPE_METHODS[name]
        return NOT_SCHEDULED, None, None
    if kind == SESSION:
        if name in supported[SESSION]:
            note = "Spark SQL SELECT statements (IRIS SQL with mborobak.sql.dialect = 'iris')" if name == "sql" else None
            return SUPPORTED, None, note
        if name in _PLANNED_SESSION:
            return PLANNED, _PLANNED_SESSION[name], None
        if name in _OUT_OF_SCOPE_SESSION:
            return OUT_OF_SCOPE, None, _OUT_OF_SCOPE_SESSION[name]
        return NOT_SCHEDULED, None, None
    if kind == WINDOW:
        if name in supported.get(WINDOW, set()):
            return SUPPORTED, None, None
        return PLANNED, _WINDOW_PLANNED, "window functions"
    if kind == READER:
        if name == "read.table":
            if "read" in supported[SESSION]:
                return SUPPORTED, None, None
            return NOT_SCHEDULED, None, "same as spark.table(), which is supported"
        return OUT_OF_SCOPE, None, "reads files or external sources; the POC data is in IRIS"
    if kind == WRITER:
        method = name.split(".", 1)[1] if "." in name else name
        if name.startswith("writeStream"):
            return OUT_OF_SCOPE, None, "streaming"
        if method in _WRITER_FILES:
            return OUT_OF_SCOPE, None, "writes files or external sources; saveAsTable writes IRIS tables"
        if method in supported.get(WRITER, set()):
            return SUPPORTED, None, None
        return NOT_SCHEDULED, None, None
    if kind == PLATFORM:
        if name == "%sql cell" and "sql(spark)" in supported[SESSION]:
            return SUPPORTED, None, "as spark.sql(\"…\") (SELECT statements)"
        return _PLATFORM.get(name, (OUT_OF_SCOPE, None, None))
    raise ValueError(kind)


# ---------------------------------------------------------------- the AST walk
class _Visitor(ast.NodeVisitor):
    def __init__(self, report: Report, file: str, cell: int | None, line_offset: int,
                 state: "_State", supported: dict[str, set[str]]):
        self.report, self.file, self.cell, self.offset = report, file, cell, line_offset
        self.state, self.supported = state, supported
        self._seen_attrs: set[int] = set()
        self._seen_calls: set[int] = set()   # inner calls of a reader or writer chain, counted once

    def add(self, kind: str, name: str, node: ast.AST) -> None:
        # for a.b().c(), report the line of .c, not the line where the chain starts
        line = node.func.end_lineno if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
            else getattr(node, "lineno", 1)
        loc = Location(self.file, (line or 1) + self.offset, self.cell)
        self.report.add(kind, name, loc, self.supported)

    # imports
    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            bound = a.asname or a.name.split(".")[0]
            if a.name in _FUNCTION_MODULES and a.asname:
                self.state.function_modules.add(a.asname)
            elif a.name in _FUNCTION_MODULES:
                self.state.function_paths.add(a.name)
            elif a.name.startswith("pyspark.pandas"):
                self.state.foreign.add(bound)
                self.state.pandas.add(bound)
            elif not a.name.startswith(_SPARK_MODULES):
                self.state.foreign.add(bound)
                if a.name.startswith(_PANDAS_MODULES):
                    self.state.pandas.add(bound)
            self._platform_import(a.name, node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        if not self._platform_import(module, node):
            for a in node.names:                    # from pyspark import pipelines
                if self._platform_import(f"{module}.{a.name}", node):
                    break
        for a in node.names:
            bound = a.asname or a.name
            full = f"{module}.{a.name}"
            if full in _FUNCTION_MODULES:
                self.state.function_modules.add(bound)
            elif module in _FUNCTION_MODULES:
                if a.name == "*":
                    self.state.star_functions = True
                else:
                    self.state.function_names[bound] = a.name
            elif a.name == "Window" and module.startswith(_SPARK_MODULES):
                self.state.window_names.add(bound)
            elif module.startswith("pyspark.pandas") or full.startswith("pyspark.pandas"):
                self.state.foreign.add(bound)
                self.state.pandas.add(bound)
            elif not module.startswith(_SPARK_MODULES) and module:
                self.state.foreign.add(bound)
                if module.startswith(_PANDAS_MODULES):
                    self.state.pandas.add(bound)

    def _platform_import(self, module: str, node: ast.AST) -> bool:
        for prefix in ("pyspark.mllib", "pyspark.ml", "pyspark.streaming", "pyspark.pandas", "dlt",
                       "pyspark.pipelines", "irispark.ml"):
            if module == prefix or module.startswith(prefix + "."):
                name = {"pyspark.pipelines": "dlt", "irispark.ml": "pyspark.ml"}.get(prefix, prefix)
                self.add(PLATFORM, name, node)
                return True
        return False

    # variables holding pandas data are not Spark DataFrames
    def visit_Assign(self, node: ast.Assign) -> None:
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        chain, root = _chain(node.value)
        attrs = {n.attr for n in chain if isinstance(n, ast.Attribute)}
        if self._is_pandas(node.value):
            self.state.pandas.update(names)
        elif isinstance(root, ast.Name) and root.id in self.state.window_names:
            self.state.window_names.update(names)       # w = Window.partitionBy(...)
        elif "builder" in attrs or "getActiveSession" in attrs:
            self.state.session_names.update(names)      # spark = SparkSession.builder...getOrCreate()
        self.generic_visit(node)

    def _is_pandas(self, value: ast.AST) -> bool:
        chain, root = _chain(value)
        if isinstance(root, ast.Name) and root.id in self.state.pandas:
            return True
        return any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in _TO_PANDAS
                   for n in chain)

    # decorators such as @udf or @F.udf are names, not calls
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        for d in node.decorator_list:
            target = d.func if isinstance(d, ast.Call) else d
            name = self._function_name(target)
            if name:
                self.add(FUNCTION, name, d)
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def _function_name(self, func: ast.AST) -> str | None:
        """The PySpark function a call target names, if any."""
        if isinstance(func, ast.Attribute):
            if isinstance(func.value, ast.Name) and func.value.id in self.state.function_modules:
                return func.attr
            dotted = _dotted(func.value)
            if dotted and dotted in self.state.function_paths:
                return func.attr
        if isinstance(func, ast.Name):
            if func.id in self.state.function_names:
                return self.state.function_names[func.id]
            if self.state.star_functions and (func.id in _KNOWN_FUNCTIONS or func.id in self.supported[FUNCTION]):
                return func.id
        return None

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # Window.unboundedPreceding and other non-call uses of Window
        if (id(node) not in self._seen_attrs and isinstance(node.value, ast.Name)
                and node.value.id in self.state.window_names):
            self.add(WINDOW, node.attr, node)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id == "dbutils" and isinstance(node.ctx, ast.Load):
            self.add(PLATFORM, "dbutils", node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if id(node) in self._seen_calls:
            self.generic_visit(node)
            return
        name = self._function_name(func)
        if name:
            self.add(FUNCTION, name, node)
        elif isinstance(func, ast.Name) and func.id in ("display", "displayHTML"):
            self.add(PLATFORM, func.id, node)
        elif isinstance(func, ast.Attribute):
            self._attribute_call(node, func)
        self.generic_visit(node)

    def _attribute_call(self, node: ast.Call, func: ast.Attribute) -> None:
        chain, root = _chain(func.value)
        attrs = [n.attr for n in chain if isinstance(n, ast.Attribute)]
        if isinstance(root, ast.Constant):
            return                                  # ", ".join(...), "{}".format(...)
        if isinstance(root, ast.Name):
            if root.id in self.state.window_names:
                self._seen_attrs.update(id(n) for n in chain if isinstance(n, ast.Attribute))
                self._seen_attrs.add(id(func))
                self.add(WINDOW, func.attr, node)
                return
            if root.id == "dbutils":
                return                              # counted once by visit_Name
            if root.id in self.state.foreign or root.id in self.state.pandas:
                return
        if any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr in _TO_PANDAS
               for n in chain):
            return                                  # df.toPandas().head(): a pandas call
        for stream, kind in (("read", READER), ("readStream", READER), ("write", WRITER),
                             ("writeStream", WRITER)):
            if stream in attrs:
                for n in chain:                     # the calls between this one and .read / .write
                    if isinstance(n, ast.Attribute) and n.attr == stream:
                        break
                    if isinstance(n, ast.Call):
                        self._seen_calls.add(id(n))
                if kind == READER:
                    self.add(READER, f"{stream}.{func.attr}" if func.attr in ("table", "jdbc")
                             else stream, node)
                else:
                    self.add(WRITER, f"{stream}.{func.attr}", node)
                return
        if attrs[:1] == ["udf"]:
            self.add(SESSION, "udf", node)
            return
        if attrs[:1] == ["na"] and func.attr in ("fill", "drop", "replace"):
            self.add(METHOD, f"na.{func.attr}", node)
            return
        if attrs[:1] == ["catalog"]:
            self.add(SESSION, f"catalog.{func.attr}", node)
            return
        if isinstance(root, ast.Name) and not chain and root.id in self.state.session_names \
                and func.attr in _SESSION_METHODS:
            self.add(SESSION, func.attr, node)
            return
        if func.attr == "builder" or "builder" in attrs:
            return                                  # SparkSession.builder...getOrCreate()
        if func.attr in ("rowsBetween", "rangeBetween"):
            self.add(WINDOW, func.attr, node)
            return
        if func.attr not in _SPARK_METHODS:
            return
        if func.attr in ("sum", "avg", "mean", "min", "max") and not node.args and not node.keywords:
            return                                  # series.max(): pandas; Spark's take column names
        if func.attr in ("filter", "where") and node.args and _is_str(node.args[0]):
            self.add(METHOD, "filter(str)", node)
            return
        if func.attr == "join":
            how = _join_how(node)
            if how and how.lower() in _JOIN_LATER:
                self.add(METHOD, f"join(how={how.lower()!r})", node)
                return
        self.add(METHOD, func.attr, node)


@dataclass
class _State:
    """Imports and names, carried from cell to cell of a notebook."""
    function_modules: set[str] = field(default_factory=set)      # F in "import ... functions as F"
    function_paths: set[str] = field(default_factory=set)        # "pyspark.sql.functions"
    function_names: dict[str, str] = field(default_factory=dict)  # from ... functions import col as c
    star_functions: bool = False
    window_names: set[str] = field(default_factory=set)
    foreign: set[str] = field(default_factory=set)                # imported non-Spark modules and names
    pandas: set[str] = field(default_factory=set)
    session_names: set[str] = field(default_factory=lambda: {"spark", "session"})


_KNOWN_FUNCTIONS = set(_PLANNED_FUNCTION_RELEASE) | set(_OUT_OF_SCOPE_FUNCTIONS) | set("""
    col column lit when count countDistinct count_distinct sum avg mean min max asc desc
    collect_list collect_set explode explode_outer posexplode array array_contains struct map
    create_map from_json to_json get_json_object size sort_array array_distinct element_at
    sumDistinct sum_distinct approx_count_distinct stddev stddev_samp stddev_pop variance var_samp
    var_pop corr covar_pop covar_samp skewness kurtosis percentile_approx median mode any_value
    unix_timestamp from_unixtime to_utc_timestamp from_utc_timestamp date_part datepart
    format_number format_string sha1 sha2 md5 hash crc32 base64 unbase64 translate soundex ascii
    rand randn uuid typedlit""".split())


def _chain(node: ast.AST) -> tuple[list[ast.AST], ast.AST]:
    """The nodes of a call chain under ``node`` (outermost first) and its root."""
    chain = []
    while True:
        if isinstance(node, ast.Attribute):
            chain.append(node)
            node = node.value
        elif isinstance(node, ast.Call):
            chain.append(node)
            node = node.func
        elif isinstance(node, ast.Subscript):
            node = node.value
        else:
            return chain, node


def _dotted(node: ast.AST) -> str | None:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        return ".".join([node.id, *reversed(parts)])
    return None


def _is_str(node: ast.AST) -> bool:
    return (isinstance(node, ast.Constant) and isinstance(node.value, str)) or isinstance(node, ast.JoinedStr)


def _join_how(node: ast.Call) -> str | None:
    for kw in node.keywords:
        if kw.arg == "how" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    if len(node.args) >= 3 and isinstance(node.args[2], ast.Constant) and isinstance(node.args[2].value, str):
        return node.args[2].value
    return None


# ---------------------------------------------------------------- sources
_MAGIC = re.compile(r"^\s*[%!]")
_SQL_MAGIC = re.compile(r"^\s*%%?sql\b")
_DATABRICKS_CELL = re.compile(r"^# COMMAND -+\s*$", re.M)
_DATABRICKS_SQL = re.compile(r"^# MAGIC\s+%sql\b", re.M)


def _parse_into(report: Report, code: str, file: str, cell: int | None, state: _State,
                supported: dict[str, set[str]], line_offset: int = 0) -> None:
    # IPython magics and shell escapes are not Python: blank them, keeping line numbers
    lines = ["" if _MAGIC.match(line) else line for line in code.splitlines()]
    try:
        tree = ast.parse("\n".join(lines))
    except SyntaxError as exc:
        where = f"cell {cell}" if cell is not None else "file"
        report.warnings.append(f"{file}: {where} skipped, not valid Python ({exc.msg}, line {exc.lineno})")
        return
    _Visitor(report, file, cell, line_offset, state, supported).visit(tree)


def scan_source(code: str, file: str = "<source>", supported: dict[str, set[str]] | None = None) -> Report:
    """Scan a Python script, including Databricks notebooks exported as ``.py``."""
    supported = supported or _supported()
    report = Report(files=[file])
    state = _State()
    if code.startswith("# Databricks notebook source"):
        cell, recorded = 0, set()
        for number, line in enumerate(code.splitlines(), 1):
            if _DATABRICKS_CELL.match(line):
                cell += 1
            elif _DATABRICKS_SQL.match(line) and cell not in recorded:
                recorded.add(cell)
                report.add(PLATFORM, "%sql cell", Location(file, number), supported)
    _parse_into(report, code, file, None, state, supported)
    return report


def scan_notebook(data: dict, file: str = "<notebook>", supported: dict[str, set[str]] | None = None) -> Report:
    """Scan a Jupyter or Databricks ``.ipynb`` notebook (code cells only)."""
    supported = supported or _supported()
    report = Report(files=[file])
    state = _State()
    for number, c in enumerate(data.get("cells", []), 1):   # numbered like Databricks' "Cmd N"
        if c.get("cell_type") != "code":
            continue
        source = c.get("source", "")
        code = "".join(source) if isinstance(source, list) else source
        if _SQL_MAGIC.match(code):
            report.add(PLATFORM, "%sql cell", Location(file, 1, number), supported)
            continue
        _parse_into(report, code, file, number, state, supported)
    return report


def scan_paths(paths: list[str | Path]) -> Report:
    supported = _supported()
    total = Report()
    for path in map(Path, paths):
        if path.is_dir():  # hidden folders (.ipynb_checkpoints, .venv, .git) hold copies or other code
            files = sorted(p for ext in ("*.py", "*.ipynb") for p in path.rglob(ext)
                           if not any(part.startswith(".") for part in p.relative_to(path).parts[:-1]))
        else:
            files = [path]
        for f in files:
            try:
                text = f.read_text(encoding="utf-8")
                if f.suffix == ".ipynb":
                    total.merge(scan_notebook(json.loads(text), str(f), supported))
                else:
                    total.merge(scan_source(text, str(f), supported))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                total.warnings.append(f"{f}: cannot read ({exc})")
    return total


# ---------------------------------------------------------------- output
def _percent(x: float) -> str:
    return f"{x * 100:.0f}%"


def format_text(report: Report, details: bool = False) -> str:
    out = [f"Mborobak compatibility scan: {len(report.files)} file(s)"]
    calls = sum(f.count for f in report.findings.values())
    if not calls:
        out.append("No PySpark calls found.")
    else:
        totals = report.totals()
        out.append(f"{calls} PySpark calls, {len(report.findings)} distinct: "
                   + ", ".join(f"{totals[s]} {s}" for s in _STATUS_ORDER))
        out.append("Calls supported: " + ", ".join(f"{k} {_percent(v)}" for k, v in report.coverage().items()))
        if totals[OUT_OF_SCOPE]:
            out.append("Without out-of-scope calls: "
                       + ", ".join(f"{k} {_percent(v)}" for k, v in report.coverage(in_scope=True).items()))
        out.append("")
        width = max(len(f.label) for f in report.findings.values())
        status_width = max(len(f.status_text) for f in report.findings.values())
        current = None
        for f in report.sorted_findings():
            if f.status != current:
                current = f.status
                out.append(f"{current.capitalize()}:")
            note = f"  {f.note}" if f.note else ""
            out.append(f"  {f.label:<{width}}  {f.count:>4}  {f.status_text:<{status_width}}{note}".rstrip())
            if details:
                for loc in f.sorted_locations[:10]:
                    out.append(f"      {loc}")
                if f.count > 10:
                    out.append(f"      ... and {f.count - 10} more")
    for w in report.warnings:
        out.append(f"warning: {w}")
    return "\n".join(out)


def format_markdown(report: Report) -> str:
    calls = sum(f.count for f in report.findings.values())
    out = ["# Mborobak compatibility scan", "", "Files: " + ", ".join(f"`{f}`" for f in report.files), ""]
    if not calls:
        return "\n".join(out + ["No PySpark calls found."])
    totals = report.totals()
    out += [f"{calls} PySpark calls, {len(report.findings)} distinct.", "",
            "| Calls supported | All calls | Without out-of-scope calls |", "|---|---|---|"]
    scoped = report.coverage(in_scope=True)
    out += [f"| {k} | {_percent(v)} | {_percent(scoped.get(k, 0))} |" for k, v in report.coverage().items()]
    out += ["", "| Status | Calls |", "|---|---|"] + [f"| {s} | {totals[s]} |" for s in _STATUS_ORDER]
    out += ["", "| API | Calls | Status | Note |", "|---|---|---|---|"]
    for f in report.sorted_findings():
        out.append(f"| `{f.label}` | {f.count} | {f.status_text} | {f.note or ''} |")
    if report.warnings:
        out += ["", "Warnings:", ""] + [f"- {w}" for w in report.warnings]
    return "\n".join(out)


def format_json(report: Report) -> str:
    return json.dumps({
        "files": report.files,
        "calls": sum(f.count for f in report.findings.values()),
        "totals": report.totals(),
        "coverage": report.coverage(),
        "coverage_in_scope": report.coverage(in_scope=True),
        "findings": [{"api": f.label, "kind": f.kind, "name": f.name, "status": f.status, "release": f.release,
                      "note": f.note, "count": f.count,
                      "locations": [{"file": l.file, "cell": l.cell, "line": l.line} for l in f.sorted_locations]}
                     for f in report.sorted_findings()],
        "warnings": report.warnings,
    }, indent=2)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="mborobak-scan",
        description="List the PySpark calls in notebooks or scripts and whether Mborobak supports them.")
    parser.add_argument("paths", nargs="+", help=".ipynb or .py files, or directories to search")
    parser.add_argument("--format", choices=("text", "markdown", "json"), default="text")
    parser.add_argument("--details", action="store_true", help="show where each call is (text format)")
    args = parser.parse_args(argv)
    missing = [p for p in args.paths if not Path(p).exists()]
    if missing:
        parser.error("not found: " + ", ".join(missing))
    report = scan_paths(args.paths)
    text = {"text": lambda r: format_text(r, args.details), "markdown": format_markdown,
            "json": format_json}[args.format](report)
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
