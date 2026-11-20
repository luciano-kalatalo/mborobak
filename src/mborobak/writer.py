"""``df.write``: tables written inside IRIS (API contract §4.7, compiler spec §9.7).

``saveAsTable`` compiles the DataFrame to one SELECT and runs ``CREATE TABLE`` plus ``INSERT … SELECT``, so
the rows never leave IRIS. Every write runs in one transaction: IRIS rolls back DDL too (probed on 2026.2,
``probes/write_probe.py``), so a failed write leaves the database as it was.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

from . import expressions as E
from . import plan as P
from .catalog import parse_table_name
from .column import Column
from .compiler import quote
from .errors import (
    AnalysisError,
    MborobakConnectionError,
    TableNotFoundError,
    UnsupportedOperationError,
    UnsupportedTypeError,
    caller_location,
)
from .execution import ExecutionMetrics
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

if TYPE_CHECKING:
    from .dataframe import DataFrame

__all__ = ["DataFrameWriter"]

# Spark's save modes; "error" and "default" mean "errorifexists".
_MODES = {"append": "append", "overwrite": "overwrite", "ignore": "ignore", "error": "errorifexists",
          "errorifexists": "errorifexists", "default": "errorifexists"}

# IRIS's longest VARCHAR (probed: VARCHAR without a length holds one character). Spark strings have no limit.
MAX_VARCHAR = 3641144

_EXISTS_SQL = ("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES "
               "WHERE UPPER(TABLE_SCHEMA) = UPPER(?) AND UPPER(TABLE_NAME) = UPPER(?)")

_NUMERIC = (IntegerType, LongType, DoubleType, DecimalType)
_DATETIME = (DateType, TimestampNTZType)


def _op(name: str) -> str:
    loc = caller_location()
    return f"{name} at {loc}" if loc else name


def ddl_type(dtype: DataType, exact_strings: bool = True, string_length: int = MAX_VARCHAR) -> str:
    """The IRIS column type that reads back as ``dtype``."""
    if isinstance(dtype, IntegerType):
        return "INTEGER"
    if isinstance(dtype, LongType):
        return "BIGINT"
    if isinstance(dtype, DoubleType):
        return "DOUBLE"
    if isinstance(dtype, DecimalType):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    if isinstance(dtype, StringType):
        # EXACT collation: case, trailing spaces and digits compare as Spark compares them (matrix §2.1).
        return f"VARCHAR({string_length})" + (" COLLATE EXACT" if exact_strings else "")
    if isinstance(dtype, BooleanType):
        return "BIT"
    if isinstance(dtype, DateType):
        return "DATE"
    if isinstance(dtype, TimestampNTZType):
        return "TIMESTAMP"
    raise UnsupportedTypeError(f"no IRIS column type for {dtype.simpleString()}")


def can_store(source: DataType, target: DataType) -> bool:
    """Spark's ANSI store assignment (``spark.sql.storeAssignmentPolicy=ANSI``): numbers into numbers (overflow
    fails when the row is written), anything into a string, dates and timestamps into each other."""
    if source == target or isinstance(source, NullType):
        return True
    if isinstance(source, _NUMERIC) and isinstance(target, _NUMERIC):
        return True
    if isinstance(target, StringType):
        return True
    return isinstance(source, _DATETIME) and isinstance(target, _DATETIME)


def check_name(name: str, what: str, op: str) -> None:
    # IRIS rejects '.', ',' and '^' in a delimited identifier (compiler.sql_alias, write probe).
    if not name or not name.strip():
        raise AnalysisError(f"{what} needs a non-empty name", operation=op)
    bad = sorted({c for c in name if c in ".,^"})
    if bad:
        raise AnalysisError(f"{what} {name!r} contains {' '.join(repr(c) for c in bad)}, which IRIS does not allow "
                            "in a column or table name; rename it with alias() or withColumnRenamed()",
                            operation=op)


class DataFrameWriter:
    """``df.write``. Formats, partitioning and bucketing describe files in Spark; in IRIS they have no effect."""

    def __init__(self, df: "DataFrame"):
        self._df = df
        self._mode = "errorifexists"
        self._options: dict[str, Any] = {}

    # ---------------------------------------------------------------- settings
    def mode(self, saveMode: str | None) -> "DataFrameWriter":  # noqa: N803
        if saveMode is not None:
            self._mode = _parse_mode(saveMode)
        return self

    def format(self, source: str) -> "DataFrameWriter":  # noqa: A003
        return self  # delta, parquet, …: the table's storage is IRIS's

    def option(self, key: str, value: Any) -> "DataFrameWriter":
        self._options[key.lower()] = value
        return self

    def options(self, **options: Any) -> "DataFrameWriter":
        for k, v in options.items():
            self.option(k, v)
        return self

    def partitionBy(self, *cols: Any) -> "DataFrameWriter":  # noqa: N802
        return self

    def bucketBy(self, numBuckets: int, col: Any, *cols: Any) -> "DataFrameWriter":  # noqa: N802, N803
        return self

    def sortBy(self, col: Any, *cols: Any) -> "DataFrameWriter":  # noqa: N802
        return self

    # ---------------------------------------------------------------- actions
    def saveAsTable(self, name: str, format: str | None = None, mode: str | None = None,  # noqa: N802, A002
                    partitionBy: Any = None, **options: Any) -> None:  # noqa: N803
        op = _op("saveAsTable")
        self.mode(mode)
        self.options(**options)
        self._check_options(op)
        _Write(self._df, op, "saveAsTable").save_as_table(name, self._mode)

    def insertInto(self, tableName: str, overwrite: bool | None = None) -> None:  # noqa: N802, N803
        op = _op("insertInto")
        self._check_options(op)
        overwrite = self._mode == "overwrite" if overwrite is None else bool(overwrite)
        _Write(self._df, op, "insertInto").insert_into(tableName, overwrite)

    def _check_options(self, op: str) -> None:
        if str(self._options.get("mergeschema", "false")).lower() == "true":
            raise UnsupportedOperationError("the mergeSchema option is not supported: append writes the table's "
                                            "existing columns; use mode('overwrite') to change them", operation=op)
        if "path" in self._options:
            raise UnsupportedOperationError("the path option writes files; Mborobak writes IRIS tables",
                                            operation=op)

    def _files(self, name: str):
        def unsupported(*args: Any, **kwargs: Any):
            raise UnsupportedOperationError(f"{name}() writes files, which Mborobak doesn't do: the data stays in "
                                            "IRIS. Use saveAsTable(name)", operation=_op(name))
        return unsupported

    def __getattr__(self, name: str):
        if name in ("save", "parquet", "csv", "json", "orc", "text", "jdbc", "xml"):
            return self._files(name)
        raise AttributeError(f"'DataFrameWriter' object has no attribute {name!r}")


def _parse_mode(mode: str) -> str:
    if not isinstance(mode, str) or mode.lower() not in _MODES:
        raise ValueError(f"unknown save mode {mode!r}; accepted: append, overwrite, ignore, error, errorifexists")
    return _MODES[mode.lower()]


class _Write:
    """One write: the statements it runs, in one transaction."""

    def __init__(self, df: "DataFrame", op: str, action: str):
        self.df = df
        self.session = df._session
        self.op = op
        self.action = action
        self.session._check_open()
        if self.session._executor is None:
            raise MborobakConnectionError("this session has no IRIS connection", operation=op)
        self.executor = self.session._executor

    # ---------------------------------------------------------------- entry points
    def save_as_table(self, name: str, mode: str) -> None:
        target = parse_table_name(name)
        existing = self.find(target)
        if existing is not None:
            ref, table_type = existing
            if mode == "ignore":
                self.finish([], "", [], 0, 0.0)
                return
            if mode == "errorifexists":
                raise AnalysisError(f"table {ref} already exists; use mode('append') or mode('overwrite')",
                                    operation=self.op)
            if table_type == "VIEW":
                raise AnalysisError(f"{ref} is a view; saveAsTable writes tables", operation=self.op)
            if mode == "append":
                return self.append(ref, by_name=True)
            return self.replace(ref)
        return self.create(target)

    def insert_into(self, name: str, overwrite: bool) -> None:
        target = parse_table_name(name)
        existing = self.find(target)
        if existing is None:
            raise TableNotFoundError(f"table {target} not found; insertInto writes into an existing table, "
                                     "saveAsTable creates one", operation=self.op)
        ref, table_type = existing
        if table_type == "VIEW":
            raise AnalysisError(f"{ref} is a view; insertInto writes tables", operation=self.op)
        if overwrite and self.reads(ref):
            raise AnalysisError(f"insertInto(overwrite=True) cannot read the table it overwrites ({ref}); "
                                "use saveAsTable(name, mode='overwrite')", operation=self.op)
        self.append(ref, by_name=False, delete_first=overwrite)

    # ---------------------------------------------------------------- the three writes
    def create(self, target: P.TableRef) -> None:
        check_name(target.name, "table", self.op)
        check_name(target.schema, "schema", self.op)
        columns = self.new_columns()
        insert, params = self.insert_sql(target, self.df, [n for n, _ in columns])
        self.run([(self.create_sql(target, columns), [])], insert, params, target)

    def replace(self, ref: P.TableRef) -> None:
        """mode('overwrite'): the table takes the DataFrame's columns. The rows go to a staging table, then the
        old table is dropped and the staging table renamed, all in the transaction.

        Not DROP, CREATE, INSERT: when that INSERT fails, ROLLBACK brings back the old table's columns but not
        its rows, as the new table shares its storage (probed on 2026.2, compiler spec §9.7). The staging table
        also lets the DataFrame read the table it replaces. IRIS keeps the renamed table's class name."""
        columns = self.new_columns()
        stage = P.TableRef(ref.schema, f"{ref.name}_mborobak_{uuid.uuid4().hex[:8]}")
        insert, params = self.insert_sql(stage, self.df, [n for n, _ in columns])
        self.run([(self.create_sql(stage, columns), [])], insert, params, ref,
                 after=[(f"DROP TABLE {ref.quoted()}", []),
                        (f"ALTER TABLE {stage.quoted()} RENAME {quote(ref.name)}", [])])

    def append(self, ref: P.TableRef, by_name: bool, delete_first: bool = False) -> None:
        self.session._catalog.refresh(str(ref))
        info = self.session._catalog.lookup(str(ref), self.op)
        unsupported = [f"{c.name} ({c.iris_type})" for c in info.columns if c.dtype is None]
        if unsupported:
            raise UnsupportedTypeError(f"table {ref} has columns of types Mborobak can't write: "
                                       f"{', '.join(unsupported)}", operation=self.op)
        output = self.df._plan.output
        targets = [(c.name, c.dtype) for c in info.columns]
        if len(output) != len(targets):
            raise AnalysisError(f"the DataFrame has {len(output)} columns and table {ref} has {len(targets)}: "
                                f"{[a.name for a in output]} vs {[n for n, _ in targets]}", operation=self.op)
        if by_name:  # saveAsTable(mode='append') matches columns by name, case-insensitively, as Spark does
            by_upper: dict[str, E.Attribute] = {}
            for a in output:
                if a.name.upper() in by_upper:
                    raise AnalysisError(f"the DataFrame has two columns named {a.name!r}", operation=self.op)
                by_upper[a.name.upper()] = a
            missing = [n for n, _ in targets if n.upper() not in by_upper]
            if missing:
                extra = [a.name for a in output if a.name.upper() not in {n.upper() for n, _ in targets}]
                raise AnalysisError(f"table {ref} has columns {missing} that the DataFrame lacks"
                                    + (f" (it has {extra} instead)" if extra else ""), operation=self.op)
            sources = [by_upper[n.upper()] for n, _ in targets]
        else:  # insertInto matches by position
            sources = list(output)
        items = []
        for a, (name, dtype) in zip(sources, targets):
            if not can_store(a.dtype, dtype):
                raise AnalysisError(f"cannot write column {a.name!r} ({a.dtype.simpleString()}) into column "
                                    f"{name!r} ({dtype.simpleString()}) of table {ref}: Spark's ANSI store "
                                    "assignment does not allow it; cast it first", operation=self.op)
            expr: E.Expr = E.AttrRef(a) if a.dtype == dtype else E.Cast(E.AttrRef(a), dtype)
            items.append(Column(E.Alias(expr, name)))
        df = self.df.select(*items)
        insert, params = self.insert_sql(ref, df, [n for n, _ in targets])
        before = [(f"DELETE FROM {ref.quoted()}", [])] if delete_first else []
        self.run(before, insert, params, ref)

    # ---------------------------------------------------------------- pieces
    def find(self, ref: P.TableRef) -> tuple[P.TableRef, str] | None:
        rows = self.executor.lookup(_EXISTS_SQL, [ref.schema, ref.name])
        if not rows:
            return None
        schema, name, table_type = rows[0][0], rows[0][1], str(rows[0][2] or "").upper()
        return P.TableRef(schema, name), table_type

    def new_columns(self) -> list[tuple[str, DataType]]:
        seen: dict[str, str] = {}
        columns = []
        for a in self.df._plan.output:
            check_name(a.name, "column", self.op)
            if a.name.upper() in seen:
                raise AnalysisError(f"the DataFrame has columns {seen[a.name.upper()]!r} and {a.name!r}; IRIS "
                                    "column names are case-insensitive, so a table can't have both",
                                    operation=self.op)
            seen[a.name.upper()] = a.name
            if isinstance(a.dtype, NullType):
                raise AnalysisError(f"column {a.name!r} has type void (a NULL literal); a table column needs a "
                                    "type: cast it, for example lit(None).cast('string')", operation=self.op)
            columns.append((a.name, a.dtype))
        return columns

    def create_sql(self, ref: P.TableRef, columns: list[tuple[str, DataType]]) -> str:
        exact = self.session.conf.get("mborobak.sql.stringSemantics") == "pyspark"
        length = self.session.conf.get("mborobak.write.stringLength")
        body = ", ".join(f"{quote(n)} {ddl_type(t, exact, length)}" for n, t in columns)
        return f"CREATE TABLE {ref.quoted()} ({body})"

    def insert_sql(self, ref: P.TableRef, df: "DataFrame", names: list[str]) -> tuple[str, list]:
        # A Project on top makes the compiler build a block, so a sql() source is wrapped and its WITH clause
        # comes out separately: IRIS takes WITH only before INSERT (probed).
        plan = df._plan
        if not isinstance(plan, P.Project):
            plan = P.Project(plan, tuple((a, E.AttrRef(a)) for a in plan.output))
        stmt = self.session._compile(plan)
        if stmt.functions:
            from .irisfuncs import ensure

            ensure(self.session, stmt.functions, self.op)  # DDL: before the write's transaction
        text = f"INSERT INTO {ref.quoted()} ({', '.join(quote(n) for n in names)})\n{stmt.body}"
        if stmt.cte:
            text = f"{stmt.cte}\n{text}"
        return text, list(stmt.params)

    def reads(self, ref: P.TableRef) -> bool:
        """Whether the DataFrame reads ``ref``. A sql() source may read anything, so it counts."""
        key = (ref.schema.upper(), ref.name.upper())

        def visit(p: P.Plan) -> bool:
            if isinstance(p, P.TableScan) and (p.table.schema.upper(), p.table.name.upper()) == key:
                return True
            if isinstance(p, P.SqlSource):
                return True
            return any(visit(c) for c in p.children())

        return visit(self.df._plan)

    def run(self, before: list[tuple[str, list]], insert: str, params: list, target: P.TableRef,
            after: list[tuple[str, list]] = ()) -> None:
        t0 = time.perf_counter()
        statements = 0
        self.executor.execute("START TRANSACTION", [], self.op)
        try:
            for sql, args in before:
                self.executor.execute(sql, args, self.op)
                statements += 1
            from .session import _error_candidates

            rows = self.executor.execute(insert, params, self.op, _error_candidates(self.df._plan))
            statements += 1
            for sql, args in after:
                self.executor.execute(sql, args, self.op)
                statements += 1
            self.executor.execute("COMMIT", [], self.op)
        except BaseException:
            try:
                self.executor.execute("ROLLBACK", [], self.op)
            except Exception:  # noqa: BLE001 - the original error matters
                pass
            raise
        finally:
            self.session._catalog.refresh(str(target))
        self.finish([s for s, _ in before] + [insert] + [s for s, _ in after], insert, params,
                    max(rows, 0) if isinstance(rows, int) else 0, (time.perf_counter() - t0) * 1000, statements)

    def finish(self, statements: list[str], insert: str, params: list, rows: int, server_ms: float,
               count: int = 0) -> None:
        self.session.lastExecution = ExecutionMetrics(
            action=self.action, sql="\n;\n".join(statements) if statements else "", params=list(params),
            data_queries=count, rows=rows, server_ms=server_ms)
