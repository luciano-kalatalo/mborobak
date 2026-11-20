"""``MborobakSession`` (API contract §3)."""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any

from . import expressions as E
from . import plan as P
from .catalog import Catalog
from .compiler import CompileOptions, Compiler, SqlStatement
from .errors import (
    MborobakConnectionError,
    SessionClosedError,
    UnsupportedOperationError,
    caller_location,
)
from .execution import (
    ConversionError,
    ExecutionMetrics,
    Executor,
    QueryCounter,
    convert_columns,
    convert_rows,
)

__all__ = ["MborobakSession"]

_CONNECTION_KEYS = ("host", "port", "namespace", "user", "password", "timeout")
_SETTINGS: dict[str, tuple[Any, Any]] = {
    # key: (default, validator)
    "mborobak.sql.stringSemantics": ("pyspark", lambda v: v in ("pyspark", "iris")),
    "mborobak.optimizer.enabled": (True, lambda v: isinstance(v, bool)),
    "mborobak.sql.maxParameters": (900, lambda v: isinstance(v, int) and 1 <= v <= 900),
    "mborobak.sql.cacheSize": (256, lambda v: isinstance(v, int) and v >= 0),
    "mborobak.show.truncate": (20, lambda v: isinstance(v, int) and v >= 0),
    # VARCHAR length of string columns in tables that df.write creates; IRIS's maximum by default
    "mborobak.write.stringLength": (3641144, lambda v: isinstance(v, int) and 1 <= v <= 3641144),
}


class RuntimeConfig:
    def __init__(self, values: dict[str, Any]):
        self._values = {k: d for k, (d, _) in _SETTINGS.items()}
        for k, v in values.items():
            self.set(k, v)

    @staticmethod
    def _check(key: str, value: Any) -> None:
        if key.startswith("spark."):
            raise UnsupportedOperationError(f"{key!r} is a Spark setting; Mborobak settings start with "
                                            "'mborobak.'")
        if key not in _SETTINGS:
            raise UnsupportedOperationError(f"unknown setting {key!r}; valid keys: {', '.join(_SETTINGS)}")
        if not _SETTINGS[key][1](value):
            raise ValueError(f"invalid value {value!r} for {key}")

    def get(self, key: str, default: Any = None) -> Any:
        return self._values.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._check(key, value)
        self._values[key] = value

    def unset(self, key: str) -> None:
        self._check(key, _SETTINGS.get(key, (None,))[0])
        self._values[key] = _SETTINGS[key][0]


class _Builder:
    def __init__(self):
        self._options: dict[str, Any] = {}
        self._connection = None
        self._app_name: str | None = None

    def config(self, key: str | None = None, value: Any = None, *, map: dict | None = None) -> "_Builder":  # noqa: A002
        items = dict(map or {})
        if key is not None:
            items[key] = value
        for k, v in items.items():
            if k.startswith("spark."):
                raise UnsupportedOperationError(f"{k!r} is a Spark setting; it has no effect in Mborobak")
            self._options[k] = v
        return self

    def connection(self, conn) -> "_Builder":
        self._connection = conn
        return self

    def appName(self, name: str) -> "_Builder":  # noqa: N802
        self._app_name = name
        return self

    def _split(self):
        conn_opts = {k: v for k, v in self._options.items() if k in _CONNECTION_KEYS}
        settings = {k: v for k, v in self._options.items() if k not in _CONNECTION_KEYS}
        unknown = [k for k in settings if not k.startswith("mborobak.")]
        if unknown:
            raise UnsupportedOperationError(f"unknown option(s) {unknown}; connection keys are "
                                            f"{', '.join(_CONNECTION_KEYS)}")
        return conn_opts, settings

    def create(self) -> "MborobakSession":
        conn_opts, settings = self._split()
        return MborobakSession(conn_opts, settings, self._connection, self._app_name)

    def getOrCreate(self) -> "MborobakSession":  # noqa: N802
        conn_opts, settings = self._split()
        active = MborobakSession._active
        if active is not None and not active._closed and active._conn_opts == conn_opts \
                and (self._connection is None or self._connection is active._connection):
            for k, v in settings.items():
                active.conf.set(k, v)
            return active
        session = MborobakSession(conn_opts, settings, self._connection, self._app_name)
        MborobakSession._active = session
        return session


class _BuilderDescriptor:
    def __get__(self, obj, owner) -> _Builder:
        return _Builder()


class _CatalogAPI:
    def __init__(self, session: "MborobakSession"):
        self._s = session

    def refresh(self, name: str | None = None) -> None:
        self._s._catalog.refresh(name)

    def tableExists(self, name: str) -> bool:  # noqa: N802
        from .errors import TableNotFoundError

        try:
            self._s._catalog.lookup(name)
            return True
        except TableNotFoundError:
            return False

    def listColumns(self, name: str):  # noqa: N802
        return list(self._s._catalog.lookup(name).columns)


class MborobakSession:
    builder = _BuilderDescriptor()
    _active: "MborobakSession | None" = None

    def __init__(self, conn_opts: dict, settings: dict, connection=None, app_name: str | None = None,
                 catalog: Catalog | None = None, describer=None):
        self._describer = describer  # unit tests: (sql, params) -> cursor.description
        self._conn_opts = dict(conn_opts)
        self._owns_connection = connection is None and catalog is None
        self._closed = False
        self.appName = app_name
        self.conf = RuntimeConfig(settings)
        self._counter = QueryCounter()
        self._connection = connection
        if self._connection is None and catalog is None:
            self._connection = self._connect()
        self._executor = Executor(self._connection, self._counter) if self._connection is not None else None
        self._catalog = catalog or Catalog(self._executor.lookup)
        self.catalog = _CatalogAPI(self)
        self._sql_cache: OrderedDict = OrderedDict()
        self.lastExecution: ExecutionMetrics | None = None
        self.irisVersion: str | None = None

    @classmethod
    def _for_tests(cls, catalog: Catalog, describer=None, **settings) -> "MborobakSession":
        """A session without IRIS, for compiler unit tests."""
        return cls({}, settings, None, None, catalog=catalog, describer=describer)

    @classmethod
    def getActiveSession(cls) -> "MborobakSession | None":  # noqa: N802
        return cls._active if cls._active is not None and not cls._active._closed else None

    @property
    def version(self) -> str:
        from . import __version__

        return __version__

    def _connect(self):
        o = self._conn_opts
        missing = [k for k in ("host", "port", "namespace", "user", "password") if k not in o]
        if missing:
            raise MborobakConnectionError(f"missing connection settings: {', '.join(missing)}")
        try:
            import iris
        except ImportError as exc:
            raise MborobakConnectionError("the IRIS driver is missing: pip install intersystems-irispython") \
                from exc
        try:
            kwargs = {"timeout": int(o["timeout"] * 1000)} if "timeout" in o else {}
            return iris.connect(o["host"], int(o["port"]), o["namespace"], o["user"], o["password"], **kwargs)
        except Exception as exc:  # noqa: BLE001
            first = str(exc).splitlines()[0] if str(exc) else type(exc).__name__
            raise MborobakConnectionError(
                f"cannot connect to IRIS at {o['host']}:{o['port']}/{o['namespace']}: {first}") from None

    def _check_open(self) -> None:
        if self._closed:
            raise SessionClosedError("this session was stopped")

    # ---------------------------------------------------------------- sources
    def table(self, tableName: str):  # noqa: N803
        from .dataframe import DataFrame

        self._check_open()
        loc = caller_location()
        op = f"table at {loc}" if loc else "table"
        info = self._catalog.lookup(tableName, op)
        attrs, unsupported = info.attributes()
        return DataFrame(self, P.TableScan(info.table, attrs, unsupported))

    def sql(self, sqlQuery: str, args: list | tuple | None = None):  # noqa: N803
        """A DataFrame over one IRIS SELECT. ``args`` fill its ``?`` markers, in order."""
        from .catalog import type_from_description
        from .dataframe import DataFrame
        from .errors import UnsupportedTypeError
        from .sqltext import prepare

        self._check_open()
        loc = caller_location()
        op = f"sql at {loc}" if loc else "sql"
        if isinstance(args, dict):
            raise UnsupportedOperationError("named sql() parameters are planned for P1; pass a list",
                                            operation=op)
        values = tuple(args or ())
        query = prepare(sqlQuery)
        if query.params != len(values):
            raise ValueError(f"the query has {query.params} '?' markers but {len(values)} args were given")
        probe = f"SELECT * FROM ({query.body}) AS mborobak_q WHERE 1 = 0"
        if query.cte:
            probe = f"{query.cte}\n{probe}"
        description = self._describe(probe, list(values), op)
        attrs = []
        for d in description:
            name, type_code, precision, scale = d[0], d[1], d[4], d[5]
            iris_type, dtype = type_from_description(type_code, precision, scale)
            if dtype is None:
                raise UnsupportedTypeError(
                    f"column {name!r} of the query has IRIS type {iris_type}, which is not supported in P0; "
                    "CAST it in the SQL (for example to VARCHAR)", operation=op)
            nullable = len(d) < 7 or d[6] != 0
            attrs.append(E.Attribute(E.new_id(), name, dtype, nullable, None))
        return DataFrame(self, P.SqlSource(query, values, tuple(attrs)))

    def _describe(self, sql: str, params: list, operation: str | None):
        if self._describer is not None:
            return self._describer(sql, params)
        if self._executor is None:
            raise MborobakConnectionError("this session has no IRIS connection")
        return self._executor.describe(sql, params, operation)

    # ---------------------------------------------------------------- compile and run
    def _options(self) -> CompileOptions:
        return CompileOptions(
            string_semantics=self.conf.get("mborobak.sql.stringSemantics"),
            max_parameters=self.conf.get("mborobak.sql.maxParameters"),
        )

    def _compile(self, plan: P.Plan) -> SqlStatement:
        stmt, _ = self._compile_cached(plan)
        return stmt

    def _compile_cached(self, plan: P.Plan) -> tuple[SqlStatement, bool]:
        options = self._options()
        try:
            key = (P.plan_key(plan), options)
            hash(key)
        except TypeError:  # an unhashable literal: skip the cache
            return Compiler(options).compile(plan), False
        size = self.conf.get("mborobak.sql.cacheSize")
        if size and key in self._sql_cache:
            self._sql_cache.move_to_end(key)
            return self._sql_cache[key], True
        stmt = Compiler(options).compile(plan)
        if size:
            self._sql_cache[key] = stmt
            while len(self._sql_cache) > size:
                self._sql_cache.popitem(last=False)
        return stmt, False

    def _execute(self, plan: P.Plan, action: str, operation: str, as_columns: bool = False):
        self._check_open()
        if self._executor is None:
            raise MborobakConnectionError("this session has no IRIS connection")
        lookups_before = self._counter.schema_lookups
        t0 = time.perf_counter()
        stmt, cache_hit = self._compile_cached(plan)
        t1 = time.perf_counter()
        candidates = _error_candidates(plan)
        rows, server_ms, fetch_ms = self._executor.run(stmt.text, stmt.params, operation, candidates)
        t2 = time.perf_counter()
        try:
            converted = convert_columns(rows, stmt.schema) if as_columns else convert_rows(rows, stmt.schema)
        except ConversionError as exc:
            from .errors import MborobakArithmeticError

            raise MborobakArithmeticError(str(exc), operation=operation, sql=stmt.text) from None
        fetch_ms += (time.perf_counter() - t2) * 1000
        self.lastExecution = ExecutionMetrics(
            action=action, sql=stmt.text, params=list(stmt.params), data_queries=1,
            schema_lookups=self._counter.schema_lookups - lookups_before, rows=len(rows),
            python_ms=(t1 - t0) * 1000, server_ms=server_ms, fetch_ms=fetch_ms, cache_hit=cache_hit)
        return tuple(stmt.schema.fieldNames()), converted

    # ---------------------------------------------------------------- lifecycle
    def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_connection and self._connection is not None:
            try:
                self._connection.close()
            except Exception:  # noqa: BLE001
                pass
        if MborobakSession._active is self:
            MborobakSession._active = None

    def __enter__(self) -> "MborobakSession":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

    def __repr__(self) -> str:
        o = self._conn_opts
        if o:
            where = f"{o.get('host')}:{o.get('port')}/{o.get('namespace')}"
        else:
            where = "given connection" if self._connection is not None else "no connection"
        return f"MborobakSession({where})"


def _error_candidates(plan: P.Plan) -> list[str]:
    """Expressions that could fail at run time, for error messages (§7.5)."""
    found: list[str] = []

    def exprs(p: P.Plan):
        if isinstance(p, P.Project):
            yield from (e for _, e in p.items)
        elif isinstance(p, P.Filter):
            yield p.condition
        elif isinstance(p, P.Aggregate):
            yield from (e for _, e in p.keys)
            yield from (e for _, e in p.aggs)
        elif isinstance(p, P.Sort):
            yield from p.orders

    def visit(p: P.Plan) -> None:
        from .naming import expr_name

        for e in exprs(p):
            for node in E.walk(e):
                if isinstance(node, E.BinaryArithmetic) and node.op in "/%":
                    found.append(expr_name(node))
                elif isinstance(node, E.Cast):
                    found.append(expr_name(E.Cast(node.child, node.to)) + " (cast)")
        for c in p.children():
            visit(c)

    visit(plan)
    return list(dict.fromkeys(found))
