"""``DataFrame`` and ``GroupedData`` (API contract §4)."""

from __future__ import annotations

import decimal
import math
import time
from typing import TYPE_CHECKING, Any

from . import expressions as E
from . import plan as P
from .analyzer import (
    unique_ids,
    check_grouped,
    wider,
    check_no_aggregate,
    find_attr,
    infer,
    make_attr,
    output_name,
    resolve,
)
from .column import Column, literal, to_column_expr
from .errors import AnalysisError, ColumnNotFoundError, UnsupportedOperationError, caller_location

from .row import Row, make_rows
from .sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    StringType,
    StructField,
    StructType,
)

if TYPE_CHECKING:
    from .session import MborobakSession

__all__ = ["DataFrame", "GroupedData"]

_LATER = {
    "unionByName": "P1", "dropDuplicates": "P1", "drop_duplicates": "P1", "selectExpr": "P1",
    "toLocalIterator": "P1", "write": "P1", "createOrReplaceTempView": "P1",
}

_JOIN_TYPES = {
    "inner": "inner",
    "left": "left", "leftouter": "left", "left_outer": "left",
    "right": "right", "rightouter": "right", "right_outer": "right",
    "full": "full", "fullouter": "full", "full_outer": "full", "outer": "full",
    "cross": "cross",
}
_LATER_JOINS = ("semi", "leftsemi", "left_semi", "anti", "leftanti", "left_anti")
_NEVER = ("cache", "persist", "unpersist", "checkpoint", "localCheckpoint", "repartition", "coalesce",
          "rdd", "foreach", "foreachPartition", "mapInPandas", "toArrow", "writeStream")


def _op(name: str) -> str:
    loc = caller_location()
    return f"{name} at {loc}" if loc else name


def _flatten(cols: tuple) -> list:
    if len(cols) == 1 and isinstance(cols[0], (list, tuple)):
        return list(cols[0])
    return list(cols)


class DataFrame:
    """A lazy, immutable query over IRIS tables."""

    def __init__(self, session: "MborobakSession", plan: P.Plan):
        self._session = session
        self._plan = plan

    def _with(self, plan: P.Plan) -> "DataFrame":
        return DataFrame(self._session, plan)

    # ---------------------------------------------------------------- columns
    def __getitem__(self, item: str | int) -> Column:
        if isinstance(item, int):
            return Column(E.AttrRef(self._plan.output[item]))
        if isinstance(item, str):
            return Column(E.AttrRef(find_attr(item, self._plan.output, _op(f"df[{item!r}]"))))
        raise TypeError(f"unexpected item type: {type(item).__name__}")

    def __getattr__(self, name: str) -> Column:
        if name.startswith("_"):
            raise AttributeError(name)
        if name in _NEVER:
            raise UnsupportedOperationError(
                f"{name}() is not supported: Mborobak runs every action as a query in IRIS",
                operation=_op(name))
        if name in _LATER:
            raise UnsupportedOperationError(f"{name}() is planned for {_LATER[name]}", operation=_op(name))
        try:
            return Column(E.AttrRef(find_attr(name, self._plan.output, None)))
        except AnalysisError:
            raise AttributeError(f"'DataFrame' object has no attribute {name!r}") from None

    # ---------------------------------------------------------------- transformations
    def select(self, *cols: Any) -> "DataFrame":
        op = _op("select")
        items: list[tuple[E.Attribute, E.Expr]] = []
        exprs = [to_column_expr(c) for c in _flatten(cols)]
        if any(E.contains_aggregate(e) for e in exprs):
            return GroupedData(self, [], op).agg(*[Column(e) for e in exprs])
        for original in exprs:
            if isinstance(original, E.Star):
                items.extend((a, E.AttrRef(a)) for a in self._plan.output)
                continue
            resolved = resolve(original, self._plan, op)
            if isinstance(resolved, E.SortOrder):
                raise AnalysisError("asc()/desc() can only be used in orderBy()", operation=op)
            items.append(make_attr(output_name(original), resolved, op))
        return self._with(P.Project(self._plan, tuple(unique_ids(items))))

    def filter(self, condition: Column) -> "DataFrame":
        op = _op("filter")
        if isinstance(condition, str):
            raise UnsupportedOperationError("filter() with a SQL string is planned for P1; use F.col(...)",
                                            operation=op)
        if not isinstance(condition, Column):
            raise TypeError("filter() takes a Column condition")
        base, (resolved,), restore = self._resolve_with_missing([condition._expr], op)
        check_no_aggregate(resolved, op, "filter(); filter after agg() instead")
        dtype, _ = infer(resolved, op)
        if not isinstance(dtype, (BooleanType, NullType)):
            raise AnalysisError(f"filter() condition must be boolean, got {dtype.simpleString()}", operation=op)
        return self._with(_restore(P.Filter(base, resolved), restore))

    def _resolve_with_missing(self, exprs: list[E.Expr], op: str):
        """Resolve ``exprs``; a column dropped by an earlier select() is brought back, as Spark does.

        Spark's analyzer resolves ``df.select("id").filter(col("i") > 1)`` by adding ``i`` to the
        projection, filtering, and projecting the original columns again (matrix NAME-07). Returns
        (plan to build on, resolved expressions, original output to restore or None).
        """
        try:
            return self._plan, [resolve(e, self._plan, op) for e in exprs], None
        except ColumnNotFoundError as first:
            chain: list[P.Plan] = []
            node = self._plan
            while isinstance(node, (P.Filter, P.Sort, P.Limit)):  # these keep the rows' columns
                chain.append(node)
                node = node.child
            if not isinstance(node, P.Project):
                raise first
            names = {a.name.lower() for a in node.output}
            ids = {a.id for a in node.output}
            extra = tuple((a, E.AttrRef(a)) for a in node.child.output
                          if a.id not in ids and a.name.lower() not in names)
            if not extra:
                raise first
            widened: P.Plan = P.Project(node.child, node.items + extra)
            for wrapper in reversed(chain):
                widened = _rebuild(wrapper, widened)
            try:
                resolved = [resolve(e, widened, op) for e in exprs]
            except ColumnNotFoundError:
                raise first from None
            return widened, resolved, self._plan.output

    where = filter

    def withColumn(self, colName: str, col: Column) -> "DataFrame":  # noqa: N802, N803
        return self._with_columns({colName: col}, _op("withColumn"))

    def withColumns(self, colsMap: dict[str, Column]) -> "DataFrame":  # noqa: N802, N803
        return self._with_columns(colsMap, _op("withColumns"))

    def _with_columns(self, mapping: dict[str, Column], op: str) -> "DataFrame":
        current = [(a, E.AttrRef(a)) for a in self._plan.output]
        for name, col in mapping.items():
            if not isinstance(col, Column):
                raise TypeError("withColumn() takes a Column")
            resolved = resolve(col._expr, self._plan, op)
            check_no_aggregate(resolved, op, "withColumn()")
            item = make_attr(name, resolved, op)
            if isinstance(item[1], E.AttrRef):  # a copy of a column needs its own id
                a = item[1].attr
                item = (E.Attribute(E.new_id(), name, a.dtype, a.nullable, a.collation), item[1])
            idx = [i for i, (a, _) in enumerate(current) if a.name.lower() == name.lower()]
            if idx:
                current[idx[0]] = item
            else:
                current.append(item)
        return self._with(P.Project(self._plan, tuple(current)))

    def withColumnRenamed(self, existing: str, new: str) -> "DataFrame":  # noqa: N802
        return self.withColumnsRenamed({existing: new})

    def withColumnsRenamed(self, colsMap: dict[str, str]) -> "DataFrame":  # noqa: N802, N803
        lowered = {k.lower(): v for k, v in colsMap.items()}
        items = []
        changed = False
        for a in self._plan.output:
            new = lowered.get(a.name.lower())
            if new is not None:
                changed = True
                items.append((a.renamed(new), E.AttrRef(a)))
            else:
                items.append((a, E.AttrRef(a)))
        return self._with(P.Project(self._plan, tuple(items))) if changed else self

    def drop(self, *cols: str | Column) -> "DataFrame":
        names, ids = set(), set()
        for c in cols:
            if isinstance(c, str):
                names.add(c.lower())
            elif isinstance(c, Column) and isinstance(c._expr, E.AttrRef):
                ids.add(c._expr.attr.id)
            elif isinstance(c, Column) and isinstance(c._expr, E.UnresolvedColumn):
                names.add(c._expr.name.lower())
            else:
                raise TypeError("drop() takes column names or columns")
        keep = [a for a in self._plan.output if a.name.lower() not in names and a.id not in ids]
        if len(keep) == len(self._plan.output):
            return self
        return self._with(P.Project(self._plan, tuple((a, E.AttrRef(a)) for a in keep)))

    # ---------------------------------------------------------------- missing values
    def _subset(self, subset: Any, op: str) -> list[E.Attribute]:
        if subset is None:
            return list(self._plan.output)
        names = [subset] if isinstance(subset, str) else list(subset)
        if not all(isinstance(n, str) for n in names):
            raise TypeError("subset takes a column name or a list of column names")
        return [find_attr(n, self._plan.output, op) for n in names]

    def fillna(self, value: Any, subset: Any = None) -> "DataFrame":
        """Replace NULL (and NaN in double columns) with ``value`` in the columns whose type matches it:
        a number fills numeric columns, a string fills string columns, a bool fills boolean columns.
        A dict maps column names to values. As in Spark, the value is cast to each column's type."""
        op = _op("fillna")
        if isinstance(value, dict):
            targets = {find_attr(k, self._plan.output, op).id: v for k, v in value.items()}
        else:
            targets = {a.id: value for a in self._subset(subset, op)}
        items: list[tuple[E.Attribute, E.Expr]] = []
        changed = False
        for a in self._plan.output:
            if a.id not in targets or not _fills(targets[a.id], a.dtype, isinstance(value, dict), op) \
                    or (not a.nullable and not isinstance(a.dtype, DoubleType)):
                items.append((a, E.AttrRef(a)))
                continue
            ref = E.AttrRef(a)
            fill = E.Cast(literal(targets[a.id]), a.dtype)
            if isinstance(a.dtype, DoubleType):  # Spark replaces NaN too
                expr = E.CaseWhen(((E.Or(E.IsNull(ref), E.Func("isnan", (ref,))), fill),), ref)
            else:
                expr = E.Func("coalesce", (ref, fill))
            infer(expr, op)
            items.append((E.Attribute(E.new_id(), a.name, a.dtype, False), expr))
            changed = True
        return self._with(P.Project(self._plan, tuple(items))) if changed else self

    def dropna(self, how: str = "any", thresh: int | None = None, subset: Any = None) -> "DataFrame":
        """Drop rows with NULL (or NaN in double columns): ``how="any"`` drops a row with any, ``"all"``
        a row with only such values; ``thresh`` keeps rows with at least that many valid values."""
        op = _op("dropna")
        if how not in ("any", "all"):
            raise ValueError("how must be 'any' or 'all'")
        attrs = self._subset(subset, op)
        valid = []
        always = 0  # columns that are never NULL or NaN
        for a in attrs:
            ref = E.AttrRef(a)
            if not a.nullable and not isinstance(a.dtype, DoubleType):
                always += 1
                continue
            cond: E.Expr = E.IsNull(ref, negated=True)
            if isinstance(a.dtype, DoubleType):
                cond = E.And(cond, E.Not(E.Func("isnan", (ref,))))
            valid.append(cond)
        if thresh is not None:
            if not isinstance(thresh, int) or isinstance(thresh, bool):
                raise TypeError("thresh takes an integer")
            if always >= thresh:
                return self
            total: E.Expr = E.Literal(always, IntegerType())
            for c in valid:
                total = E.BinaryArithmetic("+", total, E.CaseWhen(((c, E.Literal(1, IntegerType())),),
                                                                 E.Literal(0, IntegerType())))
            condition: E.Expr = E.Comparison(">=", total, E.Literal(thresh, IntegerType()))
        else:
            if not valid or (how == "all" and always):
                return self
            condition = valid[0]
            for c in valid[1:]:
                condition = E.And(condition, c) if how == "any" else E.Or(condition, c)
        infer(condition, op)
        return self._with(P.Filter(self._plan, condition))

    @property
    def na(self) -> "DataFrameNaFunctions":
        return DataFrameNaFunctions(self)

    def orderBy(self, *cols: Any, ascending: bool | list[bool] = True) -> "DataFrame":  # noqa: N802
        op = _op("orderBy")
        cols_list = _flatten(cols)
        if not cols_list:
            raise ValueError("orderBy() needs at least one column")
        flags = ascending if isinstance(ascending, list) else [ascending] * len(cols_list)
        if len(flags) != len(cols_list):
            raise ValueError("ascending list must have one value per column")
        orders = []
        for c, asc in zip(cols_list, flags):
            expr = to_column_expr(c)
            if not isinstance(expr, E.SortOrder):
                expr = E.SortOrder(expr, bool(asc), None)
            orders.append(expr)
        base, resolved, restore = self._resolve_with_missing(orders, op)
        for r in resolved:
            check_no_aggregate(r, op, "orderBy()")
        return self._with(_restore(P.Sort(base, tuple(resolved)), restore))

    sort = orderBy

    def limit(self, num: int) -> "DataFrame":
        if not isinstance(num, int) or isinstance(num, bool):
            raise TypeError("limit() takes an int")
        if num < 0:
            raise ValueError("limit() must be >= 0")
        return self._with(P.Limit(self._plan, num))

    def distinct(self) -> "DataFrame":
        return self._with(P.Distinct(self._plan))

    def groupBy(self, *cols: Any) -> "GroupedData":  # noqa: N802
        return GroupedData(self, _flatten(cols), _op("groupBy"))

    groupby = groupBy

    def agg(self, *exprs: Any) -> "DataFrame":
        return GroupedData(self, [], _op("agg")).agg(*exprs)

    def alias(self, alias: str) -> "DataFrame":
        if not isinstance(alias, str) or not alias or "." in alias:
            raise ValueError("alias() takes a non-empty name without dots")
        return self._with(P.SubqueryAlias(self._plan, alias))

    # ---------------------------------------------------------------- joins
    def join(self, other: "DataFrame", on: Any = None, how: str | None = None) -> "DataFrame":
        op = _op("join")
        if not isinstance(other, DataFrame):
            raise TypeError("join() takes a DataFrame")
        if other._session is not self._session:
            raise ValueError("cannot join DataFrames from different sessions")
        key = (how or "inner").lower()
        if key in _LATER_JOINS:
            raise UnsupportedOperationError(f"{how} joins are planned for P1", operation=op)
        if key not in _JOIN_TYPES:
            raise ValueError(f"unknown join type {how!r}; use one of {', '.join(_JOIN_TYPES)}")
        join_type = _JOIN_TYPES[key]
        left, right = self._plan, _distinct_ids(self._plan, other._plan)

        if on is None or (isinstance(on, (list, tuple)) and not on):
            return self._with(P.Join(left, right, "cross" if join_type == "inner" else join_type, None))
        if join_type == "cross":
            join_type = "inner"  # cross with a condition is an inner join, as in Spark

        names = [on] if isinstance(on, str) else list(on) if isinstance(on, (list, tuple)) and all(
            isinstance(n, str) for n in on) else None
        if names is not None:
            return self._using_join(left, right, names, join_type, op)

        conditions = list(on) if isinstance(on, (list, tuple)) else [on]
        if not all(isinstance(c, Column) for c in conditions):
            raise TypeError("join(on=...) takes a column name, a list of names, or Column conditions")
        draft = P.Join(left, right, join_type, None)
        resolved = [resolve(c._expr, draft, op) for c in conditions]
        cond = resolved[0]
        for c in resolved[1:]:
            cond = E.And(cond, c)
        check_no_aggregate(cond, op, "a join condition")
        dtype, _ = infer(cond, op)
        if not isinstance(dtype, BooleanType):
            raise AnalysisError(f"join condition must be boolean, got {dtype.simpleString()}", operation=op)
        return self._with(P.Join(left, right, join_type, cond))

    def _using_join(self, left: P.Plan, right: P.Plan, names: list[str], join_type: str, op: str) -> "DataFrame":
        pairs = []
        cond = None
        for name in names:
            la = find_attr(name, left.output, op)
            ra = find_attr(name, right.output, op)
            if wider(la.dtype, ra.dtype) is None:
                raise AnalysisError(f"join key {name!r} has types {la.dtype.simpleString()} and "
                                    f"{ra.dtype.simpleString()}", operation=op)
            pairs.append((la, ra))
            eq = E.Comparison("=", E.AttrRef(la), E.AttrRef(ra))
            cond = eq if cond is None else E.And(cond, eq)
        joined = P.Join(left, right, join_type, cond)
        current = {a.id: a for a in joined.output}
        items: list[tuple[E.Attribute, E.Expr]] = []
        for la, ra in pairs:
            la, ra = current[la.id], current[ra.id]
            if join_type == "right":
                items.append((ra, E.AttrRef(ra)))
            elif join_type == "full":
                dtype = wider(la.dtype, ra.dtype)
                attr = E.Attribute(E.new_id(), la.name, dtype, la.nullable and ra.nullable)
                items.append((attr, E.Coalesce((E.AttrRef(la), E.AttrRef(ra)))))
            else:
                items.append((la, E.AttrRef(la)))
        key_ids = {a.id for pair in pairs for a in pair}
        items += [(a, E.AttrRef(a)) for a in joined.output if a.id not in key_ids]
        return self._with(P.Project(joined, tuple(items)))

    def crossJoin(self, other: "DataFrame") -> "DataFrame":  # noqa: N802
        return self.join(other, how="cross")

    # ---------------------------------------------------------------- union
    def union(self, other: "DataFrame") -> "DataFrame":
        op = _op("union")
        if not isinstance(other, DataFrame):
            raise TypeError("union() takes a DataFrame")
        first, second = self._plan.output, other._plan.output
        if len(first) != len(second):
            raise AnalysisError(f"union() needs the same number of columns: {len(first)} and {len(second)}",
                                operation=op)
        attrs = []
        for a, b in zip(first, second):
            t = wider(a.dtype, b.dtype)
            if t is None or (isinstance(t, NullType)):
                raise AnalysisError(
                    f"union(): column {a.name!r} has types {a.dtype.simpleString()} and "
                    f"{b.dtype.simpleString()}, which can't be combined (compatibility matrix UNI-03)",
                    operation=op)
            same = t == a.dtype
            attrs.append(E.Attribute(a.id, a.name, t, a.nullable or b.nullable, a.collation if same else None))
        inputs = (self._plan.inputs if isinstance(self._plan, P.Union) else (self._plan,)) + \
            (other._plan.inputs if isinstance(other._plan, P.Union) else (other._plan,))
        return self._with(P.Union(inputs, tuple(attrs)))

    unionAll = union

    # ---------------------------------------------------------------- schema
    @property
    def schema(self) -> StructType:
        return StructType([StructField(a.name, a.dtype, a.nullable) for a in self._plan.output])

    @property
    def columns(self) -> list[str]:
        return [a.name for a in self._plan.output]

    @property
    def dtypes(self) -> list[tuple[str, str]]:
        return [(a.name, a.dtype.simpleString()) for a in self._plan.output]

    def printSchema(self) -> None:  # noqa: N802
        print("root")
        for a in self._plan.output:
            print(f" |-- {a.name}: {a.dtype.simpleString()} (nullable = {str(a.nullable).lower()})")

    # ---------------------------------------------------------------- diagnostics
    def toSQL(self) -> tuple[str, list]:  # noqa: N802
        stmt = self._session._compile(self._plan)
        return stmt.text, list(stmt.params)

    def explain(self, mode: str | bool | None = None) -> None:
        plan = self._plan
        stmt = self._session._compile(plan)
        params = "\n".join(f"{i}: {v!r}" for i, v in enumerate(stmt.params, 1)) or "(none)"
        if mode == "sql":
            print(f"{stmt.text}\n\n== Parameters ==\n{params}")
            return
        translations = "\n".join(stmt.translations) or "(none)"
        print(
            f"== Logical plan ==\n{P.tree_string(plan)}\n\n"
            f"== Translations ==\n{translations}\n\n"
            f"== Rewrites ==\n(none)\n\n"
            f"== SQL ==\n{stmt.text}\n\n"
            f"== Parameters ==\n{params}"
        )

    # ---------------------------------------------------------------- actions
    def _run(self, plan: P.Plan, action: str, operation: str):
        return self._session._execute(plan, action, operation)

    def collect(self) -> list[Row]:
        names, rows = self._run(self._plan, "collect", _op("collect"))
        return make_rows(names, rows)

    def take(self, num: int) -> list[Row]:
        names, rows = self._run(P.Limit(self._plan, num), "take", _op("take"))
        return make_rows(names, rows)

    def head(self, n: int | None = None):
        if n is None:
            rows = self.take(1)
            return rows[0] if rows else None
        return self.take(n)

    def first(self) -> Row | None:
        return self.head()

    def count(self) -> int:
        attr = E.Attribute(E.new_id(), "count", LongType(), False)
        _, rows = self._run(P.CountRows(self._plan, attr), "count", _op("count"))
        return int(rows[0][0])

    def show(self, n: int = 20, truncate: bool | int = True, vertical: bool = False) -> None:
        names, rows = self._run(P.Limit(self._plan, n + 1), "show", _op("show"))
        width = self._session.conf.get("mborobak.show.truncate") if truncate is True else int(truncate or 0)
        text = format_table(list(names), rows[:n], int(width), vertical)
        if len(rows) > n:
            text += f"only showing top {n} row{'s' if n != 1 else ''}"
        print(text)  # same trailing newlines as PySpark

    def toPandas(self):  # noqa: N802
        try:
            import pandas as pd
        except ImportError as exc:
            raise ImportError("toPandas() needs pandas: pip install mborobak[pandas]") from exc
        names, columns = self._session._execute(self._plan, "toPandas", _op("toPandas"), as_columns=True)
        t0 = time.perf_counter()
        data = {}
        for i, a in enumerate(self._plan.output):
            values = list(columns[i])
            has_null = any(v is None for v in values)
            dtype_name = type(a.dtype).__name__
            if dtype_name == "IntegerType":
                series = pd.Series(values, dtype="float64" if has_null else "int32")
            elif dtype_name == "LongType":
                series = pd.Series(values, dtype="float64" if has_null else "int64")
            elif dtype_name == "DoubleType":
                series = pd.Series([float("nan") if v is None else v for v in values], dtype="float64")
            elif dtype_name == "BooleanType":
                series = pd.Series(values, dtype="object" if has_null else "bool")
            elif dtype_name == "TimestampNTZType":
                series = pd.to_datetime(pd.Series(values, dtype="object")).astype("datetime64[ns]")
            else:
                series = pd.Series(values, dtype="object")
            data[i] = series
        frame = pd.DataFrame(data)
        frame.columns = list(names)
        metrics = self._session.lastExecution
        if metrics is not None:
            metrics.fetch_ms += (time.perf_counter() - t0) * 1000
        return frame

    def __repr__(self) -> str:
        return "DataFrame[" + ", ".join(f"{a.name}: {a.dtype.simpleString()}" for a in self._plan.output) + "]"


class GroupedData:
    def __init__(self, df: DataFrame, cols: list, operation: str):
        self._df = df
        self._op = operation
        self._keys: list[tuple[E.Attribute, E.Expr]] = []
        for c in cols:
            original = to_column_expr(c)
            resolved = resolve(original, df._plan, operation)
            check_no_aggregate(resolved, operation, "groupBy()")
            self._keys.append(make_attr(output_name(original), resolved, operation))
        self._keys = unique_ids(self._keys)

    def agg(self, *exprs: Any) -> DataFrame:
        op = _op("agg")
        from .sql import functions as F

        if len(exprs) == 1 and isinstance(exprs[0], dict):
            # No alias: the default names are Spark's ("avg(x)" for "mean", "count(1)" for "*").
            exprs = tuple(getattr(F, fn if fn != "mean" else "avg")(c) for c, fn in exprs[0].items())
        if not exprs:
            raise ValueError("agg() needs at least one aggregate expression")
        key_exprs = [e for _, e in self._keys]
        aggs = []
        for c in exprs:
            if not isinstance(c, Column):
                raise TypeError("agg() takes Columns or a dict")
            original = c._expr
            resolved = resolve(original, self._df._plan, op)
            check_grouped(resolved, key_exprs, op)
            name = output_name(original)
            dtype, nullable = infer(resolved.child if isinstance(resolved, E.Alias) else resolved, op)
            if isinstance(dtype, DecimalType) and dtype.precision > 38:
                dtype = DecimalType(38, dtype.scale)
            core = resolved.child if isinstance(resolved, E.Alias) else resolved
            # min/max of a string are compiled as MIN(%EXACT(x)), so their values are already exact.
            collation = "EXACT" if isinstance(core, E.AggregateCall) and core.function in ("min", "max") \
                else None
            aggs.append((E.Attribute(E.new_id(), name, dtype, nullable, collation), resolved))
        plan = P.Aggregate(self._df._plan, tuple(self._keys), tuple(aggs))
        return DataFrame(self._df._session, plan)

    def count(self) -> DataFrame:
        from .sql import functions as F

        return self.agg(F.count("*").alias("count"))

    def _simple(self, fn: str, cols: tuple[str, ...]) -> DataFrame:
        from .sql import functions as F

        if not cols:  # as in Spark: every numeric column of the input, grouping columns included
            cols = tuple(a.name for a in self._df._plan.output if a.dtype.is_numeric)
        return self.agg(*[getattr(F, fn)(c) for c in cols])

    def sum(self, *cols: str) -> DataFrame:  # noqa: A003
        return self._simple("sum", cols)

    def avg(self, *cols: str) -> DataFrame:
        return self._simple("avg", cols)

    mean = avg

    def min(self, *cols: str) -> DataFrame:  # noqa: A003
        return self._simple("min", cols)

    def max(self, *cols: str) -> DataFrame:  # noqa: A003
        return self._simple("max", cols)


class DataFrameNaFunctions:
    """``df.na``: ``fill`` and ``drop``, as ``fillna`` and ``dropna``."""

    def __init__(self, df: DataFrame):
        self._df = df

    def fill(self, value: Any, subset: Any = None) -> DataFrame:
        return self._df.fillna(value, subset)

    def drop(self, how: str = "any", thresh: int | None = None, subset: Any = None) -> DataFrame:
        return self._df.dropna(how, thresh, subset)


def _fills(value: Any, dtype, explicit: bool, op: str) -> bool:
    """Whether ``value`` fills a column of ``dtype``. Spark skips columns of another kind, except that a
    dict names its columns: there a mismatch is an error."""
    if isinstance(value, bool):
        ok = isinstance(dtype, BooleanType)
    elif isinstance(value, (int, float, decimal.Decimal)):
        ok = dtype.is_numeric
    elif isinstance(value, str):
        ok = isinstance(dtype, StringType)
    else:
        raise TypeError(f"fillna() takes an int, float, string or bool value, got {type(value).__name__}")
    if not ok and explicit:
        raise AnalysisError(f"fillna(): cannot fill a {dtype.simpleString()} column with {value!r}", operation=op)
    return ok


def _rebuild(node: P.Plan, child: P.Plan) -> P.Plan:
    """The same Filter/Sort/Limit node over a different child."""
    if isinstance(node, P.Filter):
        return P.Filter(child, node.condition)
    if isinstance(node, P.Sort):
        return P.Sort(child, node.orders)
    if isinstance(node, P.Limit):
        return P.Limit(child, node.n)
    raise TypeError(type(node).__name__)


def _restore(plan: P.Plan, output) -> P.Plan:
    """Project back to ``output`` after columns were added for resolution."""
    if output is None:
        return plan
    return P.Project(plan, tuple((a, E.AttrRef(a)) for a in output))


def _distinct_ids(left: P.Plan, right: P.Plan) -> P.Plan:
    """Self-join: give the right side's columns new ids so both sides can be told apart."""
    left_ids = {a.id for a in left.output}
    if not any(a.id in left_ids for a in right.output):
        return right
    items = tuple((E.Attribute(E.new_id(), a.name, a.dtype, a.nullable, a.collation), E.AttrRef(a))
                  for a in right.output)
    return P.Project(right, items)


# --------------------------------------------------------------------------- show()


def java_double(v: float) -> str:
    """A double as Spark (Java's Double.toString) prints it: NaN, -Infinity, 1.5, -1.0E19, 1.2345678E7."""
    if math.isnan(v):
        return "NaN"
    if math.isinf(v):
        return "Infinity" if v > 0 else "-Infinity"
    if v == 0 or 1e-3 <= abs(v) < 1e7:
        text = repr(v)
        return text if "e" not in text else f"{v:f}".rstrip("0").rstrip(".") + ".0"
    sign, digits, exponent = decimal.Decimal(repr(v)).normalize().as_tuple()
    mantissa = "".join(map(str, digits))
    return f"{'-' if sign else ''}{mantissa[0]}.{mantissa[1:] or '0'}E{len(mantissa) - 1 + exponent}"


def _cell(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return java_double(v)
    return str(v)


def format_table(names: list[str], rows: list[tuple], truncate: int, vertical: bool) -> str:
    cells = [[_cell(v) for v in r] for r in rows]
    if truncate > 0:
        def cut(s: str) -> str:
            if len(s) <= truncate:
                return s
            return s[:truncate] if truncate < 4 else s[: truncate - 3] + "..."
        cells = [[cut(c) for c in r] for r in cells]
        header = [cut(n) for n in names]
    else:
        header = list(names)
    if vertical:
        out = []
        width = max((len(n) for n in header), default=0)
        for i, r in enumerate(cells):
            out.append(f"-RECORD {i}" + "-" * 10)
            out.extend(f" {n.ljust(width)} | {c}" for n, c in zip(header, r))
        return "\n".join(out) + "\n" if out else "(0 rows)\n"
    widths = [max([3, len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(header)]
    sep = "+" + "+".join("-" * w for w in widths) + "+"

    def line(values: list[str]) -> str:
        if truncate > 0:
            return "|" + "|".join(v.rjust(w) for v, w in zip(values, widths)) + "|"
        return "|" + "|".join(v.ljust(w) for v, w in zip(values, widths)) + "|"

    out = [sep, line(header), sep] + [line(r) for r in cells] + [sep]
    return "\n".join(out) + "\n"
