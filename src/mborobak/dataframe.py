"""``DataFrame`` and ``GroupedData`` (API contract §4)."""

from __future__ import annotations

import datetime
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
    check_no_window,
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
    from .writer import DataFrameWriter

__all__ = ["DataFrame", "GroupedData"]

_NO_VALUE = object()  # replace(): value not given

_LATER = {
    "toLocalIterator": "P1", "createOrReplaceTempView": "P1",
}

_JOIN_TYPES = {
    "inner": "inner",
    "left": "left", "leftouter": "left", "left_outer": "left",
    "right": "right", "rightouter": "right", "right_outer": "right",
    "full": "full", "fullouter": "full", "full_outer": "full", "outer": "full",
    "cross": "cross",
    "semi": "semi", "leftsemi": "semi", "left_semi": "semi",
    "anti": "anti", "leftanti": "anti", "left_anti": "anti",
}
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

    def selectExpr(self, *expr: str) -> "DataFrame":  # noqa: N802
        """``select`` of Spark SQL expressions, each with an optional alias: ``selectExpr("a + 1 AS b")``."""
        from .sqlexpr import parse_named

        exprs = _flatten(expr)
        if not all(isinstance(e, str) for e in exprs):
            raise TypeError("selectExpr() takes SQL expression strings")
        return self.select(*[Column(parse_named(e)) for e in exprs])

    def filter(self, condition: Column) -> "DataFrame":
        op = _op("filter")
        if isinstance(condition, str):  # Spark SQL expression (API contract §4.1)
            from .sqlexpr import parse_expression

            condition = Column(parse_expression(condition))
        if not isinstance(condition, Column):
            raise TypeError("filter() takes a Column condition")
        base, (resolved,), restore = self._resolve_with_missing([condition._expr], op)
        check_no_aggregate(resolved, op, "filter(); filter after agg() instead")
        check_no_window(resolved, op, "filter()")
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

    def dropDuplicates(self, subset: Any = None) -> "DataFrame":  # noqa: N802
        """One row per distinct value of ``subset`` (all columns if None, which is ``distinct()``). As in
        Spark, which row of a duplicate set is kept is not defined."""
        op = _op("dropDuplicates")
        if subset is None:
            return self.distinct()
        attrs = self._subset(subset, op)
        if not attrs:
            return self.distinct()
        output = self._plan.output
        keys = tuple(E.AttrRef(a) for a in attrs)
        rn = E.Attribute(E.new_id(), "mborobak_rn", IntegerType(), False)
        number = E.WindowExpr(E.WindowFunc("row_number"), keys, tuple(E.SortOrder(k, True, None) for k in keys))
        numbered = P.Project(self._plan, tuple([(a, E.AttrRef(a)) for a in output] + [(rn, number)]))
        first = P.Filter(numbered, E.Comparison("=", E.AttrRef(rn), E.Literal(1, IntegerType())))
        return self._with(P.Project(first, tuple((a, E.AttrRef(a)) for a in output)))

    drop_duplicates = dropDuplicates

    @property
    def na(self) -> "DataFrameNaFunctions":
        return DataFrameNaFunctions(self)

    def replace(self, to_replace: Any, value: Any = _NO_VALUE, subset: Any = None) -> "DataFrame":
        """Spark's ``replace``: values equal to ``to_replace`` (a value, a list, or a dict of old to new)
        become ``value``, in the columns of the same kind (numbers, strings or booleans)."""
        from .sql.functions import lit, when

        op = _op("replace")
        if isinstance(to_replace, dict):
            if value is not _NO_VALUE and value is not None:
                raise ValueError("replace(): with a dict, leave value out")
            pairs = list(to_replace.items())
        else:
            if value is _NO_VALUE:
                raise TypeError("replace() needs a value when to_replace is not a dict")
            olds = list(to_replace) if isinstance(to_replace, (list, tuple)) else [to_replace]
            news = list(value) if isinstance(value, (list, tuple)) else [value] * len(olds)
            if len(olds) != len(news):
                raise ValueError(f"replace(): {len(olds)} values to replace and {len(news)} replacements")
            pairs = list(zip(olds, news))

        def kind(v: Any) -> str:
            if isinstance(v, bool):
                return "bool"
            if isinstance(v, (int, float, decimal.Decimal)):
                return "number"
            if isinstance(v, str):
                return "string"
            raise TypeError(f"replace(): values must be numbers, strings or booleans, got {type(v).__name__}")

        kinds = {kind(o) for o, _ in pairs} | {kind(n) for _, n in pairs if n is not None}
        if len(kinds) > 1:
            raise ValueError("replace(): mixed type replacements are not supported, as in Spark")
        wanted = kinds.pop()
        targets = self._subset(subset, op) if subset is not None else list(self._plan.output)

        def matches(a: E.Attribute) -> bool:
            if wanted == "bool":
                return isinstance(a.dtype, BooleanType)
            if wanted == "string":
                return isinstance(a.dtype, StringType)
            return a.dtype.is_numeric

        mapping: dict[str, Column] = {}
        for a in targets:
            if not matches(a):
                continue
            column = Column(E.AttrRef(a))
            branches = None
            for old, new in pairs:
                target = lit(new).cast(a.dtype) if new is not None else lit(None).cast(a.dtype)
                branches = when(column == old, target) if branches is None else branches.when(column == old, target)
            mapping[a.name] = branches.otherwise(column)
        return self._with_columns(mapping, op) if mapping else self

    def transform(self, func: Any, *args: Any, **kwargs: Any) -> "DataFrame":
        """``func(self, *args, **kwargs)``, for chaining custom steps (``df.transform(clean).transform(enrich)``)."""
        result = func(self, *args, **kwargs)
        if not isinstance(result, DataFrame):
            raise TypeError(f"transform(): the function returned {type(result).__name__}, not a DataFrame")
        return result

    def toDF(self, *cols: str) -> "DataFrame":  # noqa: N802
        names = _flatten(cols)
        if len(names) != len(self._plan.output):
            raise ValueError(f"toDF() needs {len(self._plan.output)} names, got {len(names)}")
        return self.select(*[Column(E.Alias(E.AttrRef(a), n)) for a, n in zip(self._plan.output, names)])

    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802
        """A name for this DataFrame in the session: ``session.table(name)`` and Spark SQL in ``sql()`` find it.
        Nothing is created in IRIS."""
        self._session._temp_views[name.lower()] = self

    def createTempView(self, name: str) -> None:  # noqa: N802
        if name.lower() in self._session._temp_views:
            raise AnalysisError(f"temporary view {name!r} already exists; use createOrReplaceTempView",
                                operation=_op("createTempView"))
        self.createOrReplaceTempView(name)

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
            check_no_window(r, op, "orderBy()")
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

    # ---------------------------------------------------------------- sampling (compiler spec §7.20)
    def sample(self, withReplacement: Any = None, fraction: Any = None, seed: int | None = None) -> "DataFrame":  # noqa: N803
        """Each row with probability ``fraction``: ``sample(0.1)``, ``sample(fraction=0.1, seed=42)``,
        ``sample(False, 0.1, 42)``. The same seed gives the same rows every time; they differ from Spark's."""
        if isinstance(withReplacement, (int, float)) and not isinstance(withReplacement, bool):
            if fraction is not None:  # sample(0.1, 42): PySpark reads the second argument as the seed
                seed = fraction
            withReplacement, fraction = None, withReplacement
        if withReplacement is not None and not isinstance(withReplacement, bool):
            raise TypeError("sample() takes withReplacement as a bool")
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
            raise TypeError("sample() takes fraction as a float")
        if withReplacement:
            raise UnsupportedOperationError("sample(withReplacement=True) is not supported; sample without "
                                            "replacement, or repeat rows with a join", operation=_op("sample"))
        fraction = float(fraction)
        if not 0.0 <= fraction <= 1.0:
            raise ValueError(f"sample() fraction must be between 0 and 1, got {fraction}")
        from .sql import functions as F

        return self.filter(F.rand(_seed(seed, "sample")) < F.lit(fraction))

    def randomSplit(self, weights: list[float], seed: int | None = None) -> list["DataFrame"]:  # noqa: N802
        """DataFrames that split the rows by ``weights`` (normalized): every row lands in exactly one of them, the
        same one every time for a given seed."""
        from .sql import functions as F

        if not isinstance(weights, (list, tuple)) or not weights:
            raise TypeError("randomSplit() takes a list of weights")
        for w in weights:
            if isinstance(w, bool) or not isinstance(w, (int, float)):
                raise TypeError("randomSplit() weights must be numbers")
            if w < 0:
                raise ValueError(f"randomSplit() weights must be positive, got {w}")
        total = float(sum(weights))
        if total <= 0:
            raise ValueError("randomSplit() weights must add up to more than 0")
        u = F.rand(_seed(seed, "randomSplit"))
        bounds, acc = [0.0], 0.0
        for w in weights:
            acc += w
            bounds.append(acc / total)
        bounds[-1] = 1.0
        return [self.filter((u >= F.lit(lo)) & (u < F.lit(hi))) for lo, hi in zip(bounds, bounds[1:])]

    def sampleBy(self, col: Any, fractions: dict, seed: int | None = None) -> "DataFrame":  # noqa: N802
        """Stratified sample: rows whose ``col`` is a key of ``fractions`` with that key's probability, other rows
        never."""
        from .sql import functions as F

        if isinstance(col, str):
            col = F.col(col)
        if not isinstance(col, Column):
            raise TypeError("sampleBy() takes a column name or a Column")
        if not isinstance(fractions, dict):
            raise TypeError("sampleBy() takes fractions as a dict")
        for k, v in fractions.items():
            if not isinstance(k, (float, int, str)) or isinstance(k, bool):
                raise TypeError(f"sampleBy() keys must be float, int or str, got {type(k).__name__}")
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 <= v <= 1:
                raise ValueError(f"sampleBy() fractions must be between 0 and 1, got {v!r}")
        u = F.rand(_seed(seed, "sampleBy"))
        if not fractions:
            return self.filter(F.lit(False))
        chosen = None
        for k, v in fractions.items():
            chosen = (F.when(col == F.lit(k), F.lit(float(v))) if chosen is None
                      else chosen.when(col == F.lit(k), F.lit(float(v))))
        return self.filter(u < chosen.otherwise(F.lit(0.0)))

    @property
    def stat(self) -> "DataFrameStatFunctions":
        return DataFrameStatFunctions(self)

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
        # Conditions see both sides, also for semi and anti joins, whose output is the left side only.
        draft = P.Join(left, right, "inner" if join_type in ("semi", "anti") else join_type, None)
        resolved = [resolve(c._expr, draft, op) for c in conditions]
        cond = resolved[0]
        for c in resolved[1:]:
            cond = E.And(cond, c)
        check_no_aggregate(cond, op, "a join condition")
        check_no_window(cond, op, "a join condition")
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
        if join_type in ("semi", "anti"):  # the left columns, keys first, as Spark orders them
            keys = [la for la, _ in pairs]
            key_ids = {a.id for a in keys}
            items = [(a, E.AttrRef(a)) for a in keys] + \
                [(a, E.AttrRef(a)) for a in joined.output if a.id not in key_ids]
            return self._with(P.Project(joined, tuple(items)))
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

    def unionByName(self, other: "DataFrame", allowMissingColumns: bool = False) -> "DataFrame":  # noqa: N802, N803
        """``union`` with columns matched by name (case-insensitively, as Spark does). With
        ``allowMissingColumns``, a column one side lacks is NULL there; the right side's extra columns come
        last."""
        op = _op("unionByName")
        if not isinstance(other, DataFrame):
            raise TypeError("unionByName() takes a DataFrame")

        def by_name(df: "DataFrame", side: str) -> dict[str, E.Attribute]:
            out: dict[str, E.Attribute] = {}
            for a in df._plan.output:
                if a.name.lower() in out:
                    raise AnalysisError(f"unionByName(): the {side} DataFrame has two columns named {a.name!r}",
                                        operation=op)
                out[a.name.lower()] = a
            return out

        left, right = by_name(self, "left"), by_name(other, "right")
        missing_right = [a.name for k, a in left.items() if k not in right]
        missing_left = [a for k, a in right.items() if k not in left]
        if not allowMissingColumns and (missing_right or missing_left):
            raise AnalysisError(
                f"unionByName(): columns {missing_right or [a.name for a in missing_left]} are only in the "
                f"{'left' if missing_right else 'right'} DataFrame; pass allowMissingColumns=True to fill them "
                "with NULL", operation=op)

        def null(dtype) -> E.Expr:
            return E.Cast(E.Literal(None, NullType()), dtype)

        left_items = [Column(E.AttrRef(a)) for a in left.values()]
        left_items += [Column(E.Alias(null(a.dtype), a.name)) for a in missing_left]
        right_items = [Column(E.Alias(E.AttrRef(right[k]), a.name)) if k in right
                       else Column(E.Alias(null(a.dtype), a.name)) for k, a in left.items()]
        right_items += [Column(E.AttrRef(a)) for a in missing_left]
        first = self.select(*left_items) if missing_left else self
        return first.union(other.select(*right_items))

    # ---------------------------------------------------------------- writing
    @property
    def write(self) -> "DataFrameWriter":
        """Writes the DataFrame to an IRIS table: ``df.write.mode("overwrite").saveAsTable("Sales.Daily")``."""
        from .writer import DataFrameWriter

        return DataFrameWriter(self)

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

    def isEmpty(self) -> bool:  # noqa: N802
        return not self.take(1)

    def tail(self, num: int) -> list[Row]:
        """The last ``num`` rows. Without an IRIS ``OFFSET`` from the end, this fetches every row."""
        return self.collect()[-num:] if num > 0 else []

    def describe(self, *cols: Any) -> "DataFrame":
        """count, mean, stddev, min and max of numeric and string columns, as strings (Spark's format)."""
        return self._summary(["count", "mean", "stddev", "min", "max"], _flatten(cols), _op("describe"))

    def summary(self, *statistics: str) -> "DataFrame":
        """describe() plus approximate quartiles, or the given statistics: count, mean, stddev, min, max,
        count_distinct, approx_count_distinct and percentages such as "33%"."""
        stats = list(_flatten(statistics)) or ["count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]
        return self._summary(stats, [], _op("summary"))

    def _summary(self, stats: list[str], cols: list, op: str) -> "DataFrame":
        """One aggregate query per 20 columns, formatted in Python as Spark prints values, returned as a
        DataFrame of strings (compiler spec §7.18). The quartiles are Spark's percentile_approx: the
        smallest value with at least p of the rows, exact here."""
        import decimal as _decimal

        from .sql import functions as F

        if cols:
            attrs = [find_attr(c, self._plan.output, op) if isinstance(c, str) else
                     resolve(to_column_expr(c), self._plan, op) for c in cols]
            attrs = [a if isinstance(a, E.Attribute) else a.attr for a in attrs]
        else:
            attrs = [a for a in self._plan.output if a.dtype.is_numeric or isinstance(a.dtype, StringType)]
        for s in stats:
            if s not in ("count", "mean", "stddev", "min", "max", "count_distinct", "approx_count_distinct") and \
                    not (s.endswith("%") and s[:-1].replace(".", "", 1).isdigit()):
                raise ValueError(f"summary(): unknown statistic {s!r}")

        def stat(a: E.Attribute, s: str) -> Column:
            column = Column(E.AttrRef(a))
            number = column if a.dtype.is_numeric else column.try_cast("double")
            if s == "count":
                return F.count(column)
            if s == "mean":
                return F.avg(number)
            if s == "stddev":
                return F.stddev(number)
            if s in ("min", "max"):
                extreme = F.min(column) if s == "min" else F.max(column)
                if s == "max" and isinstance(a.dtype, DoubleType):  # Spark's max is NaN when one is; IRIS skips it
                    return F.when(F.sum(F.isnan(column).cast("int")) > 0, F.lit(float("nan"))).otherwise(extreme)
                return extreme
            if s in ("count_distinct", "approx_count_distinct"):
                return F.countDistinct(column)
            p = _decimal.Decimal(s[:-1]) / 100
            return Column(E.AggregateCall(f"percentile_approx:{p}", number._expr))

        values: dict[tuple[int, str], Any] = {}
        for start in range(0, len(attrs), 20):
            chunk = attrs[start:start + 20]
            items = [stat(a, s).alias(f"__mborobak_{i}_{k}") for i, a in enumerate(chunk) for k, s in enumerate(stats)]
            if not items:
                continue
            row = self.groupBy().agg(*items).collect()[0]
            for i, a in enumerate(chunk):
                for k, s in enumerate(stats):
                    values[(start + i, s)] = row[i * len(stats) + k]

        def text(v: Any) -> str | None:
            if v is None:
                return None
            if isinstance(v, bool):
                return "true" if v else "false"
            if isinstance(v, float):
                return java_double(v)
            if isinstance(v, (datetime.date, datetime.datetime)):
                return v.isoformat(sep=" ") if isinstance(v, datetime.datetime) else v.isoformat()
            return str(v)

        rows = [tuple([s] + [text(values.get((i, s))) for i in range(len(attrs))]) for s in stats]
        schema = StructType([StructField("summary", StringType())] + [StructField(a.name, StringType()) for a in attrs])
        return self._session.createDataFrame(rows, schema)

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
            import numpy as np
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
            if dtype_name in ("IntegerType", "LongType"):
                if has_null:
                    series = pd.Series(values, dtype="float64")
                else:  # numpy reads the ints in C, about 4x faster than pd.Series(list)
                    series = pd.Series(np.fromiter(values, dtype="int32" if dtype_name == "IntegerType" else "int64",
                                                   count=len(values)))
            elif dtype_name == "DoubleType":
                series = pd.Series(np.fromiter((float("nan") if v is None else v for v in values) if has_null
                                               else values, dtype="float64", count=len(values)))
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
            check_no_window(resolved, operation, "groupBy()")
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
        if any(isinstance(c, Column) and _has_percentile(c._expr) for c in exprs):
            return self._percentile_agg(exprs, op)
        return self._aggregate(self._df, exprs, op)

    def _aggregate(self, source: DataFrame, exprs: tuple, op: str, names: list[str] | None = None) -> DataFrame:
        key_exprs = [e for _, e in self._keys]
        aggs = []
        for i, c in enumerate(exprs):
            if not isinstance(c, Column):
                raise TypeError("agg() takes Columns or a dict")
            original = c._expr
            resolved = resolve(original, source._plan, op)
            check_no_window(resolved, op, "agg(); add them with withColumn() after agg()")
            check_grouped(resolved, key_exprs, op)
            name = names[i] if names is not None else output_name(original)
            dtype, nullable = infer(resolved.child if isinstance(resolved, E.Alias) else resolved, op)
            if isinstance(dtype, DecimalType) and dtype.precision > 38:
                dtype = DecimalType(38, dtype.scale)
            core = resolved.child if isinstance(resolved, E.Alias) else resolved
            # min/max of a string are compiled as MIN(%EXACT(x)), so their values are already exact.
            collation = "EXACT" if isinstance(core, E.AggregateCall) and core.function in ("min", "max") \
                else None
            aggs.append((E.Attribute(E.new_id(), name, dtype, nullable, collation), resolved))
        plan = P.Aggregate(source._plan, tuple(self._keys), tuple(aggs))
        return DataFrame(self._df._session, plan)

    def _percentile_agg(self, exprs: tuple, op: str) -> DataFrame:
        """median, percentile and percentile_approx (compiler spec §7.18). IRIS has no ordered aggregate, so a
        window numbers each group's non-NULL values (NaN largest, as Spark sorts) and counts them, a second
        projection marks the rows at the wanted ranks, and the aggregate sums the marked values, interpolating
        as Spark's percentile does. SUM of the one marked value, not MAX: IRIS's MAX skips NaN (FP-06)."""
        import decimal

        from .sql import functions as F
        from .sql.window import Window

        names = [output_name(c._expr) if isinstance(c, Column) else None for c in exprs]
        keys = [Column(e) for _, e in self._keys]
        children: dict = {}   # value expression -> j
        wanted: dict = {}     # (function, value expression) -> k
        resolved_exprs = []
        for c in exprs:
            if not isinstance(c, Column):
                raise TypeError("agg() takes Columns or a dict")
            r = resolve(c._expr, self._df._plan, op)
            resolved_exprs.append(r)
            for node in E.walk(r):
                if isinstance(node, E.AggregateCall) and _is_percentile(node):
                    children.setdefault(node.child, len(children))
                    wanted.setdefault((node.function, node.child), len(wanted))
        windows = []
        for child, j in children.items():
            value = Column(child) if isinstance(infer(child, op)[0], DoubleType) else Column(child).cast("double")
            ordered = Window.partitionBy(*keys).orderBy(value.asc_nulls_last())
            windows += [F.row_number().over(ordered).alias(f"__mborobak_rn{j}"),
                        F.count(value).over(Window.partitionBy(*keys)).alias(f"__mborobak_cnt{j}"),
                        value.alias(f"__mborobak_v{j}")]
        numbered = self._df.select("*", *windows)
        by_name = {a.name: a for a in numbered._plan.output}
        marks = []
        for (function, child), k in wanted.items():
            j = children[child]
            rn, cnt = (Column(E.AttrRef(by_name[f"__mborobak_{n}{j}"])) for n in ("rn", "cnt"))
            p = F.lit(decimal.Decimal("0.5" if function == "median" else function.split(":")[1]))
            if function.startswith("percentile_approx"):  # nearest rank: the smallest value with p of the rows
                x = F.greatest(cnt * p, F.lit(1))
                marks += [F.when((rn - 1 < x) & (x <= rn), 1).alias(f"__mborobak_lo{k}")]
                continue
            # Spark's percentile: 0-based position (n - 1) * p, between ranks floor(pos) + 1 and floor(pos) + 2.
            # Decimal arithmetic and no FLOOR: the row at rank floor(pos) + 1 is the one with rn - 1 <= pos < rn.
            pos = (cnt - 1) * p
            marks += [F.when((rn - 1 <= pos) & (pos < rn), 1).alias(f"__mborobak_lo{k}"),
                      F.when((rn - 2 <= pos) & (pos < rn - 1), 1).alias(f"__mborobak_hi{k}"),
                      F.when((rn - 1 <= pos) & (pos < rn), pos - (rn - 1)).alias(f"__mborobak_fr{k}")]
        marked = numbered.select("*", *marks)
        attrs = {a.name: a for a in marked._plan.output}

        def ref(name: str) -> Column:
            return Column(E.AttrRef(attrs[name]))

        def replace(node: E.Expr):
            if not (isinstance(node, E.AggregateCall) and _is_percentile(node)):
                return None
            j, k = children[node.child], wanted[(node.function, node.child)]
            v = ref(f"__mborobak_v{j}")
            v_low = F.sum(F.when(ref(f"__mborobak_lo{k}") == 1, v))
            if node.function.startswith("percentile_approx"):
                t = infer(node.child, op)[0]
                return (v_low.cast(t) if isinstance(t, (IntegerType, LongType)) else v_low)._expr
            frac = F.sum(ref(f"__mborobak_fr{k}"))
            v_high = F.sum(F.when(ref(f"__mborobak_hi{k}") == 1, v))
            return F.when(frac == 0, v_low).otherwise(v_low + frac.cast("double") * (v_high - v_low))._expr

        rewritten = tuple(Column(E.transform_down(r, replace)) for r in resolved_exprs)
        return self._aggregate(marked, rewritten, op, names)

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
    """``df.na``: ``fill``, ``drop`` and ``replace``, as ``fillna``, ``dropna`` and ``replace``."""

    def __init__(self, df: DataFrame):
        self._df = df

    def replace(self, to_replace: Any, value: Any = None, subset: Any = None) -> DataFrame:
        return self._df.replace(to_replace, value if value is not None or isinstance(to_replace, dict) else None,
                                subset)

    def fill(self, value: Any, subset: Any = None) -> DataFrame:
        return self._df.fillna(value, subset)

    def drop(self, how: str = "any", thresh: int | None = None, subset: Any = None) -> DataFrame:
        return self._df.dropna(how, thresh, subset)


class DataFrameStatFunctions:
    """``df.stat``: ``sampleBy``."""

    def __init__(self, df: DataFrame):
        self._df = df

    def sampleBy(self, col: Any, fractions: dict, seed: int | None = None) -> DataFrame:  # noqa: N802
        return self._df.sampleBy(col, fractions, seed)


def _seed(seed: Any, fn: str) -> int | None:
    if seed is None:
        from .randomness import new_seed

        return new_seed()
    if isinstance(seed, bool) or not isinstance(seed, (int, float)):
        raise TypeError(f"{fn}() takes an integer seed")
    return int(seed)


def _is_percentile(node: E.AggregateCall) -> bool:
    return node.function == "median" or node.function.startswith("percentile")


def _has_percentile(expr: E.Expr) -> bool:
    return any(isinstance(n, E.AggregateCall) and _is_percentile(n) for n in E.walk(expr))


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
