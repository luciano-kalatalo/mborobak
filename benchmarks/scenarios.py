"""Benchmark scenarios: one DataFrame pipeline and the SQL a developer would write by hand.

Each scenario has two hand-written versions:

* ``native``: idiomatic IRIS SQL with IRIS's default (case-insensitive) string semantics;
* ``native_exact``: the same with ``%EXACT`` wherever Mborobak adds it for PySpark semantics (D10).

So ``mborobak_iris`` vs ``native`` measures Mborobak's own overhead, ``mborobak`` vs ``native_exact`` the
same under PySpark semantics, and ``native_exact`` vs ``native`` what ``%EXACT`` costs (compiler spec O4).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class Scenario:
    name: str
    family: str  # filter, projection, aggregation, join, sort, chain
    build: Callable  # (F, orders, customers) -> DataFrame
    action: str  # "count" or "collect"
    native: str  # {o} = Orders table, {c} = Customers table
    native_exact: str
    params: list = field(default_factory=list)
    ordered: bool = False


SCENARIOS = [
    Scenario(
        "filter_count_string", "filter",
        lambda F, o, c: o.filter(F.col("status") == "PAID"),
        "count",
        "SELECT COUNT(*) FROM {o} WHERE status = ?",
        "SELECT COUNT(*) FROM {o} WHERE %EXACT(status) = ?",
        ["PAID"],
    ),
    Scenario(
        "lookup_count_indexed_string", "filter",
        lambda F, o, c: c.filter(F.col("country") == "BR"),
        "count",
        "SELECT COUNT(*) FROM {c} WHERE country = ?",
        "SELECT COUNT(*) FROM {c} WHERE %EXACT(country) = ?",
        ["BR"],
    ),
    Scenario(
        "filter_fetch_numeric", "filter",
        lambda F, o, c: o.filter(F.col("amount") > 900).select("id", "amount"),
        "collect",
        "SELECT id, amount FROM {o} WHERE amount > ?",
        "SELECT id, amount FROM {o} WHERE amount > ?",
        [900],
    ),
    Scenario(
        "projection_fetch", "projection",
        lambda F, o, c: o.select("id", (F.col("amount") * 2).alias("double_amount"), "status", "created"),
        "collect",
        "SELECT id, amount * ? AS double_amount, status, created FROM {o}",
        "SELECT id, amount * ? AS double_amount, status, created FROM {o}",
        [2],
    ),
    Scenario(
        "agg_by_int", "aggregation",
        lambda F, o, c: o.groupBy("customer_id").agg(F.sum("amount").alias("revenue"), F.count("*").alias("n")),
        "collect",
        "SELECT customer_id, SUM(amount) AS revenue, COUNT(*) AS n FROM {o} GROUP BY customer_id",
        "SELECT customer_id, SUM(amount) AS revenue, COUNT(*) AS n FROM {o} GROUP BY customer_id",
    ),
    Scenario(
        "agg_by_string", "aggregation",
        lambda F, o, c: o.groupBy("status").agg(F.sum("amount").alias("revenue"), F.avg("score").alias("s")),
        "collect",
        "SELECT status, SUM(amount) AS revenue, AVG(score) AS s FROM {o} GROUP BY status",
        "SELECT %EXACT(status) AS status, SUM(amount) AS revenue, AVG(score) AS s FROM {o} "
        "GROUP BY %EXACT(status)",
    ),
    Scenario(
        "join_agg_sort", "join",
        lambda F, o, c: o.join(c, o["customer_id"] == c["id"]).groupBy(c["country"])
        .agg(F.sum("amount").alias("revenue")).orderBy(F.col("revenue").desc()),
        "collect",
        "SELECT c.country, SUM(o.amount) AS revenue FROM {o} o JOIN {c} c ON o.customer_id = c.id "
        "GROUP BY c.country ORDER BY revenue DESC",
        "SELECT %EXACT(c.country) AS country, SUM(o.amount) AS revenue FROM {o} o JOIN {c} c "
        "ON o.customer_id = c.id GROUP BY %EXACT(c.country) ORDER BY revenue DESC",
        ordered=True,
    ),
    Scenario(
        "sort_limit_double", "sort",
        lambda F, o, c: o.orderBy(F.col("score").desc()).limit(100).select("id", "score"),
        "collect",
        "SELECT TOP ? id, score FROM {o} ORDER BY score DESC",
        "SELECT TOP ? id, score FROM {o} ORDER BY score DESC",
        [100],
        ordered=True,
    ),
    Scenario(
        "chain_scope_example", "chain",
        lambda F, o, c: o.filter(F.col("status") == "PAID").groupBy("customer_id")
        .agg(F.sum("amount").alias("revenue")).orderBy(F.col("revenue").desc()).limit(20),
        "collect",
        "SELECT TOP ? customer_id, SUM(amount) AS revenue FROM {o} WHERE status = ? GROUP BY customer_id "
        "ORDER BY revenue DESC",
        "SELECT TOP ? customer_id, SUM(amount) AS revenue FROM {o} WHERE %EXACT(status) = ? "
        "GROUP BY customer_id ORDER BY revenue DESC",
        [20, "PAID"],
        ordered=True,
    ),
]
