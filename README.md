# Mborobak

Lazy, PySpark-style DataFrames for InterSystems IRIS. You write DataFrame code; Mborobak builds a plan and compiles it to one IRIS SQL statement, which runs inside IRIS when you call an action.

> **Status: Phase 2 prototype.** Implemented: `table`, `select`, `filter`, `withColumn`, `groupBy().agg()`, `orderBy`, `limit`, `distinct`, `join` (inner, left, right, full, cross; by name or condition), `alias`, `union`, and the actions `collect`, `count`, `take`, `first`, `show`, `toPandas`. Not yet: `session.sql()`. See [docs/](docs/) for the scope and the three control documents.

## Install (development)

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Example

```python
import os
from mborobak import MborobakSession
from mborobak.sql import functions as F

session = (
    MborobakSession.builder
    .config("host", os.environ["IRIS_HOST"])
    .config("port", int(os.environ.get("IRIS_PORT", 1972)))
    .config("namespace", os.environ.get("IRIS_NAMESPACE", "USER"))
    .config("user", os.environ["IRIS_USERNAME"])
    .config("password", os.environ["IRIS_PASSWORD"])
    .getOrCreate()
)

result = (
    session.table("Sales")
    .filter(F.col("status") == "PAID")
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("revenue"))
    .orderBy(F.col("revenue").desc())
)

result.explain()   # plan, SQL and parameters, without running anything
result.show()      # runs one query in IRIS
```

## Tests

```bash
.venv/bin/pytest tests/unit                 # no IRIS needed
.venv/bin/pytest tests/integration          # needs IRIS_HOST, IRIS_PORT, IRIS_NAMESPACE, IRIS_USERNAME, IRIS_PASSWORD
```

## License

MIT
