"""Notebook compatibility scanner (mborobak.scan)."""

import json
from pathlib import Path

import pytest

from mborobak.scan import (
    NOT_SCHEDULED,
    OUT_OF_SCOPE,
    PLANNED,
    SUPPORTED,
    main,
    scan_notebook,
    scan_paths,
    scan_source,
)

SAMPLE = Path(__file__).parents[2] / "examples" / "notebooks" / "sales_analysis_sample.ipynb"


def found(report) -> dict[str, tuple[str, str | None, int]]:
    return {f.label: (f.status, f.release, f.count) for f in report.findings.values()}


def notebook(*cells: str) -> dict:
    return {"cells": [{"cell_type": "code", "source": c} for c in cells]}


def test_functions_through_module_alias_and_direct_imports():
    r = found(scan_source("""
from pyspark.sql import functions as F
import pyspark.sql.functions as G
from pyspark.sql.functions import col, split as td
df.select(F.col("a"), G.sum("b"), col("c"), td(col("d")), F.explode("e"))
"""))
    assert r["F.col"] == (SUPPORTED, None, 3)
    assert r["F.sum"] == (SUPPORTED, None, 1)
    assert r["F.split"] == (PLANNED, "0.3", 1)
    assert r["F.explode"] == (NOT_SCHEDULED, None, 1)
    assert r[".select()"] == (SUPPORTED, None, 1)


def test_star_import_and_unimported_names():
    r = found(scan_source("from pyspark.sql.functions import *\ndf.select(split('a', 'x'), regexp_replace('a', 'x', ''), lit(0))"))
    assert r["F.split"] == (PLANNED, "0.3", 1)
    assert r["F.regexp_replace"] == (SUPPORTED, None, 1)
    assert r["F.lit"][0] == SUPPORTED
    # without the import, a bare regexp_replace() is not a Spark call
    assert "F.regexp_replace" not in found(scan_source("regexp_replace(a)"))


def test_supported_status_comes_from_the_installed_package():
    from mborobak.sql import functions as F

    r = found(scan_source("from pyspark.sql import functions as F\nF.countDistinct('a'); F.mean('b')"))
    assert hasattr(F, "countDistinct") and r["F.countDistinct"][0] == SUPPORTED
    assert r["F.mean"][0] == SUPPORTED


def test_non_spark_calls_are_ignored():
    r = found(scan_source("""
import os, numpy as np
import pandas as pd
from json import dumps
path = os.path.join("a", "b")
text = ", ".join(names)
n = np.sum(values)
pdf = pd.read_csv("x.csv")
pdf.head(); pdf.count()
local = df.toPandas()
local.groupby("a").sum()
df.toPandas().head()
dumps({})
start = min(start, local['when'].min())
psdf = df.pandas_api()
psdf.astype({"a": "string"})
"""))
    assert set(r) == {".toPandas()", ".pandas_api()"}
    assert r[".pandas_api()"][0] == OUT_OF_SCOPE
    assert r[".toPandas()"][2] == 2


def test_string_filter_and_join_types():
    r = found(scan_source("""
df.filter("x > 1").where(f"y = {v}").filter(F.col("z") > 0)
a.join(b, "id", "left_semi").join(c, on="id", how="left").join(d, "k", how="anti")
"""))
    assert r[".filter(str)"] == (SUPPORTED, None, 2)  # Spark SQL expressions (0.2)
    assert r[".filter()"] == (SUPPORTED, None, 1)
    assert r[".join(how='left_semi')"] == (SUPPORTED, None, 1)
    assert r[".join(how='anti')"] == (SUPPORTED, None, 1)
    assert r[".join()"] == (SUPPORTED, None, 1)


def test_windows_including_spec_variables():
    r = found(scan_source("""
from pyspark.sql import Window
from pyspark.sql import functions as F
w = Window.partitionBy("a").orderBy("b")
w2 = w.rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("r", F.row_number().over(w))
"""))
    assert r["Window.partitionBy"] == (SUPPORTED, None, 1)
    assert r["Window.orderBy"] == (SUPPORTED, None, 1)
    assert r["Window.rowsBetween"] == (SUPPORTED, None, 1)
    assert r["Window.unboundedPreceding"] == (SUPPORTED, None, 1)
    assert r[".over()"] == (SUPPORTED, None, 1)
    assert r["F.row_number"] == (SUPPORTED, None, 1)


def test_session_reader_writer_and_platform():
    r = found(scan_source("""
spark = SparkSession.builder.appName("x").getOrCreate()
t = spark.table("s.t")
q = spark.sql("select 1")
c = spark.createDataFrame(rows)
f = spark.read.format("csv").option("header", True).load("/mnt/x")
g = spark.read.table("s.u")
t.filter(F.col("a") > 1).write.mode("overwrite").saveAsTable("s.v")
t.write.parquet("/mnt/out")
spark.catalog.tableExists("s.t")
display(t)
dbutils.fs.ls("/mnt")
t.cache(); t.na.drop()
"""))
    assert r["spark.table"] == (SUPPORTED, None, 1)
    assert r["spark.sql"][0] == SUPPORTED
    assert r["spark.createDataFrame"] == (SUPPORTED, None, 1)
    assert r["spark.read"] == (OUT_OF_SCOPE, None, 1)          # format/option/load counted once
    assert r["spark.read.table"] == (SUPPORTED, None, 1)
    assert r["df.write.saveAsTable"] == (SUPPORTED, None, 1)  # mode() is part of it
    assert r["df.write.parquet"][0] == OUT_OF_SCOPE
    assert r[".filter()"] == (SUPPORTED, None, 1)               # the call before .write still counts
    assert r["spark.catalog.tableExists"][0] == SUPPORTED
    assert r["display"][0] == OUT_OF_SCOPE
    assert r["dbutils"][0] == OUT_OF_SCOPE
    assert r[".cache()"][0] == OUT_OF_SCOPE
    assert r[".na.drop()"] == (SUPPORTED, None, 1)
    assert ".getOrCreate()" not in r and ".appName()" not in r


def test_udf_and_mllib():
    r = found(scan_source("""
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler
import dlt
from pyspark import pipelines as dp
@udf("string")
def clean(s): return s
spark.udf.register("clean", clean)
"""))
    assert r["F.udf"][0] == OUT_OF_SCOPE
    assert r["pyspark.ml"][0] == OUT_OF_SCOPE
    assert r["spark.udf"][0] == OUT_OF_SCOPE
    assert r["dlt"] == (OUT_OF_SCOPE, None, 2)


def test_notebook_cells_magics_and_locations():
    report = scan_notebook({"cells": [
        {"cell_type": "markdown", "source": "# title"},
        {"cell_type": "code", "source": ["%pip install x\n", "from pyspark.sql import functions as F\n"]},
        {"cell_type": "code", "source": "%sql\nSELECT 1"},
        {"cell_type": "code", "source": "df = (spark.table('t')\n      .select(F.col('a')))"},
        {"cell_type": "code", "source": "print 'python 2'"},
    ]}, "nb.ipynb")
    r = {f.label: f for f in report.findings.values()}
    assert r["%sql cell"].status == SUPPORTED and "spark.sql" in r["%sql cell"].note
    assert str(r["%sql cell"].locations[0]) == "nb.ipynb: cell 3, line 1"
    # imports carry over from cell to cell; a chained call reports its own line
    assert str(r["F.col"].locations[0]) == "nb.ipynb: cell 4, line 2"
    assert str(r[".select()"].locations[0]) == "nb.ipynb: cell 4, line 2"
    assert len(report.warnings) == 1 and "cell 5" in report.warnings[0]


def test_databricks_source_export():
    code = ("# Databricks notebook source\nfrom pyspark.sql import functions as F\n\n"
            "# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT 1\n\n"
            "# COMMAND ----------\n\ndf.select(F.col('a'))\n")
    r = {f.label: f for f in scan_source(code, "nb.py").findings.values()}
    assert str(r["%sql cell"].locations[0]) == "nb.py: line 6"
    assert r["F.col"].count == 1


def test_coverage_is_cumulative_by_release():
    report = scan_notebook(notebook(
        "from pyspark.sql import functions as F",
        "df.select(F.col('a'), F.split('c', 'x'), F.explode('d'))",
        "spark.createDataFrame(rows)"))
    cov = report.coverage()
    # 5 calls: select, col and createDataFrame supported; split 0.3; explode not scheduled
    assert cov["today"] == pytest.approx(3 / 5)
    assert cov["after 0.2"] == pytest.approx(3 / 5)
    assert cov["after 0.2.x"] == pytest.approx(3 / 5)
    assert cov["after 0.3"] == pytest.approx(4 / 5)


def test_sample_notebook_and_cli(capsys, tmp_path):
    report = scan_paths([SAMPLE])
    assert not report.warnings
    totals = report.totals()
    assert totals[SUPPORTED] > 0 and totals[OUT_OF_SCOPE] > 0  # nothing left planned since 0.3

    assert main([str(SAMPLE), "--format", "json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["calls"] == sum(totals.values())
    assert {"api", "status", "release", "count", "locations"} <= set(data["findings"][0])

    assert main([str(SAMPLE.parent), "--format", "markdown"]) == 0
    assert "| `F.col` |" in capsys.readouterr().out

    with pytest.raises(SystemExit):
        main([str(tmp_path / "missing.ipynb")])


def test_coverage_without_out_of_scope_calls():
    report = scan_source("from pyspark.sql import functions as F\ndisplay(df.select(F.split('a', 'x')))")
    # display out of scope, select supported, split planned for 0.3
    assert report.coverage()["today"] == pytest.approx(1 / 3)
    assert report.coverage(in_scope=True)["today"] == pytest.approx(1 / 2)
    assert report.coverage(in_scope=True)["after 0.3"] == pytest.approx(1)


def test_hidden_folders_and_irispark_imports(tmp_path):
    code = ("from irispark import IrisParkSession, Window\nfrom irispark.functions import col, charindex\n"
            "from irispark.ml.feature import VectorAssembler\n"
            "df.select(col('a'), charindex('x', 'y')).show()\n")
    (tmp_path / "a.py").write_text(code)
    (tmp_path / ".ipynb_checkpoints").mkdir()
    (tmp_path / ".ipynb_checkpoints" / "a-checkpoint.py").write_text(code)
    report = scan_paths([tmp_path])
    assert len(report.files) == 1
    r = {f.label: (f.status, f.count) for f in report.findings.values()}
    assert r["F.col"] == (SUPPORTED, 1)
    assert r["F.charindex"][0] == NOT_SCHEDULED  # an IRISpark function
    assert r["pyspark.ml"][0] == OUT_OF_SCOPE
