# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Reads only the new bronze rows (Delta streaming source + checkpoint) and, per micro-batch:
# MAGIC cleans and casts, validates, sends broken rows to `silver_quarantine` and MERGEs the rest.
# MAGIC Stores and products run first because sales are validated against the store master.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from lakehouse import jobs
from lakehouse.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
t = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

# COMMAND ----------

jobs.run_silver(spark, t, jobs.Metrics(spark, t, run_id="manual"))

# COMMAND ----------

# MAGIC %md What was rejected, and why

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT entity, rule, count(*) AS rows
        FROM {t.silver_quarantine} LATERAL VIEW explode(failed_rules) r AS rule
        GROUP BY entity, rule
        ORDER BY entity, rows DESC
    """)
)
