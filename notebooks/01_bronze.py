# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Auto Loader picks up only the files that arrived since the last run (`availableNow`),
# MAGIC appends them as strings and records when and from which file each row came.
# MAGIC
# MAGIC The scheduled job runs the same code from the wheel (`lakehouse-bronze`);
# MAGIC this notebook is for running a layer by hand.

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

m = jobs.Metrics(spark, t, run_id="manual")
try:
    jobs.run_bronze(spark, t, m)
finally:
    m.flush()
