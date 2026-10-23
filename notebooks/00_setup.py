# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the schema and the volumes, then writes a sample of raw files into the landing volume.
# MAGIC Run once before the job (and again with a later `end_date` to simulate new days arriving).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
dbutils.widgets.text("days", "14")
dbutils.widgets.text("end_date", "")  # empty = today

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.checkpoints")

# COMMAND ----------

import os
import sys
from datetime import date

sys.path.append(os.path.abspath("../src"))

from lakehouse import sample_data

end_date = date.fromisoformat(dbutils.widgets.get("end_date") or date.today().isoformat())
stats = sample_data.generate(
    f"/Volumes/{catalog}/{schema}/landing",
    end_date=end_date,
    days=int(dbutils.widgets.get("days")),
)
print(stats)

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/landing/sales"))
