# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Auto Loader picks up only the files that arrived since the last run (`availableNow`),
# MAGIC appends them as strings and records when and from which file each row came.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))

from lakehouse import bronze
from lakehouse.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
t = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))

landing = f"/Volumes/{t.catalog}/{t.schema}/landing"
checkpoints = f"/Volumes/{t.catalog}/{t.schema}/checkpoints"

# COMMAND ----------

for source, table in [("stores", t.bronze_stores), ("products", t.bronze_products), ("sales", t.bronze_sales)]:
    (
        bronze.read_stream(spark, source, f"{landing}/{source}")
        .writeStream.option("checkpointLocation", f"{checkpoints}/bronze_{source}")
        .trigger(availableNow=True)
        .toTable(table)
        .awaitTermination()
    )
    print(f"{table}: {spark.table(table).count()} rows")
