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

from lakehouse import silver
from lakehouse.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
t = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
checkpoints = f"/Volumes/{t.catalog}/{t.schema}/checkpoints"

# COMMAND ----------


def run(entity: str, source_table: str, process) -> None:
    def per_batch(batch_df, batch_id):
        counts = process(batch_df.sparkSession, batch_df, t)
        print(f"{entity} batch {batch_id}: {counts}")

    (
        spark.readStream.table(source_table)
        .writeStream.foreachBatch(per_batch)
        .option("checkpointLocation", f"{checkpoints}/silver_{entity}")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


run("stores", t.bronze_stores, silver.process_stores_batch)
run("products", t.bronze_products, silver.process_products_batch)
run("sales", t.bronze_sales, silver.process_sales_batch)

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
