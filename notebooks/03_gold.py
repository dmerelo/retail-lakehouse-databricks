# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold
# MAGIC Refreshes the dimensions (dim_product as SCD2 via MERGE) and rebuilds facts and the daily
# MAGIC aggregate for the last `lookback_days`. The lookback is what absorbs sales that arrive late;
# MAGIC `replaceWhere` makes a rerun of the same window give the same result.

# COMMAND ----------

import os
import sys
from datetime import date, timedelta

sys.path.append(os.path.abspath("../src"))

from lakehouse import pipeline
from lakehouse.config import Tables

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
dbutils.widgets.text("run_date", "")  # empty = today
dbutils.widgets.text("lookback_days", "7")

t = Tables(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
end = date.fromisoformat(dbutils.widgets.get("run_date") or date.today().isoformat())
start = end - timedelta(days=int(dbutils.widgets.get("lookback_days")))
print(f"window: {start} .. {end}")

# COMMAND ----------

pipeline.refresh_dimensions(spark, t)
print(pipeline.refresh_facts(spark, t, start, end))

# COMMAND ----------

# MAGIC %md Products whose attributes changed in the period (SCD2 history)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT sku, product_name, category, subcategory, list_price, valid_from, valid_to, is_current
        FROM {t.dim_product}
        WHERE sku IN (SELECT sku FROM {t.dim_product} GROUP BY sku HAVING count(*) > 1)
        ORDER BY sku, valid_from
    """)
)
