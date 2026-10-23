"""Gold orchestration shared by the Databricks notebook and the end-to-end test."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import delta_io, gold, quality
from lakehouse.config import Tables
from lakehouse.scd import apply_scd2, stage_scd2

DIM_DATE_RANGE = (date(2024, 1, 1), date(2027, 12, 31))
MAX_UNKNOWN_PRODUCT_RATIO = 0.01


def refresh_dimensions(spark: SparkSession, t: Tables) -> None:
    if not spark.catalog.tableExists(t.dim_date):
        delta_io.overwrite(gold.build_dim_date(spark, *DIM_DATE_RANGE), t.dim_date)

    delta_io.overwrite(gold.build_dim_store(spark.table(t.silver_stores)), t.dim_store)

    current = spark.table(t.dim_product) if spark.catalog.tableExists(t.dim_product) else None
    staged = stage_scd2(
        spark.table(t.silver_products),
        current,
        key="sku",
        attrs=gold.PRODUCT_ATTRS,
        effective_col="updated_at",
        surrogate="product_key",
    )
    unknown = gold.unknown_product(spark, staged.drop("merge_key").schema) if current is None else None
    apply_scd2(spark, staged, t.dim_product, key="sku", unknown_member=unknown)


def refresh_facts(spark: SparkSession, t: Tables, start: date, end: date) -> dict[str, int]:
    dim_product = spark.table(t.dim_product)
    fact = gold.build_fact_sales(spark.table(t.silver_sales), dim_product, start, end)

    quality.assert_unique(fact, ["transaction_id", "line_number"], t.fact_sales)
    total = fact.count()
    unknown = fact.filter(F.col("product_key") == gold.UNKNOWN_KEY).count()
    quality.assert_max_ratio(unknown, total, MAX_UNKNOWN_PRODUCT_RATIO, "fact_sales lines without product")

    predicate = gold.window_predicate(start, end)
    delta_io.replace_where(spark, fact, t.fact_sales, predicate, partition_by=["sale_date"])

    agg = gold.build_daily_category_sales(
        spark.table(t.fact_sales).filter(predicate), dim_product, spark.table(t.dim_store)
    )
    delta_io.replace_where(spark, agg, t.agg_daily_category_sales, predicate, partition_by=["sale_date"])
    return {"fact_lines": total, "unknown_product_lines": unknown}
