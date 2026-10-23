"""Gold: star schema for BI.

    fact_sales (grain: one sales line) -> dim_date, dim_store (SCD1), dim_product (SCD2)
    agg_daily_category_sales            -> pre-aggregated for dashboards

Facts are rebuilt for a date window with ``replaceWhere``: running the same
window twice gives the same result, and late-arriving sales for recent days are
picked up by the next run's lookback.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.scd import BEGINNING, OPEN_END

UNKNOWN_KEY = -1
PRODUCT_ATTRS = ["product_name", "brand", "category", "subcategory", "pack_size_g", "list_price"]


def date_key(col: F.Column) -> F.Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    return days.select(
        date_key(F.col("date")).alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.weekofyear("date").alias("week_of_year"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_store(silver_stores: DataFrame) -> DataFrame:
    """SCD1: store attributes are overwritten; the key is stable because it is a
    hash of the business code, so existing facts never need to be re-keyed."""
    spark = silver_stores.sparkSession
    stores = silver_stores.select(
        F.xxhash64("store_code").alias("store_key"),
        "store_code",
        "store_name",
        "city",
        "state",
        "region",
        "store_format",
    )
    unknown = spark.createDataFrame([(UNKNOWN_KEY, "UNKNOWN", "Unknown", None, None, None, None)], stores.schema)
    return stores.unionByName(unknown)


def unknown_product(spark: SparkSession, dim_schema) -> DataFrame:
    row = {f.name: None for f in dim_schema.fields}
    row.update(
        product_key=UNKNOWN_KEY,
        sku="UNKNOWN",
        product_name="Unknown",
        attr_hash="unknown",
        valid_from=None,
        valid_to=None,
        is_current=True,
    )
    df = spark.createDataFrame([row], dim_schema)
    return df.withColumn("valid_from", F.to_timestamp(F.lit(BEGINNING))).withColumn(
        "valid_to", F.to_timestamp(F.lit(OPEN_END))
    )


def build_fact_sales(sales: DataFrame, dim_product: DataFrame, start: date, end: date) -> DataFrame:
    """Each line gets the product version that was valid at the moment of sale."""
    s = sales.filter(F.col("sale_date").between(F.lit(start), F.lit(end))).alias("s")
    p = (
        dim_product.filter(F.col("product_key") != UNKNOWN_KEY)
        .select("product_key", "sku", "valid_from", "valid_to")
        .alias("p")
    )
    as_of = (
        (F.col("s.sku") == F.col("p.sku"))
        & (F.col("s.sale_ts") >= F.col("p.valid_from"))
        & (F.col("s.sale_ts") < F.col("p.valid_to"))
    )
    gross = F.col("s.quantity") * F.col("s.unit_price")
    return s.join(p, as_of, "left").select(
        date_key(F.col("s.sale_date")).alias("date_key"),
        F.col("s.sale_date").alias("sale_date"),
        F.xxhash64("s.store_code").alias("store_key"),
        F.coalesce(F.col("p.product_key"), F.lit(UNKNOWN_KEY).cast("long")).alias("product_key"),
        F.col("s.transaction_id").alias("transaction_id"),
        F.col("s.line_number").alias("line_number"),
        F.col("s.sale_ts").alias("sale_ts"),
        F.col("s.channel").alias("channel"),
        F.col("s.quantity").alias("quantity"),
        F.col("s.unit_price").alias("unit_price"),
        F.col("s.discount").alias("discount"),
        gross.cast("decimal(14,2)").alias("gross_amount"),
        (gross - F.col("s.discount")).cast("decimal(14,2)").alias("net_amount"),
    )


def build_daily_category_sales(fact: DataFrame, dim_product: DataFrame, dim_store: DataFrame) -> DataFrame:
    """Category is taken from the product version at sale time, not today's catalog."""
    return (
        fact.join(dim_product.select("product_key", "category"), "product_key", "left")
        .join(dim_store.select("store_key", "region"), "store_key", "left")
        .groupBy(
            "sale_date",
            F.coalesce("category", F.lit("Unknown")).alias("category"),
            F.coalesce("region", F.lit("Unknown")).alias("region"),
            "channel",
        )
        .agg(
            F.sum("quantity").alias("units"),
            F.sum("net_amount").alias("net_revenue"),
            F.countDistinct("transaction_id").alias("transactions"),
        )
    )


def window_predicate(start: date, end: date) -> str:
    return f"sale_date >= '{start}' AND sale_date <= '{end}'"
