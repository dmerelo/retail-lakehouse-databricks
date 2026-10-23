"""Silver: typed, standardized, deduplicated and validated records.

Casts use ``try_cast`` / ``try_to_timestamp`` so a bad value becomes null and is
caught by the quality rules, instead of failing the whole batch (Databricks
runs with ANSI mode on, where a plain cast of "abc" to int raises).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse import delta_io
from lakehouse.config import Tables
from lakehouse.quality import Rule, split_valid_invalid

TS_FORMAT = "yyyy-MM-dd HH:mm:ss"
MONEY = "decimal(12,2)"


def _try_cast(col: str, to: str) -> F.Column:
    return F.expr(f"try_cast(trim({col}) as {to})")


def _code(col: str) -> F.Column:
    """Business codes arrive as ' sku-001 ', 'SKU-001', 'sku-001' or ''."""
    return F.nullif(F.upper(F.trim(F.col(col))), F.lit(""))


def _text(col: str) -> F.Column:
    return F.nullif(F.regexp_replace(F.trim(F.col(col)), r"\s+", " "), F.lit(""))


def dedupe_latest(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_by])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def to_quarantine(invalid: DataFrame, entity: str) -> DataFrame:
    """One row per rejected record. ``row_hash`` identifies the same record from
    the same file, so replaying a file does not quarantine it twice; the same
    record re-sent in another file is a new occurrence and is kept."""
    payload_cols = [c for c in invalid.columns if not c.startswith("_")]
    payload = F.to_json(F.struct(*payload_cols))
    return invalid.select(
        F.sha2(F.concat_ws("||", F.lit(entity), F.col("_source_file"), payload), 256).alias("row_hash"),
        F.lit(entity).alias("entity"),
        payload.alias("payload"),
        F.col("_failed_rules").alias("failed_rules"),
        F.col("_source_file").alias("source_file"),
        F.col("_quarantined_at").alias("quarantined_at"),
    ).dropDuplicates(["row_hash"])


# --- sales -------------------------------------------------------------------


def clean_sales(df: DataFrame) -> DataFrame:
    sale_ts = F.try_to_timestamp(F.trim("sale_ts"), F.lit(TS_FORMAT))
    return df.select(
        _text("transaction_id").alias("transaction_id"),
        _try_cast("line_number", "int").alias("line_number"),
        sale_ts.alias("sale_ts"),
        F.to_date(sale_ts).alias("sale_date"),
        _code("store_code").alias("store_code"),
        _code("sku").alias("sku"),
        _try_cast("quantity", "int").alias("quantity"),
        _try_cast("unit_price", MONEY).alias("unit_price"),
        F.coalesce(_try_cast("discount", MONEY), F.lit(0).cast(MONEY)).alias("discount"),
        F.lower(F.coalesce(_text("channel"), F.lit("store"))).alias("channel"),
        "_ingested_at",
        "_source_file",
    )


def sales_rules() -> list[Rule]:
    return [
        Rule("transaction_id_present", F.col("transaction_id").isNotNull()),
        Rule("line_number_valid", F.col("line_number") >= 1),
        Rule("sale_ts_parsed", F.col("sale_ts").isNotNull()),
        Rule("sale_not_in_future", F.col("sale_date") <= F.current_date()),
        Rule("sku_present", F.col("sku").isNotNull()),
        Rule("quantity_positive", F.col("quantity") > 0),
        Rule("unit_price_non_negative", F.col("unit_price") >= 0),
        Rule("discount_not_above_gross", F.col("discount") <= F.col("quantity") * F.col("unit_price")),
        Rule("channel_known", F.col("channel").isin("store", "ecommerce", "delivery_app")),
        Rule("store_known", F.col("_store_known")),
    ]


def validate_sales(cleaned: DataFrame, known_stores: DataFrame) -> tuple[DataFrame, DataFrame]:
    stores = known_stores.select("store_code").distinct().withColumn("_store_known", F.lit(True))
    flagged = cleaned.join(F.broadcast(stores), "store_code", "left").fillna({"_store_known": False})
    valid, invalid = split_valid_invalid(flagged, sales_rules())
    valid = dedupe_latest(valid.drop("_store_known"), ["transaction_id", "line_number"], ["_ingested_at"])
    return valid, invalid.drop("_store_known")


def process_sales_batch(spark: SparkSession, batch: DataFrame, t: Tables) -> dict[str, int]:
    """Called per micro-batch (foreachBatch on Databricks) or directly in tests."""
    # No .cache() here: serverless compute does not support persisting DataFrames.
    valid, invalid = validate_sales(clean_sales(batch), spark.table(t.silver_stores))
    counts = {"valid": valid.count(), "quarantined": invalid.count()}
    delta_io.upsert(spark, valid, t.silver_sales, ["transaction_id", "line_number"], newer_than="_ingested_at")
    delta_io.insert_new(spark, to_quarantine(invalid, "sales"), t.silver_quarantine, ["row_hash"])
    return counts


# --- products ----------------------------------------------------------------


def clean_products(df: DataFrame) -> DataFrame:
    return df.select(
        _code("sku").alias("sku"),
        _text("product_name").alias("product_name"),
        F.initcap(_text("brand")).alias("brand"),
        F.initcap(_text("category")).alias("category"),
        F.initcap(_text("subcategory")).alias("subcategory"),
        _try_cast("pack_size_g", "int").alias("pack_size_g"),
        _try_cast("list_price", MONEY).alias("list_price"),
        F.try_to_timestamp(F.trim("updated_at"), F.lit(TS_FORMAT)).alias("updated_at"),
        "_ingested_at",
        "_source_file",
    )


def product_rules() -> list[Rule]:
    return [
        Rule("sku_present", F.col("sku").isNotNull()),
        Rule("category_present", F.col("category").isNotNull()),
        Rule("list_price_non_negative", F.col("list_price") >= 0),
        Rule("updated_at_parsed", F.col("updated_at").isNotNull()),
    ]


def process_products_batch(spark: SparkSession, batch: DataFrame, t: Tables) -> dict[str, int]:
    """silver_products keeps every version (sku, updated_at); gold turns them into SCD2."""
    valid, invalid = split_valid_invalid(clean_products(batch), product_rules())
    valid = dedupe_latest(valid, ["sku", "updated_at"], ["_ingested_at"])
    counts = {"valid": valid.count(), "quarantined": invalid.count()}
    delta_io.upsert(spark, valid, t.silver_products, ["sku", "updated_at"], newer_than="_ingested_at")
    delta_io.insert_new(spark, to_quarantine(invalid, "products"), t.silver_quarantine, ["row_hash"])
    return counts


# --- stores ------------------------------------------------------------------


def clean_stores(df: DataFrame) -> DataFrame:
    return df.select(
        _code("store_code").alias("store_code"),
        _text("store_name").alias("store_name"),
        F.initcap(_text("city")).alias("city"),
        _code("state").alias("state"),
        _text("region").alias("region"),
        F.lower(_text("store_format")).alias("store_format"),
        F.try_to_timestamp(F.trim("updated_at"), F.lit(TS_FORMAT)).alias("updated_at"),
        "_ingested_at",
        "_source_file",
    )


def store_rules() -> list[Rule]:
    return [
        Rule("store_code_present", F.col("store_code").isNotNull()),
        Rule("state_is_uf", F.length("state") == 2),
        Rule("updated_at_parsed", F.col("updated_at").isNotNull()),
    ]


def process_stores_batch(spark: SparkSession, batch: DataFrame, t: Tables) -> dict[str, int]:
    valid, invalid = split_valid_invalid(clean_stores(batch), store_rules())
    valid = dedupe_latest(valid, ["store_code"], ["updated_at", "_ingested_at"])
    counts = {"valid": valid.count(), "quarantined": invalid.count()}
    delta_io.upsert(spark, valid, t.silver_stores, ["store_code"], newer_than="updated_at")
    delta_io.insert_new(spark, to_quarantine(invalid, "stores"), t.silver_quarantine, ["row_hash"])
    return counts
