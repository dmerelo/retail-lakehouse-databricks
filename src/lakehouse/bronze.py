"""Bronze: land raw files as-is, all columns as strings, plus ingestion metadata.

Nothing is cast or filtered here. If a source file is malformed we still want the
original values in the lake so silver can quarantine them and we can explain why.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


def _all_strings(*cols: str) -> StructType:
    return StructType([StructField(c, StringType(), True) for c in cols])


SALES_SCHEMA = _all_strings(
    "transaction_id",
    "line_number",
    "sale_ts",
    "store_code",
    "sku",
    "quantity",
    "unit_price",
    "discount",
    "channel",
)

PRODUCTS_SCHEMA = _all_strings(
    "sku",
    "product_name",
    "brand",
    "category",
    "subcategory",
    "pack_size_g",
    "list_price",
    "updated_at",
)

STORES_SCHEMA = _all_strings(
    "store_code",
    "store_name",
    "city",
    "state",
    "region",
    "store_format",
    "updated_at",
)

SOURCES = {
    "sales": ("csv", SALES_SCHEMA),
    "products": ("json", PRODUCTS_SCHEMA),
    "stores": ("csv", STORES_SCHEMA),
}


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_file", F.col("_metadata.file_path"))


def read_batch(spark: SparkSession, source: str, path: str) -> DataFrame:
    """Plain batch read, used locally and in tests."""
    fmt, schema = SOURCES[source]
    reader = spark.read.schema(schema)
    if fmt == "csv":
        reader = reader.option("header", "true")
    return add_ingestion_metadata(reader.format(fmt).load(path))


def read_stream(spark: SparkSession, source: str, path: str) -> DataFrame:
    """Auto Loader read, used on Databricks. Each new file is ingested exactly once;
    progress lives in the stream checkpoint, so re-running the job is safe."""
    fmt, schema = SOURCES[source]
    reader = spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt).schema(schema)
    if fmt == "csv":
        reader = reader.option("header", "true")
    return add_ingestion_metadata(reader.load(path))
