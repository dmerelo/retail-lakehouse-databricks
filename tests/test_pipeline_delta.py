"""End-to-end run on real Delta tables: sample files -> bronze -> silver -> gold.

Runs in CI (RUN_DELTA_TESTS=1). Locally it needs internet access the first time
so Spark can download the Delta jars.
"""

from datetime import date, timedelta

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

from lakehouse import bronze, delta_io, pipeline, sample_data, silver
from lakehouse.config import Tables

pytestmark = pytest.mark.delta

END = date.today()
START = END - timedelta(days=6)


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    landing = tmp_path_factory.mktemp("landing")
    stats = sample_data.generate(landing, end_date=END, days=7, tx_per_day=120)
    t = Tables(catalog="", schema="retail_test")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {t.schema}")

    for source, table in [("stores", t.bronze_stores), ("products", t.bronze_products), ("sales", t.bronze_sales)]:
        delta_io.append(bronze.read_batch(spark, source, str(landing / source)), table)

    silver.process_stores_batch(spark, spark.table(t.bronze_stores), t)
    silver.process_products_batch(spark, spark.table(t.bronze_products), t)
    silver.process_sales_batch(spark, spark.table(t.bronze_sales), t)

    pipeline.refresh_dimensions(spark, t)
    pipeline.refresh_facts(spark, t, START, END)
    return t, stats


def test_silver_has_unique_lines_and_quarantines_every_broken_one(spark, lake):
    t, _ = lake
    sales = spark.table(t.silver_sales)
    assert sales.count() == sales.select("transaction_id", "line_number").distinct().count()
    q = spark.table(t.silver_quarantine).filter("entity = 'sales'")
    assert q.count() >= 6 * 7  # six broken lines per daily file
    assert sales.filter(F.col("sku").rlike(r"\s|[a-z]")).count() == 0


def test_changed_products_get_contiguous_scd2_versions(spark, lake):
    t, _ = lake
    dim = spark.table(t.dim_product).filter("product_key != -1")
    assert dim.groupBy("sku").count().filter("count >= 2").count() >= 5  # repriced + recategorized
    assert dim.filter("is_current").groupBy("sku").count().filter("count > 1").count() == 0

    w = Window.partitionBy("sku").orderBy("valid_from")
    gaps = dim.withColumn("next_from", F.lead("valid_from").over(w)).filter(
        F.col("next_from").isNotNull() & (F.col("next_from") != F.col("valid_to"))
    )
    assert gaps.count() == 0


def test_second_load_closes_versions_with_merge(spark, lake):
    t, _ = lake
    later = spark.table(t.silver_products).filter("sku = 'SKU-1001'").orderBy(F.desc("updated_at")).limit(1)
    changed = later.withColumn("list_price", (F.col("list_price") + 1).cast("decimal(12,2)")).withColumn(
        "updated_at", F.current_timestamp()
    )
    delta_io.append(changed, t.silver_products)
    pipeline.refresh_dimensions(spark, t)

    rows = spark.table(t.dim_product).filter("sku = 'SKU-1001'").orderBy("valid_from").collect()
    assert [r.is_current for r in rows][-2:] == [False, True]
    assert rows[-2].valid_to == rows[-1].valid_from


def test_store_format_change_overwrites_dim_store(spark, lake):
    t, _ = lake
    assert spark.table(t.dim_store).filter("store_code = 'LJ004'").first().store_format == "supermercado"


def test_late_sales_land_in_their_own_day(spark, lake):
    t, _ = lake
    late_day = END - timedelta(days=2)
    silver_count = spark.table(t.silver_sales).filter(F.col("sale_date") == late_day).count()
    fact_count = spark.table(t.fact_sales).filter(F.col("sale_date") == late_day).count()
    assert fact_count == silver_count > 0


def test_gold_rerun_is_idempotent(spark, lake):
    t, _ = lake
    before = spark.table(t.fact_sales).count(), spark.table(t.agg_daily_category_sales).agg(F.sum("units")).first()[0]
    pipeline.refresh_dimensions(spark, t)
    pipeline.refresh_facts(spark, t, START, END)
    after = spark.table(t.fact_sales).count(), spark.table(t.agg_daily_category_sales).agg(F.sum("units")).first()[0]
    assert before == after


def test_replaying_bronze_does_not_duplicate_silver(spark, lake):
    t, _ = lake
    before = spark.table(t.silver_sales).count()
    silver.process_sales_batch(spark, spark.table(t.bronze_sales), t)
    assert spark.table(t.silver_sales).count() == before
