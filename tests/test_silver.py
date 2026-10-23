from datetime import date

from pyspark.sql import functions as F

from lakehouse.bronze import SALES_SCHEMA
from lakehouse.quality import Rule, split_valid_invalid
from lakehouse.silver import clean_products, clean_sales, dedupe_latest, validate_sales

COLS = [f.name for f in SALES_SCHEMA.fields]


def bronze_sales(spark, rows, ingested="2026-10-01 10:00:00"):
    df = spark.createDataFrame(rows, SALES_SCHEMA)
    return df.withColumn("_ingested_at", F.to_timestamp(F.lit(ingested))).withColumn("_source_file", F.lit("f.csv"))


def line(**over):
    base = dict(
        transaction_id="T1",
        line_number="1",
        sale_ts="2026-10-01 12:30:00",
        store_code="LJ001",
        sku="SKU-1001",
        quantity="2",
        unit_price="5.50",
        discount="0.00",
        channel="store",
    )
    base.update(over)
    return tuple(base[c] for c in COLS)


def stores(spark):
    return spark.createDataFrame([("LJ001",), ("LJ002",)], "store_code string")


def test_clean_sales_standardizes_codes_and_types(spark):
    df = clean_sales(
        bronze_sales(spark, [line(sku="  sku-1001 ", store_code=" lj001", channel=" Store ", discount=None)])
    )
    r = df.first()
    assert (r.sku, r.store_code, r.channel) == ("SKU-1001", "LJ001", "store")
    assert r.quantity == 2 and float(r.unit_price) == 5.5 and float(r.discount) == 0.0
    assert r.sale_date == date(2026, 10, 1)


def test_bad_values_become_null_instead_of_failing_the_batch(spark):
    r = clean_sales(bronze_sales(spark, [line(quantity="abc", sale_ts="2026/13/45 25:00")])).first()
    assert r.quantity is None and r.sale_ts is None


def test_each_broken_line_is_quarantined_with_its_reasons(spark):
    rows = [
        line(transaction_id="ok"),
        line(transaction_id="t2", sku=""),
        line(transaction_id="t3", unit_price="-3.90"),
        line(transaction_id="t4", store_code="LJ999"),
        line(transaction_id="t5", channel="telefone", quantity="0"),
    ]
    valid, invalid = validate_sales(clean_sales(bronze_sales(spark, rows)), stores(spark))

    assert [r.transaction_id for r in valid.collect()] == ["ok"]
    reasons = {r.transaction_id: set(r._failed_rules) for r in invalid.collect()}
    assert reasons == {
        "t2": {"sku_present"},
        "t3": {"unit_price_non_negative", "discount_not_above_gross"},
        "t4": {"store_known"},
        "t5": {"channel_known", "quantity_positive"},
    }


def test_resent_line_keeps_the_latest_ingestion(spark):
    first = bronze_sales(spark, [line(quantity="1")], ingested="2026-10-01 10:00:00")
    resent = bronze_sales(spark, [line(quantity="3")], ingested="2026-10-02 10:00:00")
    valid, _ = validate_sales(clean_sales(first.unionByName(resent)), stores(spark))
    assert [r.quantity for r in valid.collect()] == [3]


def test_null_check_result_counts_as_failure(spark):
    df = spark.createDataFrame([(None,), (5,)], "x int")
    valid, invalid = split_valid_invalid(df, [Rule("x_positive", F.col("x") > 0)])
    assert valid.count() == 1 and invalid.first()._failed_rules == ["x_positive"]


def test_dedupe_latest_orders_nulls_last(spark):
    df = spark.createDataFrame([("a", None), ("a", "2026-10-02")], "k string, v string")
    assert dedupe_latest(df, ["k"], ["v"]).first().v == "2026-10-02"


def test_clean_products_normalizes_text(spark):
    df = (
        spark.createDataFrame(
            [
                (
                    " sku-1 ",
                    " Vale  Cacau Tablete ",
                    "vale cacau",
                    "CHOCOLATES",
                    "tabletes",
                    "90",
                    "7.49",
                    "2026-10-01 06:00:00",
                )
            ],
            (
                "sku string, product_name string, brand string, category string, subcategory string, "
                "pack_size_g string, list_price string, updated_at string"
            ),
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("p.json"))
    )
    r = clean_products(df).first()
    assert (r.sku, r.product_name, r.brand, r.category) == ("SKU-1", "Vale Cacau Tablete", "Vale Cacau", "Chocolates")
