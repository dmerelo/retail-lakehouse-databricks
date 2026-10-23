from datetime import date, datetime

from lakehouse.gold import UNKNOWN_KEY, build_daily_category_sales, build_dim_date, build_fact_sales
from lakehouse.scd import OPEN_END

SALES = (
    "transaction_id string, line_number int, sale_ts timestamp, sale_date date, store_code string, sku string, "
    "quantity int, unit_price decimal(12,2), discount decimal(12,2), channel string"
)
DIM = "product_key long, sku string, category string, valid_from timestamp, valid_to timestamp"
END = datetime.fromisoformat(OPEN_END)


def sale(tx, ts, sku, qty=1, price="10.00", discount="0.00"):
    from decimal import Decimal

    return (tx, 1, ts, ts.date(), "LJ001", sku, qty, Decimal(price), Decimal(discount), "store")


def dim_product(spark):
    return spark.createDataFrame(
        [
            (1, "A", "Biscoitos", datetime(1900, 1, 1), datetime(2026, 10, 3, 6)),
            (2, "A", "Chocolates", datetime(2026, 10, 3, 6), END),
        ],
        DIM,
    )


def test_fact_picks_the_product_version_valid_at_sale_time(spark):
    sales = spark.createDataFrame(
        [sale("before", datetime(2026, 10, 3, 5, 59), "A"), sale("after", datetime(2026, 10, 3, 6, 0), "A")], SALES
    )
    fact = build_fact_sales(sales, dim_product(spark), date(2026, 10, 1), date(2026, 10, 5))
    keys = {r.transaction_id: r.product_key for r in fact.collect()}
    assert keys == {"before": 1, "after": 2}


def test_unknown_product_gets_the_unknown_member(spark):
    sales = spark.createDataFrame([sale("x", datetime(2026, 10, 3, 9), "NOPE")], SALES)
    fact = build_fact_sales(sales, dim_product(spark), date(2026, 10, 1), date(2026, 10, 5))
    assert fact.first().product_key == UNKNOWN_KEY


def test_fact_respects_window_and_computes_amounts(spark):
    sales = spark.createDataFrame(
        [
            sale("in", datetime(2026, 10, 3, 9), "A", qty=3, price="4.00", discount="1.20"),
            sale("out", datetime(2026, 9, 20, 9), "A"),
        ],
        SALES,
    )
    fact = build_fact_sales(sales, dim_product(spark), date(2026, 10, 1), date(2026, 10, 5)).collect()
    assert [r.transaction_id for r in fact] == ["in"]
    assert (float(fact[0].gross_amount), float(fact[0].net_amount), fact[0].date_key) == (12.0, 10.8, 20261003)


def test_aggregate_uses_category_at_sale_time(spark):
    sales = spark.createDataFrame(
        [sale("old", datetime(2026, 10, 2, 9), "A"), sale("new", datetime(2026, 10, 4, 9), "A")], SALES
    )
    dims = dim_product(spark)
    fact = build_fact_sales(sales, dims, date(2026, 10, 1), date(2026, 10, 5))
    stores = spark.createDataFrame([], "store_key long, region string")
    agg = {(r.sale_date.day, r.category) for r in build_daily_category_sales(fact, dims, stores).collect()}
    assert agg == {(2, "Biscoitos"), (4, "Chocolates")}


def test_dim_date_covers_range_and_flags_weekends(spark):
    d = {r.date: r for r in build_dim_date(spark, date(2026, 10, 1), date(2026, 10, 31)).collect()}
    assert len(d) == 31
    assert d[date(2026, 10, 3)].is_weekend and not d[date(2026, 10, 5)].is_weekend
    assert d[date(2026, 10, 5)].date_key == 20261005
