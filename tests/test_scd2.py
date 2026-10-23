from datetime import datetime

from pyspark.sql import functions as F

from lakehouse.scd import BEGINNING, OPEN_END, stage_scd2

ATTRS = ["category", "list_price"]
SCHEMA = "sku string, category string, list_price string, updated_at timestamp"
START = datetime.fromisoformat(BEGINNING)
END = datetime.fromisoformat(OPEN_END)


def stage(spark, rows, current=None):
    return stage_scd2(spark.createDataFrame(rows, SCHEMA), current, "sku", ATTRS, "updated_at", "product_key")


def inserts(staged):
    return sorted(staged.filter("merge_key IS NULL").drop("merge_key").collect(), key=lambda r: (r.sku, r.valid_from))


def as_dim(staged):
    """What the dimension holds after a first load (nothing to close yet)."""
    return staged.filter("merge_key IS NULL").drop("merge_key")


def test_first_load_inserts_one_open_version_per_key_from_the_beginning(spark):
    rows = inserts(
        stage(spark, [("A", "Chocolates", "5.00", datetime(2026, 9, 1)), ("B", "Balas", "2.00", datetime(2026, 9, 1))])
    )
    assert [(r.sku, r.valid_from, r.valid_to, r.is_current) for r in rows] == [
        ("A", START, END, True),
        ("B", START, END, True),
    ]


def test_several_versions_in_one_load_are_chained_in_time_order(spark):
    rows = inserts(
        stage(
            spark,
            [
                ("A", "Biscoitos", "6.00", datetime(2026, 10, 5)),
                ("A", "Biscoitos", "5.00", datetime(2026, 9, 1)),
                ("A", "Biscoitos", "5.00", datetime(2026, 9, 20)),  # same attributes: no new version
            ],
        )
    )
    assert [(r.list_price, r.valid_from, r.valid_to, r.is_current) for r in rows] == [
        ("5.00", START, datetime(2026, 10, 5), False),
        ("6.00", datetime(2026, 10, 5), END, True),
    ]


def test_change_closes_the_current_version_and_opens_a_new_one(spark):
    dim = as_dim(stage(spark, [("A", "Chocolates", "5.00", datetime(2026, 9, 1))]))
    staged = stage(spark, [("A", "Chocolates", "5.40", datetime(2026, 10, 3, 6))], current=dim)

    close = staged.filter(F.col("merge_key").isNotNull()).collect()
    assert [(r.merge_key, r.valid_from) for r in close] == [("A", datetime(2026, 10, 3, 6))]
    assert [(r.list_price, r.valid_from, r.is_current) for r in inserts(staged)] == [
        ("5.40", datetime(2026, 10, 3, 6), True)
    ]


def test_unchanged_attributes_stage_nothing(spark):
    dim = as_dim(stage(spark, [("A", "Chocolates", "5.00", datetime(2026, 9, 1))]))
    assert stage(spark, [("A", "Chocolates", "5.00", datetime(2026, 10, 3))], current=dim).count() == 0


def test_snapshot_older_than_the_current_version_is_ignored(spark):
    dim = as_dim(stage(spark, [("A", "Chocolates", "5.40", datetime(2026, 10, 3))])).withColumn(
        "valid_from", F.lit(datetime(2026, 10, 3))
    )
    assert stage(spark, [("A", "Chocolates", "0.99", datetime(2026, 9, 15))], current=dim).count() == 0


def test_surrogate_key_differs_per_version(spark):
    rows = inserts(
        stage(
            spark,
            [("A", "Chocolates", "5.00", datetime(2026, 9, 1)), ("A", "Biscoitos", "5.00", datetime(2026, 10, 3))],
        )
    )
    assert len({r.product_key for r in rows}) == 2
