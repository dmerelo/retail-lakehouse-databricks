import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pyspark.sql import SparkSession  # noqa: E402

DELTA = os.environ.get("RUN_DELTA_TESTS") == "1"

# Python and Spark must agree on the time zone, otherwise collected timestamps shift.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retail-lakehouse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    )
    if DELTA:
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    yield session
    session.stop()


def pytest_collection_modifyitems(config, items):
    if DELTA:
        return
    skip = pytest.mark.skip(reason="set RUN_DELTA_TESTS=1 (needs the Delta Lake jars)")
    for item in items:
        if "delta" in item.keywords:
            item.add_marker(skip)
