import time

from lakehouse import jobs
from lakehouse.config import Tables


def test_job_arguments_from_python_wheel_task():
    args = jobs._parse(["--catalog", "workspace", "--schema", "retail", "--run-id", "991"], gold=True)
    assert (args.catalog, args.schema, args.run_id, args.run_date, args.lookback_days) == (
        "workspace",
        "retail",
        "991",
        "",
        7,
    )


def test_metrics_rows_match_the_table_schema(spark):
    m = jobs.Metrics(spark, Tables("", "x"), run_id="r1")
    m.record("silver", "sales", time.time() - 2, rows_in=10, valid=8, rejected=2)
    row = spark.createDataFrame(m.rows, jobs.METRICS_SCHEMA).first()
    assert (row.run_id, row.layer, row.entity, row.rows_valid, row.rows_rejected) == ("r1", "silver", "sales", 8, 2)
    assert row.duration_s >= 2
