"""Job entry points (python_wheel_task) and run metrics.

Each layer is a console script installed with the wheel:

    lakehouse-bronze --catalog workspace --schema retail --run-id 123
    lakehouse-silver ...
    lakehouse-gold   ... --run-date 2026-10-23 --lookback-days 7

Every run appends one row per layer/entity to ``run_metrics`` (rows read, valid,
rejected, duration), which is what a dashboard or an alert would watch.
"""

from __future__ import annotations

import argparse
import time
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession

from lakehouse import bronze, delta_io, pipeline, silver
from lakehouse.config import Tables

METRICS_SCHEMA = (
    "run_id string, layer string, entity string, rows_in long, rows_valid long, rows_rejected long, "
    "started_at timestamp, finished_at timestamp, duration_s double"
)


class Metrics:
    def __init__(self, spark: SparkSession, t: Tables, run_id: str):
        self.spark, self.t, self.run_id = spark, t, run_id
        self.rows: list[tuple] = []

    def record(self, layer: str, entity: str, started: float, rows_in=None, valid=None, rejected=None) -> None:
        finished = time.time()
        self.rows.append(
            (
                self.run_id,
                layer,
                entity,
                rows_in,
                valid,
                rejected,
                datetime.fromtimestamp(started, timezone.utc).replace(tzinfo=None),
                datetime.fromtimestamp(finished, timezone.utc).replace(tzinfo=None),
                round(finished - started, 1),
            )
        )
        print(f"[{layer}] {entity}: in={rows_in} valid={valid} rejected={rejected} ({finished - started:.1f}s)")

    def flush(self) -> None:
        if self.rows:
            delta_io.append(self.spark.createDataFrame(self.rows, METRICS_SCHEMA), self.t.run_metrics)
            self.rows = []


def _input_rows(query) -> int | None:
    try:
        return sum(p["numInputRows"] for p in query.recentProgress)
    except Exception:  # progress is best effort; never fail a run because of it
        return None


# --- layers ------------------------------------------------------------------


def run_bronze(spark: SparkSession, t: Tables, m: Metrics) -> None:
    landing = f"/Volumes/{t.catalog}/{t.schema}/landing"
    checkpoints = f"/Volumes/{t.catalog}/{t.schema}/checkpoints"
    for source, table in [("stores", t.bronze_stores), ("products", t.bronze_products), ("sales", t.bronze_sales)]:
        started = time.time()
        query = (
            bronze.read_stream(spark, source, f"{landing}/{source}")
            .writeStream.option("checkpointLocation", f"{checkpoints}/bronze_{source}")
            .trigger(availableNow=True)
            .toTable(table)
        )
        query.awaitTermination()
        m.record("bronze", source, started, rows_in=_input_rows(query))


SILVER = [
    ("stores", "bronze_stores", silver.process_stores_batch),
    ("products", "bronze_products", silver.process_products_batch),
    ("sales", "bronze_sales", silver.process_sales_batch),
]


def run_silver(spark: SparkSession, t: Tables, m: Metrics) -> None:
    """Stores and products first: sales are validated against the store master.

    Metrics are written from inside each micro-batch: on serverless (Spark
    Connect) foreachBatch runs in a server-side process, so counters kept in
    this function would never see the batches.
    """
    checkpoints = f"/Volumes/{t.catalog}/{t.schema}/checkpoints"
    run_id = m.run_id
    for entity, bronze_attr, process in SILVER:

        def per_batch(batch_df, _batch_id, entity=entity, process=process):
            batch_metrics = Metrics(batch_df.sparkSession, t, run_id)
            started = time.time()
            counts = process(batch_df.sparkSession, batch_df, t)
            batch_metrics.record(
                "silver",
                entity,  # one row per micro-batch; sum by run_id to get the run total
                started,
                rows_in=counts["valid"] + counts["quarantined"],
                valid=counts["valid"],
                rejected=counts["quarantined"],
            )
            batch_metrics.flush()

        (
            spark.readStream.table(getattr(t, bronze_attr))
            .writeStream.foreachBatch(per_batch)
            .option("checkpointLocation", f"{checkpoints}/silver_{entity}")
            .trigger(availableNow=True)
            .start()
            .awaitTermination()
        )


def run_gold(spark: SparkSession, t: Tables, m: Metrics, start: date, end: date) -> None:
    started = time.time()
    pipeline.refresh_dimensions(spark, t)
    m.record("gold", "dimensions", started)

    started = time.time()
    counts = pipeline.refresh_facts(spark, t, start, end)
    m.record(
        "gold",
        "fact_sales",
        started,
        rows_in=counts["fact_lines"],
        valid=counts["fact_lines"] - counts["unknown_product_lines"],
        rejected=counts["unknown_product_lines"],
    )


# --- console scripts -----------------------------------------------------------


def _parse(argv: list[str] | None, gold: bool = False) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--run-id", default=None)
    if gold:
        p.add_argument("--run-date", default="")
        p.add_argument("--lookback-days", type=int, default=7)
    return p.parse_args(argv)


def _context(args: argparse.Namespace) -> tuple[SparkSession, Tables, Metrics]:
    spark = SparkSession.builder.getOrCreate()
    t = Tables(args.catalog, args.schema)
    return spark, t, Metrics(spark, t, args.run_id or uuid.uuid4().hex[:12])


def _run(layer_fn, args: argparse.Namespace, *extra) -> None:
    spark, t, m = _context(args)
    try:
        layer_fn(spark, t, m, *extra)
    finally:
        m.flush()  # keep the metrics of the steps that finished, even when a later one fails


def bronze_main(argv: list[str] | None = None) -> None:
    _run(run_bronze, _parse(argv))


def silver_main(argv: list[str] | None = None) -> None:
    _run(run_silver, _parse(argv))


def gold_main(argv: list[str] | None = None) -> None:
    args = _parse(argv, gold=True)
    end = date.fromisoformat(args.run_date) if args.run_date else date.today()
    _run(run_gold, args, end - timedelta(days=args.lookback_days), end)
