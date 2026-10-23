"""Small Delta Lake write helpers. ``delta`` is imported lazily so the pure
transformation code (and its tests) does not need the Delta jars."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def upsert(spark: SparkSession, df: DataFrame, table: str, keys: list[str], newer_than: str) -> None:
    """Insert new keys; update existing keys only when the incoming row is newer.

    The ``newer_than`` guard makes replays harmless: reprocessing an old file
    cannot overwrite a fresher version of the same record.
    """
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return

    from delta.tables import DeltaTable

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(df.alias("s"), on)
        .whenMatchedUpdateAll(condition=f"s.{newer_than} > t.{newer_than}")
        .whenNotMatchedInsertAll()
        .execute()
    )


def insert_new(spark: SparkSession, df: DataFrame, table: str, keys: list[str]) -> None:
    """Insert only rows whose keys are not in the table yet (idempotent append)."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return

    from delta.tables import DeltaTable

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    DeltaTable.forName(spark, table).alias("t").merge(df.alias("s"), on).whenNotMatchedInsertAll().execute()


def append(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table)


def overwrite(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def replace_where(spark: SparkSession, df: DataFrame, table: str, predicate: str, partition_by: list[str]) -> None:
    """Atomically replace only the rows matching ``predicate`` (idempotent reprocessing)."""
    if not spark.catalog.tableExists(table):
        df.write.format("delta").partitionBy(*partition_by).saveAsTable(table)
        return
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)
