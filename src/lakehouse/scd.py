"""Slowly changing dimension type 2 on Delta Lake.

The change detection (``stage_scd2``) is plain DataFrame code and is unit tested
without Delta. ``apply_scd2`` is a single MERGE, so closing the old version and
inserting the new ones happen in the same atomic commit.

Incoming data may hold several versions of the same key (e.g. a price change on
Tuesday and a category change on Thursday, loaded together). They are chained in
order of their effective timestamp, so the dimension ends up with the same
history whether the job ran daily or once a week.

Staged rows come in two kinds:
  * merge_key = <business key>: closes the current version in the dimension;
  * merge_key = null: inserts a version (brand-new key or a change).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

OPEN_END = "9999-12-31 00:00:00"
BEGINNING = "1900-01-01 00:00:00"


def attr_hash(attrs: list[str]) -> F.Column:
    parts = [F.coalesce(F.col(a).cast("string"), F.lit("<null>")) for a in attrs]
    return F.sha2(F.concat_ws("||", *parts), 256)


def stage_scd2(
    incoming: DataFrame,
    current_dim: DataFrame | None,
    key: str,
    attrs: list[str],
    effective_col: str,
    surrogate: str,
) -> DataFrame:
    """Return the rows to MERGE into the dimension.

    * Versions older than (or as old as) the current one are ignored: history is
      only appended, never rewritten by a late file.
    * Consecutive versions with identical attributes collapse into one.
    * A brand-new key starts at the beginning of time, so facts older than the
      first catalog snapshot still find their product.
    """
    inc = (
        incoming.select(key, *attrs, F.col(effective_col).alias("_effective"))
        .dropDuplicates([key, "_effective"])
        .withColumn("attr_hash", attr_hash(attrs))
    )

    if current_dim is None:
        cur = inc.sparkSession.createDataFrame([], f"{key} string, _cur_hash string, _cur_from timestamp")
    else:
        cur = current_dim.filter("is_current").select(
            key, F.col("attr_hash").alias("_cur_hash"), F.col("valid_from").alias("_cur_from")
        )

    by_time = Window.partitionBy(key).orderBy("_effective")
    versions = (
        inc.join(cur, key, "left")
        .filter(F.col("_cur_from").isNull() | (F.col("_effective") > F.col("_cur_from")))
        .withColumn("_prev_hash", F.coalesce(F.lag("attr_hash").over(by_time), F.col("_cur_hash")))
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("attr_hash")))
        .withColumn("_next", F.lead("_effective").over(by_time))
        .withColumn("_seq", F.row_number().over(by_time))
    )

    first_of_new_key = F.col("_cur_hash").isNull() & (F.col("_seq") == 1)
    versions = versions.withColumn(
        "valid_from", F.when(first_of_new_key, F.to_timestamp(F.lit(BEGINNING))).otherwise(F.col("_effective"))
    )

    inserts = versions.select(
        F.lit(None).cast("string").alias("merge_key"),
        F.xxhash64(key, "valid_from").alias(surrogate),
        key,
        *attrs,
        "attr_hash",
        "valid_from",
        F.coalesce(F.col("_next"), F.to_timestamp(F.lit(OPEN_END))).alias("valid_to"),
        F.col("_next").isNull().alias("is_current"),
    )
    # one closing row per existing key; its valid_from becomes the old version's valid_to
    closes = versions.filter(F.col("_cur_hash").isNotNull() & (F.col("_seq") == 1)).select(
        F.col(key).alias("merge_key"),
        F.lit(None).cast("long").alias(surrogate),
        key,
        *attrs,
        "attr_hash",
        "valid_from",
        F.to_timestamp(F.lit(OPEN_END)).alias("valid_to"),
        F.lit(False).alias("is_current"),
    )
    return closes.unionByName(inserts)


def apply_scd2(
    spark: SparkSession, staged: DataFrame, table: str, key: str, unknown_member: DataFrame | None = None
) -> None:
    if not spark.catalog.tableExists(table):
        first = staged.filter("merge_key IS NULL").drop("merge_key")
        if unknown_member is not None:
            first = first.unionByName(unknown_member)
        first.write.format("delta").saveAsTable(table)
        return

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(spark, table)
        .alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s.valid_from"})
        .whenNotMatchedInsert(
            condition="s.merge_key IS NULL", values={c: f"s.{c}" for c in staged.columns if c != "merge_key"}
        )
        .execute()
    )
