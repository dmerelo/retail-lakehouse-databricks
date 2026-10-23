"""Row-level data quality rules.

Each rule is a boolean column that must be true for a valid row. A null result
counts as a failure, so ``quantity > 0`` also rejects a null quantity.
Invalid rows are not dropped: they go to a quarantine table with the list of
rules they broke, so the source team can be told exactly what to fix.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    check: Column


class DataQualityError(RuntimeError):
    pass


def split_valid_invalid(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    failed = F.filter(
        F.array(*[F.when(F.coalesce(r.check, F.lit(False)), F.lit(None)).otherwise(F.lit(r.name)) for r in rules]),
        lambda x: x.isNotNull(),
    )
    flagged = df.withColumn("_failed_rules", failed)
    valid = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    invalid = flagged.filter(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


def assert_unique(df: DataFrame, keys: list[str], table: str) -> None:
    dupes = df.groupBy(*keys).count().filter("count > 1").limit(5).collect()
    if dupes:
        raise DataQualityError(f"{table}: duplicated keys {keys}, e.g. {[r.asDict() for r in dupes]}")


def assert_max_ratio(numerator: int, denominator: int, max_ratio: float, what: str) -> None:
    """Fail the run when a soft problem (e.g. unknown products in the fact) gets too big."""
    if denominator and numerator / denominator > max_ratio:
        raise DataQualityError(f"{what}: {numerator}/{denominator} exceeds {max_ratio:.0%}")
