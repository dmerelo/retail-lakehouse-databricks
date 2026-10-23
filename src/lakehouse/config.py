from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Tables:
    """Fully qualified table names for one environment.

    On Databricks this is ``<catalog>.<schema>.<table>`` (Unity Catalog).
    Locally the catalog is left empty and tables live in the Spark session catalog.
    """

    catalog: str
    schema: str

    def fq(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}" if self.catalog else f"{self.schema}.{name}"

    # bronze: raw records as received, plus ingestion metadata
    @property
    def bronze_sales(self) -> str:
        return self.fq("bronze_sales")

    @property
    def bronze_products(self) -> str:
        return self.fq("bronze_products")

    @property
    def bronze_stores(self) -> str:
        return self.fq("bronze_stores")

    # silver: typed, standardized, deduplicated, validated
    @property
    def silver_sales(self) -> str:
        return self.fq("silver_sales")

    @property
    def silver_quarantine(self) -> str:
        """Rejected rows from every entity, with the rules they failed."""
        return self.fq("silver_quarantine")

    @property
    def silver_products(self) -> str:
        """One row per product version (sku, updated_at)."""
        return self.fq("silver_products")

    @property
    def silver_stores(self) -> str:
        return self.fq("silver_stores")

    @property
    def run_metrics(self) -> str:
        """One row per layer/entity per job run: rows in, valid, rejected, duration."""
        return self.fq("run_metrics")

    # gold: star schema and aggregates for BI
    @property
    def dim_date(self) -> str:
        return self.fq("dim_date")

    @property
    def dim_store(self) -> str:
        return self.fq("dim_store")

    @property
    def dim_product(self) -> str:
        return self.fq("dim_product")

    @property
    def fact_sales(self) -> str:
        return self.fq("fact_sales")

    @property
    def agg_daily_category_sales(self) -> str:
        return self.fq("agg_daily_category_sales")
