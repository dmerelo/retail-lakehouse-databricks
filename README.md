# Retail sell-out lakehouse on Databricks

[![ci](https://github.com/dmerelo/retail-lakehouse-databricks/actions/workflows/ci.yml/badge.svg)](https://github.com/dmerelo/retail-lakehouse-databricks/actions/workflows/ci.yml)

Daily point-of-sale files from a chain of 12 stores, plus product and store master data, turned into a star schema that a BI tool can query. Built on Databricks with Delta Lake, Unity Catalog volumes, Auto Loader and a serverless job that runs the code as a Python wheel, deployed with Databricks Asset Bundles.

The data is synthetic, but it breaks the way real retail feeds break: codes with stray spaces, lines re-sent the next day, sales arriving late, prices and categories changing mid-week. Most of the code exists to deal with that.

```mermaid
flowchart LR
    L[/"landing volume<br/>CSV / JSON files"/] -->|Auto Loader<br/>availableNow| B[(bronze<br/>raw strings +<br/>file metadata)]
    B -->|stream + foreachBatch| S[(silver<br/>typed, deduplicated,<br/>validated)]
    B -.->|rows that fail a rule| Q[(silver_quarantine)]
    S -->|SCD1 / SCD2 MERGE| D[(dim_store<br/>dim_product<br/>dim_date)]
    S -->|replaceWhere<br/>last N days| F[(fact_sales)]
    D --> F
    F --> A[(agg_daily_<br/>category_sales)]
```

## Data model

```mermaid
erDiagram
    fact_sales }o--|| dim_date : date_key
    fact_sales }o--|| dim_store : store_key
    fact_sales }o--|| dim_product : product_key
    fact_sales {
        int date_key
        bigint store_key
        bigint product_key
        string transaction_id
        int line_number
        int quantity
        decimal unit_price
        decimal discount
        decimal net_amount
    }
    dim_product {
        bigint product_key "hash(sku, valid_from)"
        string sku
        string category
        decimal list_price
        timestamp valid_from
        timestamp valid_to
        boolean is_current
    }
    dim_store {
        bigint store_key "hash(store_code)"
        string store_code
        string region
        string store_format
    }
```

Grain of `fact_sales`: one line of a receipt. Each line points to the product **version** that was valid at the time of sale, so revenue by category stays correct after a product is moved to another category.

## Design decisions

**Bronze keeps everything as strings.** If a file arrives with `quantity = "abc"`, that value is still in the lake and can be shown to the source team. Casting happens in silver with `try_cast` / `try_to_timestamp`, because Databricks runs with ANSI mode on and a plain cast would fail the whole batch.

**Broken rows go to quarantine, not to /dev/null.** Every rule is a boolean column; a row that fails any rule lands in `silver_quarantine` with the list of rules it broke and the file it came from. A null result counts as a failure, so `quantity > 0` also catches a missing quantity. Quarantine inserts are keyed by a hash of (entity, file, record), so reprocessing a file does not count its errors twice.

**Incremental by default, safe to rerun.** Auto Loader and the bronze-to-silver stream keep checkpoints, so each file and each bronze row is processed once. Silver MERGEs only overwrite a record when the incoming one is newer, so replaying an old file cannot undo a newer value.

**SCD2 built from versions, applied with one MERGE.** `silver_products` keeps every version of a product. `scd.stage_scd2` chains the versions that are newer than the dimension's current row (several changes in one load are ordered by their effective time; identical consecutive versions collapse). `apply_scd2` closes the old row and inserts the new ones in the same Delta commit. The change detection is plain DataFrame code, so it is unit tested without Delta.

**Facts are rebuilt for a window, not appended.** Gold recomputes the last `lookback_days` (default 7) and writes them with `replaceWhere`. A sale that arrives two days late is picked up by the next run, and running the same window twice produces the same table.

**Stable surrogate keys.** `store_key = xxhash64(store_code)` and `product_key = xxhash64(sku, valid_from)`: no sequence table to maintain, and a rebuilt dimension keeps the same keys. Lookups that find nothing get the unknown member (`-1`); the run fails if more than 1% of fact lines end up there.

**Packaged, not pasted.** The job installs the project as a wheel and runs three console scripts (`lakehouse-bronze`, `-silver`, `-gold`) as `python_wheel_task`s on serverless compute. Notebooks call the same functions, so what runs on schedule is exactly what the tests cover. No `.cache()` anywhere (not supported on serverless).

**Observable.** Every run writes rows read, valid, rejected and duration per layer and entity to `run_metrics`, tagged with the job run id. Silver writes its metrics from inside each micro-batch, because on serverless `foreachBatch` runs in a separate process.

**Two targets.** `dev` (default) deploys a personal copy with the schedule paused; `prod` uses its own schema (`retail_prod`), keeps the schedule active and e-mails the owner on failure.

## Data quality rules

| Entity | Rule | Example caught in the sample data |
|---|---|---|
| sales | `sku_present`, `transaction_id_present`, `line_number_valid` | empty SKU |
| sales | `sale_ts_parsed`, `sale_not_in_future` | `2026/13/45 25:00` |
| sales | `quantity_positive`, `unit_price_non_negative`, `discount_not_above_gross` | price `-3.90`, quantity `0` |
| sales | `store_known` (checked against the store master) | store `LJ999` |
| sales | `channel_known` | channel `telefone` |
| products | `sku_present`, `category_present`, `list_price_non_negative`, `updated_at_parsed` | |
| stores | `store_code_present`, `state_is_uf`, `updated_at_parsed` | |

Fixable problems are fixed instead of rejected: `"  sku-1003 "` becomes `SKU-1003`, repeated spaces are collapsed, missing discount becomes 0.

## Running it

**On Databricks (Free Edition works):**

1. *Workspace → Create → Git folder* and paste this repository URL.
2. Open `notebooks/00_setup.py` and run it. It creates the `retail` schema, the `landing` and `checkpoints` volumes and 14 days of sample files.
3. Run `01_bronze`, `02_silver` and `03_gold` in order.
4. `04_analysis.sql` has example queries (revenue by region and week, price before/after a list price change, rejected rows per rule, run metrics).

To deploy the scheduled job instead, use the Databricks CLI from a clone of the repo (it builds the wheel and uploads it):

```bash
databricks bundle deploy -t dev     # or -t prod
databricks bundle run retail_sales_daily -t dev
```

To simulate new days arriving, run `00_setup` again with a later `end_date` and rerun: only the new files are processed. `reset = true` in `00_setup` starts over from an empty schema.

**Tests locally:**

```bash
pip install -r requirements-dev.txt
pytest -q                     # transformation logic, plain Spark
RUN_DELTA_TESTS=1 pytest -q   # also the end-to-end run on Delta (downloads the Delta jars)
```

CI runs both on every push.

## Layout

```
databricks.yml                 bundle: wheel artifact, variables, dev / prod targets
resources/retail_sales.job.yml serverless job: three python_wheel_tasks, retries, schedule, failure e-mail
notebooks/                     interactive entry points (setup, one notebook per layer, analysis)
src/lakehouse/
  bronze.py                    Auto Loader / batch readers, ingestion metadata
  silver.py                    cleaning, validation, dedup, MERGE
  quality.py                   rule engine and run-level checks
  scd.py                       SCD2 staging + MERGE
  gold.py                      dimensions, fact, aggregate
  pipeline.py                  gold orchestration
  jobs.py                      console scripts for the job + run metrics
  sample_data.py               synthetic feed with injected problems
tests/                         unit tests + end-to-end test on Delta
```

## What I would change for a real deployment

- Read silver changes with Change Data Feed instead of re-reading `silver_products` to stage SCD2; fine at catalog size, not for a large dimension.
- Move the rules to Lakeflow Declarative Pipelines expectations if the team standardizes on it; the rule list maps one to one.
- Liquid clustering on `fact_sales` by `(sale_date, store_key)` instead of date partitions once volume grows.
- A SQL alert on `run_metrics` / `silver_quarantine` (rejection rate per rule above a threshold) instead of relying only on job failure e-mails.
- A service principal as `run_as` in `prod` instead of the deploying user.
