-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Analysis
-- MAGIC Queries a BI tool would run against the star schema. Set the catalog/schema first.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA retail;

-- COMMAND ----------

-- Net revenue by region and week
SELECT d.year, d.week_of_year, s.region, sum(f.net_amount) AS net_revenue, sum(f.quantity) AS units
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY ALL
ORDER BY d.year, d.week_of_year, net_revenue DESC;

-- COMMAND ----------

-- Average realised price before and after each list price change
SELECT p.sku, p.product_name, p.list_price, p.valid_from, p.valid_to,
       round(sum(f.net_amount) / sum(f.quantity), 2) AS avg_net_unit_price,
       sum(f.quantity) AS units
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.sku IN (SELECT sku FROM dim_product GROUP BY sku HAVING count(*) > 1)
GROUP BY ALL
ORDER BY p.sku, p.valid_from;

-- COMMAND ----------

-- Weekend share of revenue by store format
SELECT s.store_format,
       round(100 * sum(CASE WHEN d.is_weekend THEN f.net_amount END) / sum(f.net_amount), 1) AS weekend_pct
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_store s ON f.store_key = s.store_key
GROUP BY s.store_format;

-- COMMAND ----------

-- Data quality: rejected rows per rule and day
SELECT to_date(quarantined_at) AS day, entity, rule, count(*) AS rows
FROM silver_quarantine LATERAL VIEW explode(failed_rules) r AS rule
GROUP BY ALL
ORDER BY day DESC, rows DESC;

-- COMMAND ----------

-- Pipeline health: rows and duration per layer for the last runs
SELECT run_id, layer, entity,
       sum(rows_in) AS rows_in, sum(rows_valid) AS valid, sum(rows_rejected) AS rejected,
       round(sum(duration_s), 1) AS seconds, max(finished_at) AS finished_at
FROM run_metrics
GROUP BY ALL
ORDER BY finished_at DESC
LIMIT 30;
