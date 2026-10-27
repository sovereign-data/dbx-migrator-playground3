# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: reporting marts
# MAGIC
# MAGIC Aggregates over `silver.sales.order_lines`: a monthly revenue table, a customer
# MAGIC ranking, and a view on top. Adds column comments and governance tags so the
# MAGIC migrator has metadata to carry across, not just rows.

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#2.1 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT silver_catalog DEFAULT 'pg3-silver'
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#2.2 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT silver_schema DEFAULT 'sales'
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#2.3 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT gold_catalog DEFAULT 'pg3-gold'
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#2.4 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT gold_schema DEFAULT 'marts'

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#3.5 is needs_review for sqe (session_statement); not run
# -- USE CATALOG IDENTIFIER('`' || :gold_catalog || '`')
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#3.6 is needs_review for sqe (session_statement); not run
# -- USE SCHEMA IDENTIFIER(:gold_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE playground3.pg3_gold__marts.revenue_by_month CLUSTER BY (order_month, region) COMMENT 'Monthly net revenue by geography and market segment.' AS SELECT CAST(DATE_TRUNC('MONTH', order_date) AS DATE) AS order_month, region, nation, market_segment, COUNT(DISTINCT order_key) AS orders, CAST(SUM(net_revenue) AS DECIMAL(38, 2)) AS net_revenue, AVG(discount) AS avg_discount FROM playground3.pg3_silver__sales.order_lines GROUP BY ALL;

# COMMAND ----------

# -- not on SQE yet (ALTER COLUMN ... COMMENT): ALTER TABLE playground3.pg3_gold__marts.revenue_by_month ALTER COLUMN order_month COMMENT 'First day of the order month.';
# -- not on SQE yet (ALTER COLUMN ... COMMENT): ALTER TABLE playground3.pg3_gold__marts.revenue_by_month ALTER COLUMN orders COMMENT 'Distinct orders.';
# -- not on SQE yet (ALTER COLUMN ... COMMENT): ALTER TABLE playground3.pg3_gold__marts.revenue_by_month ALTER COLUMN net_revenue COMMENT 'Sum of extended_price * (1 - discount).';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE playground3.pg3_gold__marts.top_customers COMMENT 'Top 100 customers per region by lifetime net revenue.' AS WITH lifetime AS (SELECT customer_key, customer_name, region, nation, market_segment, SUM(net_revenue) AS lifetime_revenue, COUNT(DISTINCT order_key) AS orders, MAX(order_date) AS last_order_date FROM playground3.pg3_silver__sales.order_lines GROUP BY ALL) SELECT *, CAST(RANK() OVER (PARTITION BY region ORDER BY lifetime_revenue DESC) AS BIGINT) AS region_rank FROM lifetime QUALIFY region_rank <= 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW playground3.pg3_gold__marts.v_revenue_yoy COMMENT 'Year-over-year net revenue growth per region.' AS WITH yearly AS (SELECT EXTRACT(YEAR FROM order_month) AS order_year, region, SUM(net_revenue) AS net_revenue FROM playground3.pg3_gold__marts.revenue_by_month GROUP BY ALL) SELECT order_year, region, net_revenue, LAG(net_revenue) OVER (PARTITION BY region ORDER BY order_year NULLS FIRST) AS prev_year_revenue, CASE WHEN LAG(net_revenue) OVER (PARTITION BY region ORDER BY order_year NULLS FIRST) <> 0 THEN CAST((net_revenue - LAG(net_revenue) OVER (PARTITION BY region ORDER BY order_year NULLS FIRST)) AS DOUBLE) / LAG(net_revenue) OVER (PARTITION BY region ORDER BY order_year NULLS FIRST) * 100 END AS yoy_growth_pct FROM yearly;

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#7.13 is blocked for sqe (uc_tags); not run
# -- ALTER TABLE revenue_by_month SET TAGS ('domain' = 'sales', 'certified' = 'false')
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#7.14 is blocked for sqe (uc_tags); not run
# -- ALTER TABLE top_customers   SET TAGS ('domain' = 'sales', 'contains_pii' = 'customer_name')
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/gold/marts#7.15 is blocked for sqe (uc_tags); not run
# -- ALTER TABLE top_customers ALTER COLUMN customer_name SET TAGS ('pii' = 'name')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM playground3.pg3_gold__marts.v_revenue_yoy ORDER BY region NULLS FIRST, order_year NULLS FIRST;
