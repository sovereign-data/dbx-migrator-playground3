# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: conformed order lines
# MAGIC
# MAGIC Joins the bronze TPC-H tables into one denormalised fact at order-line grain,
# MAGIC derives revenue, and rewrites the table on every run (idempotent, like the gold marts).
# MAGIC
# MAGIC Hand port for the portal: the PySpark DataFrame and `DeltaTable.merge` code is one SQL CTAS on SQE.
# MAGIC A `MERGE` of the full join is refused by SQE's 256 MiB query memory (it decodes every lineitem column);
# MAGIC the CTAS streams. Partitioning by region and the change data feed are Delta options and are left out.
# MAGIC `DESCRIBE HISTORY` is Delta only; the last cell reads the Iceberg snapshots instead.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE playground3.pg3_silver__sales.order_lines
# MAGIC COMMENT 'TPC-H order lines joined to customer and geography. Grain: one row per order line.'
# MAGIC AS
# MAGIC SELECT l.l_orderkey AS order_key, l.l_linenumber AS line_number, o.o_orderdate AS order_date,
# MAGIC          o.o_orderpriority AS order_priority, c.c_custkey AS customer_key, c.c_name AS customer_name,
# MAGIC          c.c_mktsegment AS market_segment, g.nation, g.region, l.l_partkey AS part_key,
# MAGIC          l.l_quantity AS quantity, l.l_extendedprice AS extended_price, l.l_discount AS discount, l.l_tax AS tax,
# MAGIC          CAST(l.l_extendedprice * (1 - l.l_discount) AS DECIMAL(38, 4)) AS net_revenue,
# MAGIC          l.l_returnflag AS return_flag, l.l_shipdate AS ship_date, l.l_shipmode AS ship_mode,
# MAGIC          current_timestamp AS _updated_at
# MAGIC FROM playground3.pg3_bronze__tpch.lineitem l
# MAGIC JOIN playground3.pg3_bronze__tpch.orders o ON l.l_orderkey = o.o_orderkey
# MAGIC JOIN playground3.pg3_bronze__tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC JOIN (SELECT n.n_nationkey AS nation_key,
# MAGIC                upper(substr(n.n_name, 1, 1)) || lower(substr(n.n_name, 2)) AS nation,
# MAGIC                upper(substr(r.r_name, 1, 1)) || lower(substr(r.r_name, 2)) AS region
# MAGIC         FROM playground3.pg3_bronze__tpch.nation n
# MAGIC         JOIN playground3.pg3_bronze__tpch.region r ON n.n_regionkey = r.r_regionkey) g
# MAGIC   ON c.c_nationkey = g.nation_key

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_id, committed_at, operation FROM playground3.pg3_silver__sales."order_lines$snapshots"
# MAGIC ORDER BY committed_at DESC LIMIT 5
