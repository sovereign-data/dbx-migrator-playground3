# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: conformed order lines
# MAGIC
# MAGIC Joins the bronze TPC-H tables into one denormalised fact at order-line grain,
# MAGIC derives revenue, and upserts it with `MERGE` so reruns are idempotent.
# MAGIC
# MAGIC Hand port for the portal: the PySpark DataFrame and `DeltaTable.merge` code is one SQL `MERGE` on SQE.
# MAGIC The table exists (the migration copied it), so the first-run `saveAsTable` branch is gone.
# MAGIC `DESCRIBE HISTORY` is Delta only; the last cell reads the Iceberg snapshots instead.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO playground3.pg3_silver__sales.order_lines AS t
# MAGIC USING (
# MAGIC   SELECT l.l_orderkey AS order_key, l.l_linenumber AS line_number, o.o_orderdate AS order_date,
# MAGIC          o.o_orderpriority AS order_priority, c.c_custkey AS customer_key, c.c_name AS customer_name,
# MAGIC          c.c_mktsegment AS market_segment, g.nation, g.region, l.l_partkey AS part_key,
# MAGIC          l.l_quantity AS quantity, l.l_extendedprice AS extended_price, l.l_discount AS discount, l.l_tax AS tax,
# MAGIC          CAST(l.l_extendedprice * (1 - l.l_discount) AS DECIMAL(38, 4)) AS net_revenue,
# MAGIC          l.l_returnflag AS return_flag, l.l_shipdate AS ship_date, l.l_shipmode AS ship_mode,
# MAGIC          current_timestamp AS _updated_at
# MAGIC   FROM playground3.pg3_bronze__tpch.lineitem l
# MAGIC   JOIN playground3.pg3_bronze__tpch.orders o ON l.l_orderkey = o.o_orderkey
# MAGIC   JOIN playground3.pg3_bronze__tpch.customer c ON o.o_custkey = c.c_custkey
# MAGIC   JOIN (SELECT n.n_nationkey AS nation_key,
# MAGIC                upper(substr(n.n_name, 1, 1)) || lower(substr(n.n_name, 2)) AS nation,
# MAGIC                upper(substr(r.r_name, 1, 1)) || lower(substr(r.r_name, 2)) AS region
# MAGIC         FROM playground3.pg3_bronze__tpch.nation n
# MAGIC         JOIN playground3.pg3_bronze__tpch.region r ON n.n_regionkey = r.r_regionkey) g
# MAGIC     ON c.c_nationkey = g.nation_key
# MAGIC ) AS s
# MAGIC ON t.order_key = s.order_key AND t.line_number = s.line_number
# MAGIC WHEN MATCHED THEN UPDATE SET order_date = s.order_date, order_priority = s.order_priority,
# MAGIC   customer_key = s.customer_key, customer_name = s.customer_name, market_segment = s.market_segment,
# MAGIC   nation = s.nation, region = s.region, part_key = s.part_key, quantity = s.quantity,
# MAGIC   extended_price = s.extended_price, discount = s.discount, tax = s.tax, net_revenue = s.net_revenue,
# MAGIC   return_flag = s.return_flag, ship_date = s.ship_date, ship_mode = s.ship_mode, _updated_at = s._updated_at
# MAGIC WHEN NOT MATCHED THEN INSERT VALUES (s.order_key, s.line_number, s.order_date, s.order_priority, s.customer_key,
# MAGIC   s.customer_name, s.market_segment, s.nation, s.region, s.part_key, s.quantity, s.extended_price, s.discount,
# MAGIC   s.tax, s.net_revenue, s.return_flag, s.ship_date, s.ship_mode, s._updated_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT snapshot_id, committed_at, operation FROM playground3.pg3_silver__sales."order_lines$snapshots"
# MAGIC ORDER BY committed_at DESC LIMIT 5
