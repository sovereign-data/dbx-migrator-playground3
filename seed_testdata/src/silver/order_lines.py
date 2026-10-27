# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: conformed order lines
# MAGIC
# MAGIC Joins the bronze TPC-H tables into one denormalised fact at order-line grain,
# MAGIC derives revenue, and upserts it with `MERGE` so reruns are idempotent.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("bronze_catalog", "pg3-bronze")
dbutils.widgets.text("bronze_schema", "tpch")
dbutils.widgets.text("silver_catalog", "pg3-silver")
dbutils.widgets.text("silver_schema", "sales")

bronze = f"`{dbutils.widgets.get('bronze_catalog')}`.{dbutils.widgets.get('bronze_schema')}"
target = f"`{dbutils.widgets.get('silver_catalog')}`.{dbutils.widgets.get('silver_schema')}.order_lines"

# COMMAND ----------


def bronze_table(name):
    # Drop ingestion metadata; silver carries its own.
    return spark.read.table(f"{bronze}.{name}").drop("_ingested_at", "_source")


lineitem = bronze_table("lineitem")
orders = bronze_table("orders")
customer = bronze_table("customer")
nation = bronze_table("nation")
region = bronze_table("region")

geography = (
    nation.join(region, nation.n_regionkey == region.r_regionkey)
    .select(
        F.col("n_nationkey").alias("nation_key"),
        F.initcap("n_name").alias("nation"),
        F.initcap("r_name").alias("region"),
    )
)

order_lines = (
    lineitem.join(orders, lineitem.l_orderkey == orders.o_orderkey)
    .join(customer, orders.o_custkey == customer.c_custkey)
    .join(F.broadcast(geography), customer.c_nationkey == geography.nation_key)
    .select(
        F.col("l_orderkey").alias("order_key"),
        F.col("l_linenumber").alias("line_number"),
        F.col("o_orderdate").alias("order_date"),
        F.col("o_orderpriority").alias("order_priority"),
        F.col("c_custkey").alias("customer_key"),
        F.col("c_name").alias("customer_name"),
        F.col("c_mktsegment").alias("market_segment"),
        "nation",
        "region",
        F.col("l_partkey").alias("part_key"),
        F.col("l_quantity").alias("quantity"),
        F.col("l_extendedprice").alias("extended_price"),
        F.col("l_discount").alias("discount"),
        F.col("l_tax").alias("tax"),
        (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("net_revenue"),
        F.col("l_returnflag").alias("return_flag"),
        F.col("l_shipdate").alias("ship_date"),
        F.col("l_shipmode").alias("ship_mode"),
    )
    .withColumn("_updated_at", F.current_timestamp())
)

# COMMAND ----------

if not spark.catalog.tableExists(target):
    (
        order_lines.write.partitionBy("region")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(target)
    )
    spark.sql(f"COMMENT ON TABLE {target} IS 'TPC-H order lines joined to customer and geography. Grain: one row per order line.'")
else:
    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(
            order_lines.alias("s"),
            "t.order_key = s.order_key AND t.line_number = s.line_number",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {target}").select("version", "timestamp", "operation", "operationMetrics").limit(5))
