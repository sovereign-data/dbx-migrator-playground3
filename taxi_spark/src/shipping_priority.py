"""TPC-H Query 3 (shipping priority), written twice: DataFrame API and Spark SQL.

Both versions must agree; the job fails if they do not. Useful as a migration check in its
own right -- run it before and after, compare the output tables.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SEGMENT = "BUILDING"
CUTOFF = "1995-03-15"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    tpch = "playground3.pg3_bronze__tpch"  # hand port: samples is Databricks only; the migration copied TPC-H here

    customer = spark.read.table(f"{tpch}.customer")
    orders = spark.read.table(f"{tpch}.orders")
    lineitem = spark.read.table(f"{tpch}.lineitem")

    dataframe_q3 = (
        customer.where(F.col("c_mktsegment") == SEGMENT)
        .join(orders.where(F.col("o_orderdate") < F.lit(CUTOFF)), F.col("c_custkey") == F.col("o_custkey"))
        .join(lineitem.where(F.col("l_shipdate") > F.lit(CUTOFF)), F.col("o_orderkey") == F.col("l_orderkey"))
        .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
        .orderBy(F.desc("revenue"), "o_orderdate")
        .limit(10)
    )

    sql_q3 = spark.sql(
        f"""
        SELECT l_orderkey, o_orderdate, o_shippriority,
               sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM {tpch}.customer
        JOIN {tpch}.orders   ON c_custkey = o_custkey
        JOIN {tpch}.lineitem ON l_orderkey = o_orderkey
        WHERE c_mktsegment = :segment
          AND o_orderdate < :cutoff
          AND l_shipdate  > :cutoff
        GROUP BY l_orderkey, o_orderdate, o_shippriority
        ORDER BY revenue DESC, o_orderdate
        LIMIT 10
        """,
        args={"segment": SEGMENT, "cutoff": CUTOFF},
    )

    mismatches = dataframe_q3.exceptAll(sql_q3).count() + sql_q3.exceptAll(dataframe_q3).count()
    if mismatches:
        raise RuntimeError(f"DataFrame and SQL versions of Q3 disagree on {mismatches} rows")

    target = f"`{args.catalog}`.{args.schema}.tpch_q3_shipping_priority"
    (
        dataframe_q3.withColumn("segment", F.lit(SEGMENT))
        .withColumn("cutoff_date", F.lit(CUTOFF).cast("date"))
        .withColumn("computed_at", F.current_timestamp())
        .write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    spark.read.table(target).show(truncate=False)


if __name__ == "__main__":
    main()
