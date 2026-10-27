"""Trip-level features over samples.nyctaxi.trips.

A tour of the DataFrame API a migration has to carry across intact: derived columns,
window functions (ranking and a time-range rolling average), a broadcast join against a
small dimension, and a partitioned Delta write with replaceWhere.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def zip_profile(trips: DataFrame) -> DataFrame:
    """A small per-ZIP dimension: small enough to broadcast to every executor."""
    return trips.groupBy(F.col("pickup_zip").alias("zip")).agg(
        F.count("*").alias("zip_trip_count"),
        F.percentile_approx("fare_amount", 0.5).alias("zip_median_fare"),
    )


def features(trips: DataFrame) -> DataFrame:
    trips = (
        trips.where((F.col("fare_amount") > 0) & (F.col("trip_distance") > 0))
        .withColumn("duration_min", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
        .where(F.col("duration_min").between(1, 180))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
        .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
    )

    by_zip_day = Window.partitionBy("pickup_zip", "pickup_date")
    # Rolling one-hour window per pickup ZIP, keyed on epoch seconds.
    rolling_hour = (
        Window.partitionBy("pickup_zip")
        .orderBy(F.col("tpep_pickup_datetime").cast("long"))
        .rangeBetween(-3600, 0)
    )

    return (
        trips.withColumn("fare_rank_in_zip_day", F.dense_rank().over(by_zip_day.orderBy(F.desc("fare_amount"))))
        .withColumn("share_of_zip_day_fare", F.round(F.col("fare_amount") / F.sum("fare_amount").over(by_zip_day), 4))
        .withColumn("rolling_1h_avg_fare", F.round(F.avg("fare_amount").over(rolling_hour), 2))
        .withColumn("trips_last_hour_from_zip", F.count("*").over(rolling_hour))
        .join(F.broadcast(zip_profile(trips)), F.col("pickup_zip") == F.col("zip"), "left")
        .withColumn("fare_vs_zip_median", F.round(F.col("fare_amount") - F.col("zip_median_fare"), 2))
        .drop("zip")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    target = f"`{args.catalog}`.{args.schema}.trip_features"

    result = features(spark.read.table("samples.nyctaxi.trips"))

    if spark.catalog.tableExists(target):
        # Rewrite only the dates present in this batch; other partitions are untouched.
        dates = [r.pickup_date for r in result.select("pickup_date").distinct().collect()]
        predicate = "pickup_date IN ({})".format(", ".join(f"'{d}'" for d in dates))
        result.write.mode("overwrite").option("replaceWhere", predicate).saveAsTable(target)
    else:
        result.write.partitionBy("pickup_date").saveAsTable(target)
        spark.sql(f"COMMENT ON TABLE {target} IS 'Per-trip features: rankings, rolling averages, ZIP baselines.'")

    print(f"{target}: {spark.read.table(target).count()} rows")


if __name__ == "__main__":
    main()
