# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: drop taxi trips as JSON files
# MAGIC
# MAGIC Simulates an upstream system delivering files. Each run writes one new batch of
# MAGIC `samples.nyctaxi.trips` to the landing volume as JSON, under a run-specific folder,
# MAGIC so the `nyctaxi_trips` pipeline's Auto Loader source has something new to ingest
# MAGIC every time.

# COMMAND ----------

from datetime import UTC, datetime

from pyspark.sql import functions as F

dbutils.widgets.text("volume_path", "/Volumes/pg3-bronze/raw/files")
dbutils.widgets.text("batch_rows", "5000")

volume_path = dbutils.widgets.get("volume_path")
batch_rows = int(dbutils.widgets.get("batch_rows"))
batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
# "batch-", not "batch=": Auto Loader would read key=value folders as a partition column.
target = f"{volume_path}/nyctaxi/batch-{batch_id}"

# COMMAND ----------

trips = (
    spark.read.table("samples.nyctaxi.trips")
    .orderBy(F.rand())
    .limit(batch_rows)
    # Inject a few bad records so the pipeline's expectations have something to catch.
    .withColumn(
        "fare_amount",
        F.when(F.rand() < 0.01, F.lit(-1.0)).otherwise(F.col("fare_amount")),
    )
    .withColumn("batch_id", F.lit(batch_id))
)

trips.coalesce(4).write.mode("overwrite").json(target)

# COMMAND ----------

files = dbutils.fs.ls(target)
print(f"Wrote {batch_rows} trips to {target} in {len([f for f in files if f.name.endswith('.json')])} files")
