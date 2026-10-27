"""Delta maintenance on everything the job wrote: compaction, Z-ordering, statistics."""

import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    schema = f"`{args.catalog}`.{args.schema}"

    spark.sql(f"OPTIMIZE {schema}.trip_features ZORDER BY (pickup_zip, pickup_hour)").show(truncate=False)
    spark.sql(f"ANALYZE TABLE {schema}.trip_features COMPUTE STATISTICS FOR ALL COLUMNS")
    spark.sql(f"OPTIMIZE {schema}.tpch_q3_shipping_priority")

    for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
        detail = spark.sql(f"DESCRIBE DETAIL {schema}.{row.tableName}").first()
        print(f"{row.tableName}: {detail.numFiles} files, {detail.sizeInBytes / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
