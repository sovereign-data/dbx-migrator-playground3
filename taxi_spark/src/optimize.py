"""Iceberg maintenance on everything the job wrote: compaction, Z-ordering, statistics.

Ported from Delta: OPTIMIZE ... ZORDER BY becomes Iceberg's rewrite_data_files (the gateway translates it),
ANALYZE TABLE is left out (Iceberg keeps column statistics in its manifests), and DESCRIBE DETAIL reads the
table's `files` metadata table.
"""

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
    # ANALYZE TABLE ... COMPUTE STATISTICS is Delta's: Iceberg writes each column's counts and bounds into its
    # manifests as it writes. Its compute_table_stats procedure (distinct-count sketches) is refused by the
    # governed engine: it reads the table without a name the masks could be checked against.
    spark.sql(f"OPTIMIZE {schema}.tpch_q3_shipping_priority").show(truncate=False)

    for row in spark.sql(f"SHOW TABLES IN {schema}").collect():
        files = spark.sql(
            f"SELECT count(*) AS n, coalesce(sum(file_size_in_bytes), 0) AS bytes FROM {schema}.{row.tableName}.files"
        ).first()
        print(f"{row.tableName}: {files.n} files, {files.bytes / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
