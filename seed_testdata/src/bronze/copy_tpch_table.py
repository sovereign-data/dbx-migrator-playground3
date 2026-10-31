# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: copy one TPC-H table
# MAGIC
# MAGIC Run once per table by the `for_each` task in `seed_testdata`. A straight CTAS from the
# MAGIC TPC-H source, plus ingestion metadata, so every bronze table carries where and when it came from.
# MAGIC
# MAGIC Hand port for the portal: the SQL widgets, `USE CATALOG IDENTIFIER(...)` and `IDENTIFIER(:table_name)`
# MAGIC become Python widgets and fully qualified names, checked before they go into the statement (SQE has no
# MAGIC `IDENTIFIER()`, and the portal refuses a catalog it cannot read from the text). The Unity names map as the
# MAGIC migration put them: `pg3-bronze`.`tpch` is `playground3.pg3_bronze__tpch`. The Delta properties (change
# MAGIC data feed) are left out; Iceberg keeps every snapshot. `samples.tpch` is the Databricks share: point
# MAGIC `source` at a catalog that holds TPC-H here.

# COMMAND ----------

import re

dbutils.widgets.text("bronze_catalog", "playground3")
dbutils.widgets.text("bronze_schema", "pg3_bronze__tpch")
dbutils.widgets.text("source", "samples.tpch")
dbutils.widgets.text("table_name", "region")

TPCH = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
NAME = re.compile(r"[a-z_][a-z0-9_]*")


def _name(value: str, what: str) -> str:
    parts = value.strip().lower().split(".")
    if not all(NAME.fullmatch(p) for p in parts):
        raise ValueError(f"{what} is not a plain name: {value!r}")
    return ".".join(parts)


table_name = dbutils.widgets.get("table_name").strip().lower()
if table_name not in TPCH:
    raise ValueError(f"table_name must be one of {sorted(TPCH)}, not {table_name!r}")
target = f"{_name(dbutils.widgets.get('bronze_catalog'), 'bronze_catalog')}.{_name(dbutils.widgets.get('bronze_schema'), 'bronze_schema')}.{table_name}"
source = f"{_name(dbutils.widgets.get('source'), 'source')}.{table_name}"
print(f"{source} -> {target}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {target}
COMMENT 'Raw copy of TPC-H, refreshed by the seed_testdata job.'
AS
SELECT
  src.*,
  current_timestamp  AS _ingested_at,
  '{source}'         AS _source
FROM {source} AS src
""")

# COMMAND ----------

display(spark.sql(f"SELECT '{table_name}' AS table_name, count(*) AS row_count FROM {target}"))
