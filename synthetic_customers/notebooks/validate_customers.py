# Databricks notebook source
# MAGIC %md
# MAGIC # Validate synthetic customers
# MAGIC
# MAGIC A data contract check with [pandera](https://pandera.readthedocs.io). The library is not
# MAGIC on the serverless base image: it comes from `requirements.txt`, which the job's
# MAGIC `validate` environment installs before this notebook starts.

# COMMAND ----------

import pandera.pandas as pa
from pandera.pandas import Check, Column

dbutils.widgets.text("catalog", "pg3-bronze")
dbutils.widgets.text("schema", "synthetic")
target = f"`{dbutils.widgets.get('catalog')}`.{dbutils.widgets.get('schema')}"

# COMMAND ----------

contract = pa.DataFrameSchema(
    {
        "customer_id": Column(str, Check.str_matches(r"^C\d{8}$"), unique=True),
        "email": Column(str, Check.str_contains("@"), unique=True),
        "country_code": Column(str, Check.isin(["NL", "DE", "FR", "GB", "US"])),
        "segment": Column(str, Check.isin(["consumer", "smb", "enterprise"])),
        "lifetime_value": Column(float, Check.ge(0)),
    },
    strict=False,
)

# A sample is enough for a contract check and keeps this cheap.
sample = (
    spark.read.table(f"{target}.customers")
    .sample(fraction=0.2, seed=7)
    .selectExpr("*", "CAST(lifetime_value AS DOUBLE) AS lifetime_value_d")
    .drop("lifetime_value")
    .withColumnRenamed("lifetime_value_d", "lifetime_value")
    .toPandas()
)

# lazy=True collects every failure instead of stopping at the first.
contract.validate(sample, lazy=True)
print(f"Contract holds on {len(sample)} sampled customers.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT segment, country_code, count(*) AS customers, round(avg(lifetime_value), 2) AS avg_ltv
        FROM {target}.customers
        GROUP BY ALL
        ORDER BY customers DESC
    """)
)
