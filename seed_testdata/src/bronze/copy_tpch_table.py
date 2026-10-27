# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: copy one TPC-H table
# MAGIC
# MAGIC Run once per table by the `for_each` task in `seed_testdata`. A straight CTAS from the
# MAGIC read-only `samples.tpch` share, plus ingestion metadata, so every bronze table carries
# MAGIC where and when it came from.
# MAGIC
# MAGIC Catalog names contain a hyphen (`pg3-bronze`), so they are backtick-quoted before being
# MAGIC handed to `IDENTIFIER()`.

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#2.1 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT bronze_catalog DEFAULT 'pg3-bronze'
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#2.2 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT bronze_schema DEFAULT 'tpch'
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#2.3 is needs_review for sqe (notebook_parameter); not run
# -- CREATE WIDGET TEXT table_name DEFAULT 'region'

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#3.4 is needs_review for sqe (session_statement); not run
# -- USE CATALOG IDENTIFIER('`' || :bronze_catalog || '`')
#
# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#3.5 is needs_review for sqe (session_statement); not run
# -- USE SCHEMA IDENTIFIER(:bronze_schema)

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#4.6 is needs_review for sqe (delta_property, dynamic_identifier); not run
# -- CREATE OR REPLACE TABLE IDENTIFIER(:table_name)
# -- COMMENT 'Raw copy of samples.tpch, refreshed by the seed_testdata job.'
# -- TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'layer' = 'bronze')
# -- AS
# -- SELECT
# --   src.*,
# --   current_timestamp()                     AS _ingested_at,
# --   'samples.tpch.' || :table_name          AS _source
# -- FROM IDENTIFIER('samples.tpch.' || :table_name) AS src

# COMMAND ----------

# -- dbx-migrator: sql:playground3-1027/Users/4438510214518295/.bundle/seed_testdata/playground3/files/src/bronze/copy_tpch_table#5.7 is needs_review for sqe (dynamic_identifier); not run
# -- SELECT :table_name AS table_name, count(*) AS row_count
# -- FROM IDENTIFIER(:table_name)
