# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Auto Loader: landing -> bronze (Delta)
# MAGIC Read JSON files from landing and loads them to Delta table in `gabrielajaniszews786_bronze`.
# MAGIC Idempotent: the checkpoint tracks which files were already loaded, so re-runs don't duplicate data.
# MAGIC Adds metadata columns (source filename, ingestion timestamp, load date). Trigger `availableNow` processes all available files, then stops (it doesn't run 24/7).

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date

BASE       = "abfss://gabrielajaniszews786@dlspl21databricks.dfs.core.windows.net"
LANDING    = "/Volumes/dbr_dev/gabrielajaniszews786_bronze/entsoe_landing"
SCHEMA_LOC = f"{BASE}/_schema/entsoe_prices" # Schema location
CHECKPOINT = f"{BASE}/_checkpoint/entsoe_prices" # Checkpoint
TARGET     = "dbr_dev.gabrielajaniszews786_bronze.entsoe_prices"

# COMMAND ----------

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", SCHEMA_LOC)
      .option("cloudFiles.inferColumnTypes", "true")   # To prevent prices from being read as strings
      .load(LANDING)
      # --- Metadata columns required in the task ---
      .selectExpr("*",
                  "_metadata.file_name as source_file",         # source filename
                  "_metadata.file_path as source_path")
      .withColumn("ingestion_ts", current_timestamp())          # ingestion timestamp
      .withColumn("load_date",    current_date()))              # load date

# COMMAND ----------

(df.writeStream
   .format("delta")
   .option("checkpointLocation", CHECKPOINT)   # Indempotency: tracks processed files
   .trigger(availableNow=True)                 # Process outstanding files
   .toTable(TARGET))

# COMMAND ----------

# Verification check
cnt = spark.table("dbr_dev.gabrielajaniszews786_bronze.entsoe_prices").count()
assert cnt > 0, "Bronze empty - ingestion problem"

# COMMAND ----------

# Idempotency test

spark.table("dbr_dev.gabrielajaniszews786_bronze.entsoe_prices").count()
