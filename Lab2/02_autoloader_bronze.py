# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2 — Auto Loader: landing -> bronze (Delta)
# MAGIC Czyta pliki JSON z landing i ładuje je do tabeli Delta w `gabrielajaniszews786_bronze`.
# MAGIC Idempotentnie (checkpoint pamięta wczytane pliki) + kolumny metadanych
# MAGIC (source filename, ingestion timestamp, load date). Trigger `availableNow` — przetworzy
# MAGIC zaległości i kończy (nie chodzi 24/7).

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date

BASE       = "abfss://gabrielajaniszews786@dlspl21databricks.dfs.core.windows.net"
LANDING    = "/Volumes/dbr_dev/gabrielajaniszews786_bronze/entsoe_landing"
SCHEMA_LOC = f"{BASE}/_schema/entsoe_prices"
CHECKPOINT = f"{BASE}/_checkpoint/entsoe_prices"
TARGET     = "dbr_dev.gabrielajaniszews786_bronze.entsoe_prices"

# COMMAND ----------

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", SCHEMA_LOC)
      .option("cloudFiles.inferColumnTypes", "true")   # inaczej ceny wejdą jako string
      .load(LANDING)
      # --- kolumny metadanych wymagane przez zadanie ---
      .selectExpr("*",
                  "_metadata.file_name as source_file",         # source filename
                  "_metadata.file_path as source_path")
      .withColumn("ingestion_ts", current_timestamp())          # ingestion timestamp
      .withColumn("load_date",    current_date()))              # load date

# COMMAND ----------

(df.writeStream
   .format("delta")
   .option("checkpointLocation", CHECKPOINT)   # idempotencja: pamięta, które pliki już wczytane
   .trigger(availableNow=True)                 # przetwórz zaległości i zakończ
   .toTable(TARGET))

# COMMAND ----------

# MAGIC %md ### Weryfikacja

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                AS wiersze,
# MAGIC        count(DISTINCT source_file) AS pliki,
# MAGIC        min(timestamp_utc)      AS od,
# MAGIC        max(timestamp_utc)      AS do
# MAGIC FROM dbr_dev.gabrielajaniszews786_bronze.entsoe_prices;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbr_dev.gabrielajaniszews786_bronze.entsoe_prices LIMIT 20;

