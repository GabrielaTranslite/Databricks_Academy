from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Lab 5 — Lakeflow declarative pipeline: bronze -> silver -> gold.
# Sources: prices_bronze = JSON files (file source), sensor_bronze = streaming (Delta stream).
# Silver: expectations replace Lab 4 .filter()/CHECK; dedup via create_auto_cdc_flow replaces MERGE.
# Params come from the pipeline configuration (spark.conf.get), not widgets.

CATALOG       = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
LANDING       = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/entsoe_landing/prices"

# Single source of truth for table names
TABLES = {
    "prices_bronze": "prices_bronze",
    "prices_silver": "prices_silver",
    "sensor_bronze": "sensor_bronze",
    "sensor_silver_clean": "sensor_silver_clean",
    "sensor_silver": "sensor_silver",
    "sensor_daily": "sensor_daily"
    }

# ==============================================
# BRONZE LAYER (two sources: file and streaming)
# ==============================================

# Creating a materialized view from prices data (batch source)
@dp.materialized_view(name = TABLES["prices_bronze"])
def prices_bronze():
    return spark.read.format("json").load(LANDING)

# Creating a streaming source from events data (sensors)
@dp.table(name = TABLES["sensor_bronze"])
def sensor_bronze():
    return spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.sensor_data")


# ==============================================
# SILVER LAYER - data quality + deduplication
# ==============================================

# Prices silver: cast types and drop rows that must not reach silver.
@dp.materialized_view(name = TABLES["prices_silver"])
@dp.expect_all_or_drop({
    "zone_present":  "bidding_zone IS NOT NULL",
    "price_present": "price IS NOT NULL"
})
def prices_silver():
    return (spark.read.table(TABLES["prices_bronze"])
                 .withColumn("price", F.col("price").cast("decimal(10,2)")))
    
# Sensor silver (clean):
# - cast types
# - enforce domain rules (expect_all_or_fail)
@dp.table(name=TABLES["sensor_silver_clean"])
@dp.expect_all_or_drop({                       # bad rows are dropped
    "key_present":  "event_id IS NOT NULL",
    "zone_present": "bidding_zone IS NOT NULL",
})
@dp.expect_all_or_fail({                       # broken domain rules stop the run
    "pue_valid":    "pue BETWEEN 1.0 AND 3.0",
    "kwh_positive": "consumption_kwh >= 0",
})
def sensor_silver_clean():
    return (spark.readStream.table(TABLES["sensor_bronze"])
                 .withColumn("pue",             F.col("pue").cast("decimal(4,3)"))
                 .withColumn("consumption_kwh", F.col("consumption_kwh").cast("decimal(10,4)")))
    
# Sensor deduplicaton: keep the newest row per key (latest ingestion_ts). The engine does the idempotent upsert.
dp.create_streaming_table(TABLES["sensor_silver"])          # 1) empty target
dp.create_auto_cdc_flow(                                    # 2) the rule
    target      = TABLES["sensor_silver"],
    source      = TABLES["sensor_silver_clean"],
    keys        = ["event_id"],
    sequence_by = F.col("ingestion_ts"),
)

# =============================================
# GOLD — a small aggregate
# =============================================
@dp.materialized_view(name=TABLES["sensor_daily"])
def sensor_daily():
    return (spark.read.table(TABLES["sensor_silver"])
                 .groupBy("site_id", F.to_date("timestamp_utc").alias("day"))
                 .agg(F.avg("consumption_kwh").alias("avg_kwh")))

