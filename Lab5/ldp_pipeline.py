from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Lab 5 — Lakeflow declarative pipeline: bronze -> silver -> gold.
# Sources: prices_bronze = JSON files (file source), sensor_bronze = streaming (Delta stream).
# Silver: expectations replace Lab 4 .filter()/CHECK; dedup via create_auto_cdc_flow replaces MERGE.
# Params come from the pipeline configuration (spark.conf.get), not widgets.

CATALOG       = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA   = spark.conf.get("gold_schema")
LANDING       = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/entsoe_landing/prices"

# Single source of truth for table names
TABLES = {
    "prices_bronze": "prices_bronze",
    "prices_silver": "prices_silver",
    "sensor_bronze": "sensor_bronze",
    "sensor_silver_clean": "sensor_silver_clean",
    "sensor_silver": "sensor_silver",
    "sensor_daily": "sensor_daily"
    "silver_datacenter": "silver_datacenter"
    "consumption_hourly": "consumption_hourly"
    "dim_datacenter": "dim_datacenter"
    "dim_date": "dim_date"
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
# Datacenter table based on sensor data
@dp.materialized_view(name = TABLES["silver_datacenter"])
def silver_datacenter():
    return spark.sql(f"""
        SELECT DISTINCT
            site_id,
            site_name,
            country,
            bidding_zone,
            current_timestamp()       AS valid_from,
            CAST(NULL AS TIMESTAMP)    AS valid_to,
            true                       AS is_current
        FROM sensor_silver
        """)

# =============================================
# GOLD — one fact table and two dimensions
# =============================================

@dp.materialized_view(name=TABLES["consumption_hourly"])
def consumption_hourly():
    return spark.sql(f""" 
          SELECT 
          s.site_id,
          s.bidding_zone,
          DATE(s.timestamp_utc) AS date,
          HOUR(s.timestamp_utc) AS hour,
          AVG(s.consumption_kwh) AS avg_consumption_kwh,
          AVG(s.avg_power_kw) AS avg_power_kw,
          AVG(s.pue) AS avg_pue,
          AVG((s.consumption_kwh * p.price) / 1000) as cost_per_hour
          FROM sensor_silver AS s
          LEFT JOIN prices AS p
          ON DATE_TRUNC('hour', s.timestamp_utc) = DATE_TRUNC('hour', p.timestamp_utc) 
             AND s.bidding_zone = p.bidding_zone
          GROUP BY s.bidding_zone, s.site_id, DATE(s.timestamp_utc), HOUR(s.timestamp_utc)""")

@dp.materialized_view(name=TABLES["dim_datacenter"])
def dim_datacenter():
    return spark.sql(f""" 
          SELECT    
          site_id, 
          site_name, 
          country, 
          bidding_zone, 
          valid_from, 
          valid_to, 
          is_current 
          FROM silver_datacenter""")

# Dim table with different time grains
@dp.materialized_view(name=TABLES["dim_date"])
def dim_date():
    return spark.sql(f""" 
          SELECT
            date,
            month(date) AS month,
            day(date) AS day,
            weekofyear(date) AS week,
            year(date) AS year,
            dayofweek(date) AS day_of_week,

            CASE dayofweek(date)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
                ELSE 'Unknown'
            END AS day_of_week_name,
            weekofyear(date) AS week_of_year,
            date_format(date, 'MMMM') AS month_name,
            quarter(date) AS quarter,
            dayofweek(date) IN (1, 7) AS is_weekend
            FROM (
                SELECT DISTINCT
                    CAST(date AS DATE) AS date
                FROM consumption_hourly
            )
            """)
