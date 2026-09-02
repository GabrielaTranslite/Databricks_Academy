from pyspark import pipelines as dp
from pyspark.sql import functions as F
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from silver_transformations import clean_sensor, clean_prices

# Lab 7 — Lakeflow declarative pipeline: bronze -> silver -> gold with DBX suite.
# Sources: prices_bronze = JSON files (file source), sensor_bronze = streaming (Delta stream).
# Silver: expectations replace Lab 4 .filter()/CHECK; dedup via create_auto_cdc_flow replaces MERGE.
# Params come from the pipeline configuration (spark.conf.get), not widgets.

dq = DQEngine(WorkspaceClient())

CATALOG       = spark.conf.get("catalog")
BRONZE_SCHEMA = spark.conf.get("bronze_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")
LANDING       = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/entsoe_landing/prices"

# Single source of truth for table names
TABLES = {
    "prices_bronze": "prices_bronze",
    "sensor_bronze": "sensor_bronze",
    "checked_prices": "checked_prices",
    "valid_prices": "valid_prices",
    "quarantine_prices": "quarantine_prices",
    "checked_sensor": "checked_sensor",
    "valid_sensor": "valid_sensor",
    "quarantine_sensor": "quarantine_sensor",
    "sensor_silver": "sensor_silver",
    "silver_datacenter": "silver_datacenter",
    "consumption_hourly": "consumption_hourly",
    "dim_datacenter": "dim_datacenter",
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

# --- DQX quality checks (metadata form) ---
prices_checks = [
    # completeness
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "bidding_zone"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "price"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "timestamp_utc"}}},

    # validity
    {"criticality": "error", "check": {"function": "is_not_less_than", "arguments": {"column": "price", "limit": 0}}},
    {"criticality": "error", "check": {"function": "is_in_list",
        "arguments": {"column": "bidding_zone", "allowed": ["PL","DE_LU","FR","ES","CZ","SK","LT","PT"]}}},
    {"criticality": "warn",  "check": {"function": "is_in_list", "arguments": {"column": "currency", "allowed": ["EUR"]}}},
    {"criticality": "warn",  "check": {"function": "is_in_list", "arguments": {"column": "unit", "allowed": ["MWH"]}}},

    # uniqueness (dataset-level): one price per zone and timestamp_utc
    {"criticality": "error", "check": {"function": "is_unique", "arguments": {"columns": ["bidding_zone", "timestamp_utc"]}}},

    # timeliness - data is fresh for 1 day
    {"criticality": "warn",  "check": {"function": "is_data_fresh", "arguments": {"column": "ingestion_ts", "max_age_minutes": 1440}}},
]

sensor_checks = [
    # completeness
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "event_id"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "bidding_zone"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "site_id"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "timestamp_utc"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "consumption_kwh"}}},
    {"criticality": "error", "check": {"function": "is_not_null", "arguments": {"column": "pue"}}},

    # validity
    {"criticality": "error", "check": {"function": "is_in_range", "arguments": {"column": "pue", "min_limit": 1.0, "max_limit": 3.0}}},
    {"criticality": "error", "check": {"function": "is_not_less_than", "arguments": {"column": "consumption_kwh", "limit": 0}}},
    {"criticality": "warn",  "check": {"function": "is_not_less_than", "arguments": {"column": "avg_power_kw", "limit": 0}}},
    {"criticality": "error", "check": {"function": "is_in_list",
        "arguments": {"column": "bidding_zone", "allowed": ["PL","DE_LU","FR","ES","CZ","SK","LT","PT"]}}},

    # consistency (cross-column, SQL): if there is consumption, there must be power draw
    {"criticality": "warn", "check": {"function": "sql_expression",
        "arguments": {"expression": "consumption_kwh = 0 OR avg_power_kw > 0",
                      "msg": "consumption without power draw", "name": "consumption_power_consistency"}}},

    # timeliness: sensor stream is fresh for 2 hours
    {"criticality": "warn", "check": {"function": "is_data_fresh", "arguments": {"column": "timestamp_utc", "max_age_minutes": 120}}},
]
#---PRICES----
# Prices silver: transform and apply DQX
@dp.materialized_view(name = TABLES["checked_prices"])
def checked_prices():
    df = clean_prices(spark.read.table(TABLES["prices_bronze"]))
    return dq.apply_checks_by_metadata(df, prices_checks)

# Valid prices silver — good rows only, drop DQX helper cols
@dp.materialized_view(name=TABLES["valid_prices"])
def valid_prices():
    return (spark.read.table(TABLES["checked_prices"])
            .filter("_errors IS NULL")
            .drop("_errors", "_warnings"))
    
# Quarantine prices — bad rows, keep the DQX error detail
@dp.materialized_view(name=TABLES["quarantine_prices"])
def quarantine_prices():
    return spark.read.table(TABLES["checked_prices"]).filter("_errors IS NOT NULL")

#---SENSOR----    
# Sensor silver: transform and apply DQX
@dp.table(name=TABLES["checked_sensor"])
def checked_sensor():
    df = clean_sensor(spark.readStream.table(TABLES["sensor_bronze"]))
    return dq.apply_checks_by_metadata(df, sensor_checks)

# Valid sensor silver — good rows only, drop DQX helper cols
@dp.table(name=TABLES["valid_sensor"])
def valid_sensor():
    return (spark.readStream.table(TABLES["checked_sensor"])
            .filter("_errors IS NULL")
            .drop("_errors", "_warnings"))
    
# Quarantine sensor — bad rows, keep the DQX error detail
@dp.table(name=TABLES["quarantine_sensor"])
def quarantine_sensor():
    return spark.readStream.table(TABLES["checked_sensor"]).filter("_errors IS NOT NULL")

    
# Sensor deduplicaton: keep the newest row per key (latest ingestion_ts). The engine does the idempotent upsert.
dp.create_streaming_table(TABLES["sensor_silver"])          # 1) empty target
dp.create_auto_cdc_flow(                                    # 2) the rule
    target      = TABLES["sensor_silver"],
    source      = TABLES["valid_sensor"],
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

@dp.materialized_view(name=f"{CATALOG}.{GOLD_SCHEMA}.consumption_hourly")
def consumption_hourly():
    return spark.sql(f""" 
          SELECT 
          s.site_id,
          s.bidding_zone,
          DATE(s.timestamp_utc) AS date,
          HOUR(s.timestamp_utc) AS hour,
          ROUND(AVG(s.consumption_kwh), 2) AS avg_consumption_kwh,
          ROUND(AVG(s.avg_power_kw), 2) AS avg_power_kw,
          ROUND(AVG(s.pue), 3) AS avg_pue,
          CAST(AVG((s.consumption_kwh * p.price) / 1000) AS DECIMAL(10,2)) as cost_per_hour
          FROM sensor_silver AS s
          LEFT JOIN valid_prices AS p
          ON DATE_TRUNC('hour', s.timestamp_utc) = DATE_TRUNC('hour', p.timestamp_utc) 
             AND s.bidding_zone = p.bidding_zone
          GROUP BY s.bidding_zone, s.site_id, DATE(s.timestamp_utc), HOUR(s.timestamp_utc)""")

@dp.materialized_view(name=f"{CATALOG}.{GOLD_SCHEMA}.dim_datacenter")
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


