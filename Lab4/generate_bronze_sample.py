# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic bronze generator (Free Edition seed)
# MAGIC Creates `entsoe_prices` and `sensor_data` bronze tables with synthetic but
# MAGIC realistic data, so silver can be developed and tested without internet egress
# MAGIC (ENTSO-E API) or an Event Hub. Schemas match what the silver notebooks read.
# MAGIC
# MAGIC The data is deliberately imperfect: a small share of duplicated rows (to exercise
# MAGIC dedup / MERGE) and a few null prices (to exercise the data quality rules).
# MAGIC Run this on Free Edition, then run the silver notebooks against the result.

# COMMAND ----------

dbutils.widgets.combobox("catalog", "workspace", ["workspace", "dbr_dev"], "Unity Catalog")
dbutils.widgets.combobox("bronze_schema", "bronze", ["bronze", "gabrielajaniszews786_bronze"], "Bronze schema")
dbutils.widgets.text("n_days", "14", "Days of history")
dbutils.widgets.text("seed", "42", "Random seed")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("bronze_schema")
N_DAYS  = int(dbutils.widgets.get("n_days"))
SEED    = int(dbutils.widgets.get("seed"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

import math, random, uuid
from datetime import datetime, timedelta, timezone, date
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType,
                               IntegerType, LongType, TimestampType, DateType)

rng = random.Random(SEED)

# The eight sites / bidding zones used across the project
SITES = [
    # site_id,     site_name,        country, bidding_zone
    ("DC-PL-01",  "Warsaw DC",       "PL",    "PL"),
    ("DC-DE-01",  "Frankfurt DC",    "DE",    "DE_LU"),
    ("DC-FR-01",  "Paris DC",        "FR",    "FR"),
    ("DC-ES-01",  "Barcelona DC",    "ES",    "ES"),
    ("DC-CZ-01",  "Brno DC",         "CZ",    "CZ"),
    ("DC-SK-01",  "Bratislava DC",   "SK",    "SK"),
    ("DC-LT-01",  "Vilnius DC",      "LT",    "LT"),
    ("DC-PT-01",  "Lisbon DC",       "PT",    "PT"),
]

TODAY = datetime.now(timezone.utc).date()
NOW   = datetime.now(timezone.utc)

def iso_z(dt):
    """Format a datetime as the ISO string used in the source, e.g. 2026-08-05T14:00:00Z."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

# MAGIC %md ## Prices (fact) - one row per zone per hour

# COMMAND ----------

# Zone base price levels (EUR/MWh); the shape is a daily curve plus noise.
ZONE_BASE = {"PL": 95, "DE_LU": 88, "FR": 82, "ES": 78, "CZ": 91, "SK": 93, "LT": 99, "PT": 76}

price_rows = []
for site_id, site_name, country, zone in SITES:
    base = ZONE_BASE[zone]
    for d in range(1, N_DAYS + 1):
        day = datetime.combine(TODAY - timedelta(days=d), datetime.min.time(), tzinfo=timezone.utc)
        for hour in range(24):
            ts = day + timedelta(hours=hour)
            # daily shape: cheaper at night, peaks morning/evening
            shape = 25 * math.sin((hour - 6) / 24 * 2 * math.pi)
            price = round(base + shape + rng.uniform(-12, 12), 2)
            # occasional negative price (renewable oversupply) - still a valid value
            if rng.random() < 0.01:
                price = round(rng.uniform(-40, -1), 2)
            # a few missing prices to exercise the DQ filter (price IS NOT NULL)
            if rng.random() < 0.004:
                price = None
            fname = f"prices_{zone}_{ts.strftime('%Y%m%d')}.json"
            price_rows.append((
                iso_z(ts), price, "EUR", "MWH", zone, None,        # source columns (country left null = legacy)
                fname, f"/Volumes/{CATALOG}/{SCHEMA}/entsoe_landing/{fname}",
                NOW, TODAY, None                                    # metadata columns
            ))

# Inject duplicates with a LATER ingestion_ts so dedup must keep the most recent one
dupes = rng.sample(price_rows, k=max(1, len(price_rows) // 40))
for r in dupes:
    r2 = list(r)
    r2[1] = round((r[1] or 50) + rng.uniform(-3, 3), 2)   # slightly different price on the newer copy
    r2[8] = NOW + timedelta(minutes=5)                    # newer ingestion_ts
    price_rows.append(tuple(r2))

price_schema = StructType([
    StructField("timestamp_utc", StringType()),
    StructField("price", DoubleType()),
    StructField("currency", StringType()),
    StructField("unit", StringType()),
    StructField("bidding_zone", StringType()),
    StructField("country", StringType()),
    StructField("source_file", StringType()),
    StructField("source_path", StringType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("load_date", DateType()),
    StructField("_rescued_data", StringType()),
])

(spark.createDataFrame(price_rows, price_schema)
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.entsoe_prices"))

print("entsoe_prices rows:", spark.table(f"{CATALOG}.{SCHEMA}.entsoe_prices").count())

# COMMAND ----------

# MAGIC %md ## Sensors (fact) - one reading per site per hour

# COMMAND ----------

sensor_rows = []
offset = 0
for site_id, site_name, country, zone in SITES:
    for d in range(1, N_DAYS + 1):
        day = datetime.combine(TODAY - timedelta(days=d), datetime.min.time(), tzinfo=timezone.utc)
        for hour in range(24):
            ts = day + timedelta(hours=hour)
            consumption = round(rng.uniform(9.5, 15.5), 4)
            avg_power   = round(rng.uniform(700, 800), 2)
            pue         = round(rng.uniform(1.20, 1.45), 3)   # always >= 1.0, within the CHECK range
            sensor_rows.append((
                str(uuid.uuid4()), 1, site_id, site_name, country, zone,
                iso_z(ts), consumption, avg_power, pue, 3600,
                0, offset, ts, NOW
            ))
            offset += 1

# Inject duplicate events (same event_id) with a later ingestion_ts to test event_id dedup
dupes = rng.sample(sensor_rows, k=max(1, len(sensor_rows) // 40))
for r in dupes:
    r2 = list(r)
    r2[14] = NOW + timedelta(minutes=5)   # newer ingestion_ts, same event_id
    sensor_rows.append(tuple(r2))

sensor_schema = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("site_id", StringType()),
    StructField("site_name", StringType()),
    StructField("country", StringType()),
    StructField("bidding_zone", StringType()),
    StructField("timestamp_utc", StringType()),
    StructField("consumption_kwh", DoubleType()),
    StructField("avg_power_kw", DoubleType()),
    StructField("pue", DoubleType()),
    StructField("reading_interval_s", IntegerType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
    StructField("enqueued_ts", TimestampType()),
    StructField("ingestion_ts", TimestampType()),
])

(spark.createDataFrame(sensor_rows, sensor_schema)
    .write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.sensor_data"))

print("sensor_data rows:", spark.table(f"{CATALOG}.{SCHEMA}.sensor_data").count())

# COMMAND ----------

# MAGIC %md ## Quick sanity check

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'entsoe_prices' AS tbl,
           COUNT(*)                                    AS rows,
           COUNT(DISTINCT timestamp_utc, bidding_zone) AS unique_key,
           SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_prices
    FROM {CATALOG}.{SCHEMA}.entsoe_prices
    UNION ALL
    SELECT 'sensor_data',
           COUNT(*), COUNT(DISTINCT event_id), 0
    FROM {CATALOG}.{SCHEMA}.sensor_data
"""))

