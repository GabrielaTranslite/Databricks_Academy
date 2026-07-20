# Script for setting a scheduled job
# Configuration

from pyspark.sql.functions import current_timestamp, current_date
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

CATALOG    = "dbr_dev"
STORAGE_ACCOUNT = "dlspl21databricks"
CONTAINER  = "gabrielajaniszews786"
SCHEMA     = "gabrielajaniszews786_bronze"
BASE       = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CHECKPOINT = f"{BASE}/_checkpoint/sensor_data" # Checkpoint
TARGET     = f"{CATALOG}.{SCHEMA}.sensor_data"

EH_NAMESPACE = "evhpl24databricks"
EH_NAME = "gabrielajaniszews786_eventhub"
EH_CONN_STR = dbutils.secrets.get("default2", "eventhub-con-str-gabriela")

BOOTSTRAP = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
JAAS = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";'

# Read from Event Hub
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", EH_NAME)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS)
    .option("startingOffsets", "earliest")   # Reading from the earliest available data
    .load())

# Explicit schema
event_schema = (StructType()
    .add("event_id", StringType())
    .add("schema_version", IntegerType())
    .add("site_id", StringType())
    .add("site_name", StringType())
    .add("country", StringType())
    .add("bidding_zone", StringType())
    .add("timestamp_utc", StringType())
    .add("consumption_kwh", DoubleType())
    .add("avg_power_kw", DoubleType())
    .add("pue", DoubleType())
    .add("reading_interval_s", IntegerType())
)
# Parse the JSON
parsed = (raw
    .select(
        from_json(col("value").cast("string"), event_schema).alias("e"), 
        col("partition"), col("offset"), 
        col("timestamp").alias("enqueued_ts")) 
    .select("e.*", "partition", "offset", "enqueued_ts")  
    .withColumn("ingestion_ts", current_timestamp())) 

# Write to Delta Lake
query = (parsed.writeStream
   .format("delta")
   .option("checkpointLocation", CHECKPOINT)
   .option("mergeSchema","true")  
   .trigger(availableNow=True)                
   .toTable(TARGET))

query.awaitTermination()

# Testing
# Observability
lp = query.lastProgress
print("Batch rows:", lp["numInputRows"] if lp else 0)

df = spark.table(TARGET)

# No null in key columns
null_keys = df.filter(
    col("event_id").isNull() |
    col("timestamp_utc").isNull() |
    col("bidding_zone").isNull()
).count()
assert null_keys == 0, f"DQ FAIL: {null_keys} rows with nulls in key columns"

# No duplicates in event_id — exactly once write
total       = df.count()
distinct_id = df.select("event_id").distinct().count()
assert total == distinct_id, f"DQ FAIL: {total - distinct_id} duplicated event_ids"

# Values in realistic ranges
bad_vals = df.filter((col("consumption_kwh") < 0) |
                     (col("pue") < 1.0) | (col("pue") > 2.0)).count()
assert bad_vals == 0, f"DQ FAIL: {bad_vals} rows out of range"

print("DQ checks passed")
