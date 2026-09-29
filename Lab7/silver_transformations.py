from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_prices(df):
    """
    Cleans the bronze DataFrame with prices and returns a cleaned DataFrame with prices.
    Handles conversion from string types (bronze) to proper types (silver).
    Invalid values are converted to null.
    """
    df = (df
        # Convert price from string to decimal, invalid values become null
        .withColumn("price", F.expr("try_cast(price as decimal(10,2))"))
        # Convert timestamps from string to timestamp, invalid values become null
        .withColumn("timestamp_utc",       F.try_to_timestamp("timestamp_utc"))
        .withColumn("ingestion_ts",        F.try_to_timestamp("ingestion_ts"))
        .withColumn("silver_processed_ts", F.current_timestamp())
    )
    
    # De-duplicate: ENTSO-E responses / repeated fetches yield several identical
    # copies of the same (bidding_zone, timestamp_utc). Keep one (latest ingestion)
    # so the is_unique check guards the data instead of quarantining every copy.
    w = Window.partitionBy("bidding_zone", "timestamp_utc") \
              .orderBy(F.col("ingestion_ts").desc_nulls_last())
    return (df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn"))


def clean_sensor(df):
    """Cleans the bronze DataFrame with sensor data and returns a cleaned DataFrame with sensor data."""

    return (df
        .withColumn("pue", F.expr("try_cast(pue as decimal(4,3))"))
        .withColumn("consumption_kwh", F.expr("try_cast(consumption_kwh as decimal(10,4))"))
    )
