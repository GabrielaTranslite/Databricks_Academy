from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_prices(df):
    """
    Cleans the bronze DataFrame with prices and returns a cleaned DataFrame with prices.
    Handles conversion from string types (bronze) to proper types (silver).
    Invalid values are converted to null.
    """
    return (df
        # Convert price from string to decimal, invalid values become null
        .withColumn("price", F.col("price").cast("decimal(10,2)"))
        # Convert timestamps from string to timestamp, invalid values become null
        .withColumn("timestamp_utc", F.to_timestamp(F.col("timestamp_utc")))
        .withColumn("ingestion_ts", F.to_timestamp(F.col("ingestion_ts")))
        .withColumn("silver_processed_ts", F.to_timestamp(F.col("silver_processed_ts")))
    )


def clean_sensor(df):
    """Cleans the bronze DataFrame with sensor data and returns a cleaned DataFrame with sensor data."""

    return (df
        .withColumn("pue", F.col("pue").cast("decimal(4,3)"))
        .withColumn("consumption_kwh", F.col("consumption_kwh").cast("decimal(10,4)"))
    )
