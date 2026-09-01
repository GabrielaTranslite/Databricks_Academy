import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from decimal import Decimal
from silver_transformations import clean_prices, clean_sensor

# Defining the schema of bronze prices data (realistic: strings from JSON/CSV)
INPUT_SCHEMA = T.StructType([
    T.StructField("bidding_zone", T.StringType(), True),
    T.StructField("timestamp_utc", T.StringType(), True),
    T.StructField("price", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("unit", T.StringType(), True),
    T.StructField("source_file", T.StringType(), True),
    T.StructField("ingestion_ts", T.StringType(), True),
    T.StructField("silver_processed_ts", T.StringType(), True)
])

def make_input(spark_session, **overrides):
    """Create one production-shaped row while keeping each test explicit."""
    values = {
        "bidding_zone": "FR",
        "timestamp_utc": "2026-07-29T15:30:08",
        "price": "100.00",
        "currency": "EUR",
        "unit": "MWH",
        "source_file": "prices_PL_20260805.json",
        "ingestion_ts": "2026-07-29T15:30:08",
        "silver_processed_ts": "2026-07-29T15:30:08"
    }
    values.update(overrides)
    return spark_session.createDataFrame(
        [tuple(values[field.name] for field in INPUT_SCHEMA)], INPUT_SCHEMA
    )

def get_result(spark_session, **overrides):
    """Creates a Spark data frame with clean_prices"""
    return clean_prices(make_input(spark_session, **overrides)).first()

def test_null_measurements_propagate_without_being_reinterpreted(spark_session):
    """Tests if null is correctly propagated"""
    result = get_result(
        spark_session,
        timestamp_utc=None,
        currency=None,
        unit=None,
        source_file=None,
        ingestion_ts=None,
        silver_processed_ts=None
    )

    assert result.timestamp_utc is None
    assert result.currency is None
    assert result.unit is None
    assert result.source_file is None
    assert result.ingestion_ts is None
    assert result.silver_processed_ts is None

def test_invalid_timestamp_strings_become_null(spark_session):
    """Checks if values such as not a timestamp do not cause unexpected values"""
    result = get_result(
        spark_session,
        timestamp_utc="not-a-timestamp",
        ingestion_ts="2026-99-99T99:99:99Z",
        silver_processed_ts="",
    )

    assert result.timestamp_utc is None
    assert result.ingestion_ts is None
    assert result.silver_processed_ts is None

def test_empty_input_keeps_schema_and_has_no_rows(spark_session):
    """Tests if the function behaves correctly with zero rows"""
    empty = spark_session.createDataFrame([], INPUT_SCHEMA)

    result = clean_prices(empty)

    assert result.count() == 0
    assert {
        "bidding_zone",
        "timestamp_utc",
        "price",
        "currency",
        "unit",
        "source_file",
        "ingestion_ts",
        "silver_processed_ts"
    }.issubset(result.columns)

def test_valid_row_is_cast_to_target_types(spark_session):
    """Tests if the function behaves correctly with valid data"""
    
       r = get_result(spark_session)  # domyślny dobry wiersz
       assert r.price == Decimal("100.00")
       assert r.timestamp_utc == datetime(2026, 7, 29, 15, 30, 8)

def test_output_schema_types(spark_session):
    """Tests if the function returns the correct schema"""
       out = clean_prices(make_input(spark_session)).schema
       assert out["price"].dataType == T.DecimalType(10, 2)
       assert isinstance(out["timestamp_utc"].dataType, T.TimestampType)
    