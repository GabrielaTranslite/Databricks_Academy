import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timezone
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
    actual_utc = r.timestamp_utc.astimezone(timezone.utc).replace(tzinfo=None) # 
    assert actual_utc == datetime(2026, 7, 29, 15, 30, 8)

def test_output_schema_types(spark_session):
    """Tests if the function returns the correct schema"""
    out = clean_prices(make_input(spark_session)).schema
    assert out["price"].dataType == T.DecimalType(10, 2)
    assert isinstance(out["timestamp_utc"].dataType, T.TimestampType)


# --- clean_sensor -----------------------------------------------------------

# Defining the schema of bronze sensor data (realistic: strings from JSON/CSV)
INPUT_SCHEMA_SENSOR = T.StructType([
    T.StructField("event_id",            T.StringType(), True),
    T.StructField("timestamp_utc",       T.StringType(), True),
    T.StructField("site_id",             T.StringType(), True),
    T.StructField("site_name",           T.StringType(), True),
    T.StructField("country",             T.StringType(), True),
    T.StructField("bidding_zone",        T.StringType(), True),
    T.StructField("reading_interval_s",  T.StringType(), True),
    T.StructField("consumption_kwh",     T.StringType(), True),
    T.StructField("avg_power_kw",        T.StringType(), True),
    T.StructField("pue",                 T.StringType(), True),
    T.StructField("enqueued_ts",         T.StringType(), True),
    T.StructField("ingestion_ts",        T.StringType(), True),
])

def make_sensor_input(spark_session, **overrides):
    """Create one production-shaped sensor row while keeping each test explicit."""
    values = {
        "event_id": "evt-001",
        "timestamp_utc": "2026-07-29T15:30:08",
        "site_id": "DC-FR-01",
        "site_name": "Paris DC",
        "country": "FR",
        "bidding_zone": "FR",
        "reading_interval_s": "60",
        "consumption_kwh": "12.5000",
        "avg_power_kw": "750.00",
        "pue": "1.350",
        "enqueued_ts": "2026-07-29T15:30:08",
        "ingestion_ts": "2026-07-29T15:30:08",
    }
    values.update(overrides)
    return spark_session.createDataFrame(
        [tuple(values[field.name] for field in INPUT_SCHEMA_SENSOR)], INPUT_SCHEMA_SENSOR
    )

def get_sensor_result(spark_session, **overrides):
    """Creates a Spark data frame with clean_sensor"""
    return clean_sensor(make_sensor_input(spark_session, **overrides)).first()

def test_sensor_valid_row_is_cast_to_target_types(spark_session):
    """Tests if the function behaves correctly with valid data"""
    r = get_sensor_result(spark_session)
    assert r.pue == Decimal("1.350")
    assert r.consumption_kwh == Decimal("12.5000")

def test_sensor_null_measurements_propagate(spark_session):
    """Tests if null is correctly propagated"""
    r = get_sensor_result(spark_session, pue=None, consumption_kwh=None)
    assert r.pue is None
    assert r.consumption_kwh is None

def test_sensor_empty_input_keeps_schema_and_has_no_rows(spark_session):
    """Tests if the function behaves correctly with zero rows"""
    empty = spark_session.createDataFrame([], INPUT_SCHEMA_SENSOR)

    result = clean_sensor(empty)

    assert result.count() == 0
    assert {"pue", "consumption_kwh"}.issubset(result.columns)

def test_sensor_output_schema_types(spark_session):
    """Tests if the function returns the correct schema"""
    out = clean_sensor(make_sensor_input(spark_session)).schema
    assert out["pue"].dataType == T.DecimalType(4, 3)
    assert out["consumption_kwh"].dataType == T.DecimalType(10, 4)

def test_sensor_invalid_pue_string_becomes_null(spark_session):
    """Checks if a malformed value does not cause unexpected values or raise"""
    r = get_sensor_result(spark_session, pue="not-a-number")
    assert r.pue is None

def test_sensor_pue_overflow_becomes_null(spark_session):
    """decimal(4,3) allows at most 1 integer digit; a value that doesn't fit
    should become null rather than raise under ANSI mode."""
    r = get_sensor_result(spark_session, pue="12.345")
    assert r.pue is None
