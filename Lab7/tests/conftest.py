import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    # Use the active Spark session from the notebook
    spark = SparkSession.getActiveSession()
    
    if spark is None:
        raise RuntimeError(
            "No active Spark session found. "
            "Please ensure the notebook has an active Spark session before running tests."
        )
    
    yield spark
    # Don't stop the session - it belongs to the notebook