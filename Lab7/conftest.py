import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

# DATABRICKS_CONFIG_PROFILE: which ~/.databrickscfg profile to use for branch 2
# below. Needed because the CLI's default_profile may point at a different
# workspace (e.g. dbr_dev is Azure, not Free Edition) -- set per-developer in
# Lab7/.env (gitignored), not hardcoded here.
DATABRICKS_PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE")
SPARK_SESSION_TIMEZONE = os.environ.get("SPARK_SESSION_TIMEZONE", "UTC")


# Fixture for three different Spark session scenarios:
@pytest.fixture(scope="session")
def spark_session():
    # 1) Databricks notebook
    try:
        from pyspark.sql import SparkSession
        active = SparkSession.getActiveSession()
        if active is not None:
            active.conf.set("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)
            yield active
            return
    except Exception:
        pass

    # 2) Databricks Connect (serverless)
    if DATABRICKS_PROFILE:
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.profile(DATABRICKS_PROFILE).serverless().getOrCreate()
            spark.conf.set("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)
            yield spark
            return
        except Exception:
            pass

    # 3) Local Spark session
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .master("local[1]").appName("lab7-tests")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)
             .getOrCreate())
    yield spark
    spark.stop()