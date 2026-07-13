# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1 — fetch ENTSO-E day-ahead prices (A44) to landing
# MAGIC Ingests day-ahead prices for Poland for each day and saves **one JSON file per day**
# MAGIC to container landing. Idempotency: consistent file name = no duplicates.
# MAGIC Token read from shared secret scope `default2`.

# COMMAND ----------

# Parameters (widgets)
dbutils.widgets.text("n_days", "30", "Number of days")
dbutils.widgets.text("country_code", "10YPL-AREA-----S", "EIC domain code")
dbutils.widgets.text("country_label", "PL", "Country label")
dbutils.widgets.text("secret_scope", "default2", "Secret scope")
dbutils.widgets.text("secret_key", "gabriela-entsoe-token", "Token with key")

N_DAYS       = int(dbutils.widgets.get("n_days"))
COUNTRY_CODE = dbutils.widgets.get("country_code")
COUNTRY      = dbutils.widgets.get("country_label")
SCOPE        = dbutils.widgets.get("secret_scope")
KEY          = dbutils.widgets.get("secret_key")

LANDING = "/Volumes/dbr_dev/gabrielajaniszews786_bronze/entsoe_landing"
API_URL = "https://web-api.tp.entsoe.eu/api"

# COMMAND ----------

# Volume for landing
spark.sql("CREATE VOLUME IF NOT EXISTS dbr_dev.gabrielajaniszews786_bronze.entsoe_landing")

# COMMAND ----------

import requests, time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import pandas as pd

token = dbutils.secrets.get(scope=SCOPE, key=KEY)

def parse_prices(xml_text):
    """XML A44 -> tuple list (timestamp_utc_iso, price, currency, unit).
    Returns [] if Acknowledgement (no data - weekends, etc.)"""
    root = ET.fromstring(xml_text)
    tag = root.tag.split("}")[-1]
    if tag != "Publication_MarketDocument":     # for instance Acknowledgement_MarketDocument = no data
        return []
    ns = {"ns": root.tag.split("}")[0].strip("{")}
    rows = []
    for ts in root.findall("ns:TimeSeries", ns):
        currency = ts.findtext("ns:currency_Unit.name", namespaces=ns)
        unit     = ts.findtext("ns:price_Measure_Unit.name", namespaces=ns)
        for period in ts.findall("ns:Period", ns):
            start = period.findtext("ns:timeInterval/ns:start", namespaces=ns)
            res   = period.findtext("ns:resolution", namespaces=ns)
            start_dt = datetime.strptime(start, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            step = timedelta(minutes=15 if res == "PT15M" else 60)
            for point in period.findall("ns:Point", ns):
                pos   = int(point.findtext("ns:position", namespaces=ns))
                price = float(point.findtext("ns:price.amount", namespaces=ns))
                ts_iso = (start_dt + step * (pos - 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                rows.append((ts_iso, price, currency, unit))
    return rows


def fetch_day(day):
    """Gets one day of prices (A44); supports 429 status code with retry. Returns (status, xml_text)."""
    params = {
        "securityToken": token,
        "documentType": "A44",
        "in_Domain":  COUNTRY_CODE,
        "out_Domain": COUNTRY_CODE,
        "periodStart": day.strftime("%Y%m%d0000"),
        "periodEnd":  (day + timedelta(days=1)).strftime("%Y%m%d0000"),
    }
    for attempt in range(5):
        r = requests.get(API_URL, params=params, timeout=60)
        if r.status_code == 429:            # limit 400/min exceeded — wait and retry
            time.sleep(5 * (attempt + 1))
            continue
        return r.status_code, r.text
    return 429, ""

# COMMAND ----------

today = datetime.now(timezone.utc).date()
written, empty, failed = 0, 0, 0

for i in range(1, N_DAYS + 1):
    day = datetime.combine(today - timedelta(days=i), datetime.min.time(), tzinfo=timezone.utc)
    day_str = day.strftime("%Y%m%d")
    path = f"{LANDING}/prices_{COUNTRY}_{day_str}.json"
    try:
        status, xml_text = fetch_day(day)
        if status != 200:
            print(f"{day_str}: HTTP {status} — omitted")
            failed += 1
            continue
        rows = parse_prices(xml_text)
        if not rows:
            print(f"{day_str}: no data (Acknowledgement)")
            empty += 1
            continue
        out = pd.DataFrame(rows, columns=["timestamp_utc", "price", "currency", "unit"])
        out["country"] = COUNTRY
        out.to_json(path, orient="records", lines=True, force_ascii=False)
        written += 1
    except Exception as e:
        print(f"{day_str}: {e} error")
        failed += 1
    time.sleep(0.3)                            

print(f"\nProcess completed. Saved data: {written}, days with no data: {empty}, errors: {failed}")

# COMMAND ----------

# Podgląd — co wylądowało w landing
display(dbutils.fs.ls(LANDING))
