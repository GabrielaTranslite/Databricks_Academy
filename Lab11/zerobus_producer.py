%pip install databricks-zerobus-ingest-sdk

import logging
import datetime, random, time
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties, StreamConfigurationOptions, RecordType
from sensor_stream import make_event, SITES


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
# Configuration
WORKSPACE_URL = "https://adb-7405615123305702.2.azuredatabricks.net"
SERVER_ENDPOINT = "https://7405615123305702.zerobus.eastus.azuredatabricks.net"
CLIENT_ID = dbutils.secrets.get(scope = "entsoe", key = "zerobus-client-id-gabriela")
CLIENT_SECRET = dbutils.secrets.get(scope = "entsoe", key = "zerobus-client-secret-gabriela")
TABLE_NAME = f"{CATALOG}.{BRONZE_SCHEMA}.zerobus_bronze"
ROUNDS = 20
SLEEP_S = 3

# Initialize SDK
sdk = ZerobusSdk(SERVER_ENDPOINT, WORKSPACE_URL)

# Configure table properties
table_properties = TableProperties(
    table_name=TABLE_NAME,
)

# Create stream
stream = sdk.create_stream(
    CLIENT_ID,
    CLIENT_SECRET,
    table_properties,
    options=StreamConfigurationOptions(record_type=RecordType.JSON))


sent = 0

try:
    for _ in range(ROUNDS):
        records = []
        ts   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        for site in SITES:
            avg_power = random.uniform(700.0, 800.0)
            event = make_event(
                site, 
                timestamp_utc=ts,
                consumption_kwh = round(avg_power * random.uniform(0.85, 1.25), 2),
                avg_power_kw=avg_power,
                pue = random.uniform(1.2, 1.4),
            )
            records.append(event.to_dict())
            sent +=1
            
        stream.ingest_records_offset(records)
        time.sleep(SLEEP_S)
        print(f"Sent {sent} events from {len(SITES)} sites")
        stream.flush()
finally:
    stream.close()
            