import uuid
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

# --- Meter event schema: one reading from one data-center meter -----------
@dataclass
class MeterEvent:
    event_id: str          # unique per event -> deduplication / exactly-once
    schema_version: int    # bump when you add/rename a field (schema-evolution demo)
    site_id: str           # e.g. "DC-DE-01"
    site_name: str         # e.g. "Frankfurt DC"
    country: str           # e.g. "DE"
    bidding_zone: str      # JOIN KEY to ENTSO-E prices: PL, DE_LU, FR, ES, CZ, SK
    timestamp_utc: str     # event time, ISO-8601 UTC -> bucket to price hour in Silver
    reading_interval_s: int  # length of the interval this reading covers (e.g. 60)
    consumption_kwh: float   # energy used in that interval -> cost = kwh * price/1000
    avg_power_kw: float      # average IT+cooling load over the interval
    pue: float               # power usage effectiveness (~1.1–1.6), data-center flavour

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json_bytes(self) -> bytes:
        # Event Hub wants bytes; JSON lines are easy to consume in Structured Streaming
        return json.dumps(asdict(self)).encode("utf-8")


# --- Helper to stamp a fresh event (values are placeholders for your generator) ---
def make_event(site: dict, timestamp_utc: str, consumption_kwh: float,
               avg_power_kw: float, pue: float) -> MeterEvent:
    return MeterEvent(
        event_id=str(uuid.uuid4()),
        schema_version=1,
        site_id=site["site_id"],
        site_name=site["site_name"],
        country=site["country"],
        bidding_zone=site["bidding_zone"],
        timestamp_utc=timestamp_utc,
        reading_interval_s=60,
        consumption_kwh=round(consumption_kwh, 4),
        avg_power_kw=round(avg_power_kw, 2),
        pue=round(pue, 3),
    )


# --- Minimal site registry (mapping data centers -> bidding zones) ---------
# You'll expand this; it only exists so bidding_zone lines up with your prices.
SITES = [
    {"site_id": "DC-PL-01",    "site_name": "Warsaw DC",    "country": "PL", "bidding_zone": "PL"},
    {"site_id": "DC-DE-01",    "site_name": "Frankfurt DC",  "country": "DE", "bidding_zone": "DE_LU"},
    {"site_id": "DC-FR-01",    "site_name": "Paris DC",      "country": "FR", "bidding_zone": "FR"},
    {"site_id": "DC-ES-01",    "site_name": "Barcelona DC",     "country": "ES", "bidding_zone": "ES"},
    {"site_id": "DC-CZ-01",    "site_name": "Brno DC",    "country": "CZ", "bidding_zone": "CZ"},
    {"site_id": "DC-SK-01",    "site_name": "Bratislava DC", "country": "SK", "bidding_zone": "SK"},
    {"site_id": "DC-LT-01",    "site_name": "Vilnus DC",     "country": "LT", "bidding_zone": "LT"},
    {"site_id": "DC-PT-01",    "site_name": "Lisbon DC",     "country": "PT", "bidding_zone": "PT"},
]