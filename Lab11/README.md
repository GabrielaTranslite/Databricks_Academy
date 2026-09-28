# Lab 11 – Zero-Bus Streaming with Zerobus

## Goal
Implement an event-driven pipeline that writes directly to a Unity Catalog Delta
table using Databricks Zerobus Ingest, without a centralized message-bus layer,
and compare this single-sink design against the multi-sink, bus-based approach
used in Lab 3 (Event Hub / Kafka).

## Environment
- Catalog: `dbr_dev`
- Bronze schema: `gabrielajaniszews786_bronze`
- Silver schema: `gabrielajaniszews786_silver`
- Server endpoint: `https://7405615123305702.zerobus.eastus.azuredatabricks.net`
- Authentication: OAuth Client Credentials flow via a dedicated service principal,
  used only for the Zerobus write path (separate from the ENTSO-E token and the
  Event Hub connection string used in Lab 2/Lab 3).

## Files
- `zerobus_entsoe.ipynb` – main producer notebook: creates the target table,
  opens a Zerobus stream, generates synthetic sensor events, sends them in
  periodic rounds, and runs the idempotency check.
- `zerobus_producer.py` – standalone script version of the producer, mirroring
  the Lab 3 notebook/script split. **Not finalized yet** – parked pending the
  Lab 12 / final-project decision on whether Zerobus becomes the modern
  streaming component of the capstone. If adopted, this file will be completed
  to match the notebook's logic.

## Data flow
```
synthetic meter events (make_event(), reused from Lab 3)
        -> Zerobus stream.ingest_records_offset()
        -> dbr_dev.gabrielajaniszews786_bronze.zerobus_bronze (landing, at-least-once)
        -> MERGE INTO (dedup by event_id)
        -> dbr_dev.gabrielajaniszews786_silver.zerobus_silver (clean, deduplicated)
```

## Idempotent processing

Zerobus Ingest guarantees at-least-once delivery – there is no consumer-side
checkpoint like in Structured Streaming, so deduplication is the producer/
pipeline's own responsibility, not something the platform provides for free.

**Design:**
- `event_id` (UUID4, generated in `make_event()`) is the business key used for
  deduplication.
- `zerobus_bronze` is the raw landing table written directly by the Zerobus
  stream; it may contain duplicates.
- `zerobus_silver` is fed by a `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`
  statement, which is the clean, deduplicated table downstream consumers
  should use.
- Important detail: `MERGE` only deduplicates against rows already present in
  the target table – it does **not** deduplicate the source itself. If the
  source (bronze) already contains two rows with the same `event_id` and
  neither exists yet in the target, both get inserted independently. The
  source is therefore deduplicated first, using
  `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_id)` and keeping
  only `rn = 1`, before the `MERGE`.

**Test and evidence:**
- A test run intentionally duplicated one event per round (20 rounds), sending
  180 events in total (160 unique + 20 intentional duplicates).
- `zerobus_bronze` row count after the run: **180**.
- Duplicate check on bronze (`GROUP BY event_id HAVING COUNT(*) > 1`):
  **20 distinct `event_id` values, each appearing twice**, confirming the
  duplicates landed as expected.
- After running the `MERGE` into `zerobus_silver`, the same duplicate check on
  the silver table returned **0 rows** – no duplicate `event_id` remained,
  confirming the pipeline is idempotent despite Zerobus's at-least-once
  delivery guarantee.

## Comparison: Kafka-style (Event Hub, Lab 3) vs Zero-bus (Zerobus, Lab 11)

| Aspect | Kafka-style (Event Hub, Lab 3) | Zero-bus (Zerobus, Lab 11) |
|---|---|---|
| Moving parts | Event Hub namespace + hub, separate streaming consumer job with its own cluster and checkpoint location | Service principal + Unity Catalog grants + one target Delta table |
| Code | Producer *and* a separate consumer/streaming query (`fetch_script.py`) needed to land data | Producer only – no separate consumer, writes land directly in the target table |
| Delivery / exactly-once | Structured Streaming checkpoint gives close to exactly-once processing on the consumer side, largely "for free" | At-least-once only; idempotency (dedup, `MERGE`) is entirely the pipeline's own responsibility |
| Access model | Simple to configure correctly the first time (a single connection string) | Fine-grained Unity Catalog grants (catalog, schema, table) plus service-principal entitlements; a single missing grant produces an opaque `401 invalid_authorization_details` error that is hard to diagnose |
| Fan-out | Multiple independent consumer groups can read the same stream | Single-sink – writes to exactly one table; fan-out to multiple consumers would have to be built downstream from that table |
| Observability | `query.lastProgress` / `recentProgress` give built-in streaming metrics | No equivalent built-in monitoring for the write path |
| Schema evolution | Auto Loader supports `mergeSchema` | No auto-evolve – the target schema must be fixed upfront and changed manually |
| Operational cost | A cluster/compute is needed to run the streaming consumer continuously (even with `availableNow` + auto-terminate) plus Event Hub throughput units | Serverless ingestion path – no cluster required just to receive and write events |
| Maturity | Well established | Public preview; less mature documentation and community support |

**Discussion – event-driven pipelines, decoupled systems, cost and operational
trade-offs:**

Zero-bus trades ongoing operational complexity (maintaining a cluster, a
streaming query, and an Event Hub namespace) for upfront configuration
complexity and a stricter security model (Unity Catalog grants and a
dedicated service principal), while also shifting delivery guarantees
(exactly-once processing, idempotency) from the platform onto the pipeline
itself. It removes the message bus as a decoupling layer between producer and
table, which simplifies the architecture for a single-sink use case like this
project's `zerobus_bronze` table, but would reintroduce complexity if multiple
independent downstream consumers were ever needed, since Zerobus itself
provides no fan-out.

## Definition of Done
- [x] Producer writes events directly to a Unity Catalog Delta table via
      Zerobus (no message bus).
- [x] Processing is idempotent (unique key + `MERGE`-based dedup), with a
      deliberate duplicate-send test proving no duplicates survive in the
      silver table.
- [x] Comparison against the bus-based approach (Lab 3) is documented above,
      grounded in this project's two real implementations.

## Known limitations / next steps
- `zerobus_producer.py` (standalone script) is not finalized – parked until
  the final-project scope decision (after Lab 12) on whether Zerobus is
  adopted as the project's modern streaming component.
