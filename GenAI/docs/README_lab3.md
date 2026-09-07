# Lab 3 — Streaming, Incremental Ingestion & Schema Evolution

End-to-end streaming lab on the **ENTSO-E / data-center energy** project. It covers two
independent streaming sources landing in the governed `bronze` schema: a **file-based**
source ingested with Auto Loader, and an **unbounded** source consumed from Azure Event Hub.

## Scope note

The official Lab 3 tasks are: many-file Auto Loader ingestion, schema inference & evolution
(rescued-data column, added/renamed source column), streaming statistics, trigger types,
safe checkpoint-based reload, an Event Hub producer/consumer writing to bronze with metadata,
and a scheduled streaming job with cost awareness.

For the Event Hub source I used **synthetic data-center meter events** — an allowed producer
source. Each event carries `bidding_zone`, which is designed as the join key to the ENTSO-E
prices should a later lab require it.

## Environment

- Catalog: `dbr_dev`; schema: `gabrielajaniszews786_bronze`
- Compute: GA job/all-purpose clusters
- Shared storage account `dlspl21databricks`, container `gabrielajaniszews786`
- Shared secret scope `default2` (ENTSO-E token and Event Hub connection string read from here — never hard-coded)
- Shared Azure Event Hub namespace `evhpl24databricks`, hub `gabrielajaniszews786_eventhub`

## Files

| File | Purpose |
|------|---------|
| `01_fetch_entsoe_prices_lab3.ipynb` | Fetcher — pulls ENTSO-E day-ahead prices (A44) per bidding zone × day from the API and writes **one JSON file per zone-day** to the landing Volume. Handles the 400/min rate limit (sleep + 429 retry) and empty Acknowledgement responses. Parameterised with widgets (zones, number of days, secret scope/key). |
| `02_autoloader_bronze_lab3.ipynb` | Auto Loader — incrementally ingests the landing JSON files into the `entsoe_prices` Delta table with schema location, inferred column types, metadata columns, streaming statistics, and a safe checkpoint-based reload. |
| `03_fetch_sensor_streaming_lab3.ipynb` | Event Hub consumer — reads the meter-event stream via the Kafka endpoint, parses the JSON payload against an explicit schema, attaches Event Hub metadata, and writes to the `sensor_data` Delta table. Interactive/exploratory version. |
| `event_hub_task.ipynb` | Producer notebook (job task) — sends synthetic meter events to Event Hub across all sites. |
| `sensor_stream.py` | Shared module: `MeterEvent` schema, `make_event()` helper, and the `SITES` reference list. |
| `event_hub_script.py` | Standalone producer script (same logic as the producer notebook). |
| `fetch_script.py` | Standalone consumer script used by the scheduled job — consumer logic plus `awaitTermination()` and an end-of-run data-quality gate that fails the job on bad data. |
| `Lab3_screenshots.ipynb` | Evidence & observations: schema-evolution retry, rescued-data column, trigger experiments, streaming-stats dashboards, and the scheduled-job design note (with screenshots). |

## Data flow

```
ENTSO-E API ──(01 fetcher)──▶ landing Volume (JSON) ──(02 Auto Loader)──▶ bronze.entsoe_prices (Delta)

synthetic meters ──(producer)──▶ Azure Event Hub ──(consumer / fetch job)──▶ bronze.sensor_data (Delta)
```

Both bronze tables are raw (1:1 with the source) plus metadata columns. All ingestion is
incremental and idempotent — nothing is loaded twice.

## Part A — Auto Loader & schema evolution

- **Many files.** 1054 JSON files landed (zones × days), so Auto Loader processes a realistic
  backlog across multiple batches.
- **Incremental ingestion.** `readStream.format("cloudFiles")` with `schemaLocation`; the
  checkpoint tracks which files were already read, so re-runs add only new files.
- **Schema inference & evolution.** `inferColumnTypes=true` keeps prices/MW numeric. When a new
  source column appears (ES-zone files), the default `addNewColumns` mode stops the stream once,
  records the new schema, and succeeds on automatic retry — the column is added without data loss.
- **Rescued-data column.** A deliberately broken file (a price sent as a string) routes the
  mismatched value into `_rescued_data` instead of dropping it silently.
- **Streaming statistics.** Files-per-batch and input/processing rates read from
  `query.recentProgress`, plus the live streaming dashboards captured in the screenshots notebook.
- **Trigger types.** Experiments with `availableNow` vs `once` vs a continuous `processingTime`
  trigger, including the debugging note on two queries sharing one checkpoint.
- **Safe reload.** Keeping the checkpoint gives incremental, duplicate-free re-runs; a conscious
  full reload clears the checkpoint **and** truncates the target table together.

## Part B — Event streaming

- **Producer.** Sends synthetic meter events (all sites, batched per round) to the shared Event
  Hub; connection string read from the secret scope.
- **Consumer.** Spark Structured Streaming reads Event Hub through its Kafka-compatible endpoint
  (`SASL_SSL` / `PLAIN`, port 9093). The binary `value` is cast to string and parsed with
  `from_json` against an explicit schema.
- **Bronze + metadata.** Writes to `sensor_data` (Delta) with Event Hub metadata (`partition`,
  `offset`, `enqueued_ts`) plus `ingestion_ts`. Checkpoint guarantees no record is read twice.
- **No UDFs.** All transformations use built-in Spark functions — no UDF was justified.
- **Scheduled job — cost awareness.** Job `lab3_stream_fetching_job_gabriela` runs the consumer
  on a schedule (every 15 min - paused now) with `trigger(availableNow=True)`: it drains the backlog and stops
  so the job cluster auto-terminates between runs, instead of a 24/7 stream. The producer is a
  separate, manually-run job that simulates the external live source, so the two can run on
  independent cadences. A data-quality gate at the end of the run (null keys, duplicate
  `event_id`, out-of-range values) raises on bad data and fails the task.

## Optional additions

Exactly-once vs at-least-once semantics and checkpointing / fault tolerance are discussed in the
final markdown note of `Lab3_screenshots.ipynb`.

## Definition of Done

- [x] Auto Loader ingests incrementally (new files added, not reprocessed from scratch)
- [x] A newly added source column is handled via schema evolution without failing; rescued-data column demonstrated
- [x] Checkpoint-based safe reload works (and a conscious full reload with a fresh checkpoint)
- [x] Producer sends synthetic data to the shared Event Hub
- [x] Consumer (Structured Streaming) writes the stream to bronze with metadata
- [x] Streaming job created and scheduled, with cost awareness
