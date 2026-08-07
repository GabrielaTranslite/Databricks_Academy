# ENTSO-E / Data-Center Energy — Bronze & Silver pipeline (Lab 4)

End-to-end lakehouse pipeline on the ENTSO-E day-ahead prices and synthetic
data-center meter events. Bronze ingestion (Lab 3) feeds a clean, analytics-ready
Silver layer (Lab 4) with deduplication, MERGE upserts, SCD, schema
enforcement/evolution, data-quality rules and table maintenance.

The pipeline is delivered as a **Databricks Asset Bundle** (`databricks.yml`) with two
targets: `dev` (developed on Databricks Free Edition) and `prod` (deployed on the
reviewer's Databricks). Same Silver code, different bronze sources per environment.

## Environments

| | dev (Free Edition) | prod (reviewer's Databricks) |
|---|---|---|
| Catalog | `workspace` | `dbr_dev` |
| Bronze schema | `gabrielajaniszews786_bronze` | `bronze` |
| Silver schema | `gabrielajaniszews786_silver` | `silver` |
| Prices bronze | synthetic generator | ENTSO-E API → Auto Loader |
| Sensor bronze | synthetic generator | Azure Event Hub stream |
| Secrets needed | none | ENTSO-E token + Event Hub conn. string |

Free Edition is serverless-only and blocks outbound internet, so the real ENTSO-E API
and Event Hub are unreachable there. The generator produces bronze tables with the
**same schema** as the real sources (plus deliberate duplicates and null prices) so the
Silver notebooks can be developed and tested unchanged, then run for real in prod.

## Repository layout

```
databricks.yml                     Asset Bundle: dev + prod targets, the Silver job
RUN_ON_FREE_EDITION.md             Step-by-step to deploy & run the dev pipeline

Lab3/  (bronze)
  01_fetch_entsoe_prices_lab3      Fetch ENTSO-E day-ahead prices -> landing Volume (JSON)
  02_autoloader_bronze_lab3        Auto Loader landing -> bronze.entsoe_prices (Delta)
  03_fetch_sensor_streaming_lab3   Event Hub (Kafka endpoint) -> bronze.sensor_data (Delta)

Lab4/  (silver)
  generate_bronze_sample.py        Synthetic bronze seed for Free Edition (dev only)
  03_silver_entsoe                 Silver prices: dedup, MERGE, DQ, column mapping, table maintenance
  04_silver_sensor_data            Silver sensor: dedup, MERGE, DQ, controlled schema evolution
  05_silver_dim_datacenter         Dimension dim_datacenter with SCD Type 2 via MERGE
  Schema_evolution                 Standalone demo: schema enforcement vs mergeSchema
```

## Parameters

All notebooks read their configuration from widgets, so nothing environment-specific is
hardcoded. The bundle injects the values per target (see `databricks.yml`).

| Parameter | Meaning | dev | prod |
|---|---|---|---|
| `catalog` | Unity Catalog | `workspace` | `dbr_dev` |
| `bronze_schema` | Bronze schema | `gabrielajaniszews786_bronze` | `bronze` |
| `silver_schema` | Silver schema | `gabrielajaniszews786_silver` | `silver` |
| `run_checks` | Run verification/experiment cells (`display`, demos) | `true` when run by hand | `false` in the job |

`run_checks` gates only exploratory cells (previews, constraint tests, column-mapping
demo). Structural steps (CREATE, MERGE, constraints, maintenance) always run.

## Secrets (prod only)

Never stored in Git. The reviewer creates a secret scope and the two keys below, then the
`prod` target reads them by name:

| Scope | Key | Used by |
|---|---|---|
| `default2` | `gabriela-entsoe-token` | `01_fetch_entsoe_prices` (ENTSO-E API token) |
| `default2` | `eventhub-con-str-gabriela` | `03_fetch_sensor_streaming` (Event Hub connection string) |

The dev pipeline needs no secrets — the generator has no external dependencies.

## Run order

**dev** — one bronze task, then three Silver tasks in parallel:

```
generate_bronze ──┬─▶ silver_prices
                  ├─▶ silver_sensor
                  └─▶ silver_dim
```

**prod** — two bronze branches, then the matching Silver tasks:

```
fetch_prices ─▶ autoloader_prices ─▶ silver_prices
ingest_sensor (Event Hub) ─┬─▶ silver_sensor
                           └─▶ silver_dim
```

## Deploy & run

See **RUN_ON_FREE_EDITION.md** for the dev walkthrough. In short:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run silver_pipeline -t dev
```

The reviewer deploys prod with `-t prod` after filling in the workspace host and secrets.

## Lab 4 requirement coverage

| Requirement | Where |
|---|---|
| Silver: dedup, defined schema, metadata columns | 03, 04 (`row_number` dedup, explicit CREATE, `ingestion_ts` / `silver_processed_ts`) |
| MERGE pipeline | 03, 04 (upsert on business key) |
| SCD Type 2 | 05 (`dim_datacenter`, close-and-open versions via MERGE) |
| SCD Type 1 | 03, 04 (fact MERGE `UPDATE SET *` = overwrite-in-place upsert) |
| Schema enforcement (rejected write) | `Schema_evolution` (append with extra column is rejected) |
| Schema evolution (`mergeSchema`, add column) | `Schema_evolution` (`mergeSchema`) + 04 (controlled `ADD COLUMN`) |
| Delta column mapping (rename/drop) | 03 (on a `DEEP CLONE` demo table, real table untouched) |
| Data quality rules | 03, 04 (`CHECK` constraints; NOT NULL keys) |
| Reliability / idempotent re-runs | keep-table + idempotent DDL; MERGE dedup on key |
| Table maintenance | 03 (`OPTIMIZE`, `VACUUM`, Liquid Clustering vs ZORDER/partitioning) |
| Scheduling | `databricks.yml` (daily job, cron) |

## Known simplifications (deliberate, for the lab)

- **Generator vs real source.** On Free Edition bronze is synthetic. Schemas match the
  real sources, so Silver code is identical across environments.
- **`dim_datacenter` seed** runs once (guarded on empty table); the SCD2 change is a
  simulated source event. In real prod the SCD2 MERGE would consume actual incoming
  changes.
- **Table maintenance runs inline** in `03` for demonstration. In production, OPTIMIZE /
  VACUUM are usually a separate, less-frequent job (or handled by Predictive Optimization
  on Unity Catalog managed tables).
