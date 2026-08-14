# Lab 5 — Declarative Pipeline (Lakeflow)

A Lakeflow Spark Declarative Pipeline that refines the ENTSO-E price and data-center sensor data
through the medallion layers: **bronze → silver → gold**. It is the declarative counterpart of the Lab 4 Job (kept for the comparison).

## Files

| File | Purpose |
|---|---|
| `00_setup.ipynb` | Creates the bronze/silver schemas and the `entsoe_landing` Volume. All object creation lives here. |
| `ldp_pipeline.py` | The declarative pipeline: bronze, silver (with expectations + CDC dedup), gold. Table definitions only — no analytics. |
| `Comparison.md` | Write-up comparing the declarative pipeline with the classic Spark Job. |

The pipeline is deployed as a Databricks Asset Bundle resource (`entsoe_silver_ldp`) defined in the
repo-root `databricks.yml`.

## Sources

- **File / JSON source** → `prices_bronze` reads the daily ENTSO-E price JSON files from
  `/Volumes/<catalog>/<bronze_schema>/entsoe_landing/prices`.
- **Streaming source** → `sensor_bronze` reads the `sensor_data` bronze Delta table as a stream.

## Prerequisites (bronze must exist before the pipeline runs)

The pipeline reads bronze; it does not ingest raw data itself. Make sure bronze exists first:

- **dev (Free Edition):** run `00_setup`, then `generate_bronze_sample` (Lab 4). The generator creates
  the `entsoe_prices` / `sensor_data` tables **and** writes the price JSON files into
  `entsoe_landing/prices/`.
- **prod (Azure):** the real ingestion produces the same shapes — `01_fetch` writes the price JSON
  files (into `entsoe_landing/prices/`) and Auto Loader + the Event Hub consumer produce `sensor_data`.

> **Path note:** the price JSON files must land in `entsoe_landing/prices/` on **both** dev and prod,
> because that is the folder `prices_bronze` reads. Confirm `01_fetch` writes to that subfolder on prod.

## Run order

```
00_setup            ->  schemas + Volume
generate_bronze     ->  bronze tables + price JSON files   (dev)
   (prod: 01_fetch + 02_autoloader + Event Hub consumer)
ldp_pipeline        ->  bronze -> silver -> gold            (run as a PIPELINE, not a notebook)
```

## Parameters

The pipeline reads its parameters from the pipeline **configuration** (not notebook widgets), via
`spark.conf.get(...)`:

| Key | dev | prod |
|---|---|---|
| `catalog` | `workspace` | `dbr_dev` |
| `bronze_schema` | `bronze` | `gabrielajaniszews786_bronze` |
| `silver_schema` | `silver` | `gabrielajaniszews786_silver` |

These are supplied automatically by the Asset Bundle target.

## Deploy & run (via the Asset Bundle)

```bash
databricks bundle validate                        # check the bundle parses
databricks bundle deploy  -t dev                  # create the pipeline on Free Edition
databricks bundle run     entsoe_silver_ldp -t dev  # trigger a pipeline update (dev only)

databricks bundle deploy  -t prod                 # deploy to prod — do NOT run (reviewer runs it)
```

## Requirements coverage

- [x] Pipeline from a **streaming** source (`sensor_bronze`) and a **JSON/file** source (`prices_bronze`).
- [x] **Expectations** on load into silver (`expect_all_or_drop`, `expect_all_or_fail`).
- [x] **Lineage** visible in the pipeline graph / Catalog Explorer.
- [x] **Comparison** declarative vs classic (`Comparison.md`).
- [x] **Deployed** through the Asset Bundle.
- [ ] **Safe reload** — demonstrate a Full refresh (incremental Start vs full recompute) and note the result.

## Notes

- Analytics / dashboards stay **outside** the pipeline — the pipeline file defines tables only.
- Manage this pipeline **only through the bundle**; do not also create/run a second pipeline by hand on
  the same target, or the two will conflict over table ownership.
