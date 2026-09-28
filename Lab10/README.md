# Lab 10 - External Operational Databases: Lakehouse Federation and CDC

Working with an external operational database in two complementary ways: querying it in
place with **Lakehouse Federation** (Part A), and capturing its changes incrementally with
**Change Data Capture** over Delta Change Data Feed (Part B). The domain object is a
data-center dimension that lives in an external PostgreSQL database (Neon) and ties into the
ENTSO-E gold layer.

## Goal

Query an external SQL database from Databricks without copying it, then keep a
history-tracked copy in sync incrementally.

**Done when:** an external table is queried through a foreign catalog (Part A), and source
inserts, updates and deletes are reflected incrementally in a target table (Part B).

## Files in this lab

- `databricks_federation.ipynb` - the main notebook, Part A and Part B end to end.


## External database

The external source is a free **Neon PostgreSQL** database. I have created a `public.dc_dim` table of 15 columns describing data centers (id, operator,
location, IT power, power density, tier, PUE, cooling), with `dc_id` values aligned to the
`site_id` values in the ENTSO-E fact table so the join is a clean one-to-one.

## Part A - Lakehouse Federation

Federation exposes the external database inside Unity Catalog and runs queries against it
live, with no ingestion. It is wired with three objects:

- **Connection** (`neon_pg`) holds how to reach Neon: host, port, user, and the password,
  which is read from a Databricks secret scope. The PostgreSQL connector uses
  `trustServerCertificate 'true'` for SSL, not `sslmode`.
- **Foreign catalog** (`neon`) mirrors the Neon database into Unity Catalog, so its tables
  appear as `neon.public.dc_dim` and are queryable like any UC table.
- **Query pushdown** sends filters and joins to Postgres where possible.

The notebook queries the external table directly, then joins it to the governed Delta fact
table `consumption_hourly` on `site_id = dc_id`, keeping the latest reading per site with a
`QUALIFY ROW_NUMBER()` window. This is the federation-versus-ingestion contrast in practice:
the same data center dimension is available both live through federation and, separately,
ingested into the gold layer as `dim_site` (see below).

## Part B - Change Data Capture with Delta Change Data Feed

Because the external source is PostgreSQL, the CDC mechanism used here is **Delta Change Data
Feed**, which needs no external CDC infrastructure. The flow:

1. **Land and enable CDF.** `cdc_table` is dropped and recreated from `neon.public.dc_dim`
   with `delta.enableChangeDataFeed = true`, giving a clean version 0.
2. **Simulate changes** directly on the Delta table (this is what a change source would
   otherwise deliver): two updates, one insert, one delete.
3. **Capture the events** with `table_changes(cdc_table, 1)`, which returns each change tagged
   `insert`, `update_preimage`, `update_postimage` or `delete`, with `_commit_version` and
   `_commit_timestamp`.
4. **Apply incrementally into an SCD Type 2 target** (`dc_dim_scd2`) with history columns
   `valid_from`, `valid_to`, `is_current`. The target is first loaded from the unchanged Neon
   snapshot (the starting state), then updated in two steps:
   - close current rows for keys that were updated or deleted (`MERGE ... UPDATE`),
   - insert a new current row for keys that were inserted or updated (`INSERT ... SELECT`).

After the run, a changed key shows a closed old row plus an open new row, a deleted key shows
only a closed row, and a new key shows one open row. This is the history model from Week 4,
now fed by a change feed instead of a full snapshot.

## Relation to the gold layer

The same dimension is ingested into the ENTSO-E gold layer as `dim_site`
(`Lab6/06_gold_layer.ipynb`), materialized from the federated `neon.public.dc_dim` with
`INSERT OVERWRITE`. In the production pipeline `dim_site` is a Type 1 snapshot, refreshed
with each job run. The SCD2 and CDC work in this lab is a demonstration of the change-tracking
mechanics on a separate table, and does not change how `dim_site` is loaded.

## How to run

1. In Databricks, open `databricks_federation.ipynb` on a serverless SQL warehouse (or a
   Standard / Dedicated access-mode cluster). Set the `catalog` and `gold_schema` widgets.
2. Run Part A cells: connection, foreign catalog, sanity query, and the join.
3. Run Part B cells in order: recreate `cdc_table` with CDF, load the SCD2 target from Neon,
   simulate the changes, inspect `table_changes`, then the two SCD2 steps, then verify.

## Done when

- [x] An external table is queried through a foreign catalog (`neon.public.dc_dim`).
- [x] The external table is joined to a governed Delta table (`consumption_hourly`).
- [x] A CDC source is used (Delta Change Data Feed on `cdc_table`).
- [x] Updates, an insert and a delete are simulated and captured via `table_changes`.
- [x] The changes are reflected incrementally in a target table via `MERGE INTO` plus insert,
      with full SCD2 history.

## Discussion points

- **Federation vs ingestion.** Federation is live and needs no pipeline or storage, but every
  query hits the source and is bounded by its speed. Ingestion gives fast Delta reads, history
  and decoupling from the source, at the cost of a pipeline and storage. Federate to peek at a
  live source or join a small reference table; ingest for heavy analytics and history.
- **Latency and security.** A federated query runs on Postgres in real time and competes with
  its application traffic, which is often the real reason to ingest. Credentials live in a
  secret scope and access is governed centrally by Unity Catalog.
- **Batch vs CDC.** A full reload moves everything every run and misses intermediate states.
  CDC moves only what changed and is near real time, at the cost of a change source and a
  keyed, idempotent MERGE.
- **SCD and late-arriving data.** SCD1 overwrites and keeps only the current value; SCD2 keeps
  history with valid-from/valid-to. Ordering changes by a reliable key such as `_commit_version`
  keeps the newest version per key even when events arrive out of order.

## Assumptions and notes

- Developed on Databricks Free Edition (Unity Catalog plus serverless), with Neon as the
  external PostgreSQL database. The connection and foreign catalog are Unity Catalog governance
  objects created with plain SQL; as infrastructure-as-code they belong in the Terraform layer,
  not in the asset bundle.
- The CDC demonstration writes to a separate `cdc_table` and `dc_dim_scd2`, so the simulated
  deletes never touch production gold tables.
- To get clean change-feed version numbers, `cdc_table` is dropped and recreated so the initial
  load is version 0 and the simulated changes are versions 1 and up.
- Fake data: `dc_dim` values are invented but internally consistent (power density is derived
  from IT power and area) and physically plausible.
