# Part B – Requirements prompt given to the coding agent

This is the exact task specification handed to the AI coding agent (Claude Code + Databricks AI
Dev Kit). It was given as requirements and acceptance criteria, not as ready code, so the agent
had to derive the implementation and it could then be reviewed.

---

## Task: Add a data-center map gold table (Neon federation) – Lab 12 Part B

### Goal
Add a new gold table `dc_map` that powers a dashboard map of data centers. Each data center is one
point; it renders GREEN when it has a recent energy-consumption reading and RED when it does not.
Data-center metadata comes from Neon PostgreSQL, already exposed to Unity Catalog as the `neon`
foreign catalog (Lakehouse Federation).

### Context to read first
- `Lab10/databricks_federation.ipynb`, `Lab10/databricks_federation_datacenters.sql` – how the `neon`
  foreign catalog and `neon.public.dc_dim` were set up. The `neon` catalog already exists in the prod
  workspace (dbr_dev_trial). Do not recreate the connection.
- `Lab7/06_gold_dim_date.ipynb` – the pattern for a standalone parameterized gold notebook task.
- `databricks.yml` – the bundle. Prod job is `entsoe-pipeline-prod`; `run_pipeline` builds
  `consumption_hourly` in the gold schema, `build_dim_date` runs after it.

### Facts
- `neon.public.dc_dim` has `dc_id` (PK), `operator`, `city`, `bidding_zone`, `latitude`, `longitude`,
  `it_power_mw`, `power_density_kw_m2`, and more.
- `consumption_hourly.site_id` matches `dc_dim.dc_id` exactly (e.g. `DC-PL-01`).

### Requirements
1. Create a parameterized notebook `Lab7/08_gold_dc_map.ipynb` (widgets: catalog, bronze_schema,
   gold_schema; defaults matching the prod target).
2. Materialize a bronze snapshot of `neon.public.dc_dim` into the project's own bronze schema, so the
   dashboard does not depend on Neon being reachable at every refresh.
3. Build the gold table `dc_map`, joining each data center to its LATEST consumption reading (one row
   per site, most recent date/hour). It must include location, operator, power fields, the latest
   consumption metrics, and a boolean `is_energy_consumed` (a data center with no recent, non-zero
   consumption reading must be marked false).
4. Every data center in `dc_dim` must appear exactly once, including ones with no consumption at all –
   otherwise the map can never show a red point. Choose the join direction accordingly.
5. Wire it into the bundle as a task in the prod job only (dev is Free Edition with no `neon`
   catalog), running after the declarative pipeline, in parallel with the dim_date task.

### Conventions
- English comments/markdown; en-dashes (–), never em-dashes (—); parameterize everything, no
  hardcoded catalog/schema/paths.

### Acceptance criteria
- `bundle validate -t prod` passes.
- `dc_map` has exactly one row per `dc_dim` data center.
- A data center with a recent non-zero consumption reading has `is_energy_consumed = true`; one with
  none has `false` (and still appears, so it can render red).
- `latitude`/`longitude` are non-null for every row.
