# Declarative Pipelines vs Classic Spark Jobs — Lab 5

## What I compared

- **Classic (Lab 4):** three Silver notebooks (`03_silver_entsoe`, `04_silver_sensor_data`,
  `05_silver_dim_datacenter`) that each `CREATE TABLE`, read bronze, deduplicate with a window +
  `MERGE`, and enforce quality with `.filter()` and Delta `CHECK` constraints. Orchestrated by a
  Databricks Job with hand-declared task dependencies.
- **Declarative (Lab 5):** one pipeline file `ldp_pipeline.py` where each table is a decorated function
  (`@dp.materialized_view` / `@dp.table`), quality is declared with `expect_all_*`, and deduplication is
  a `create_auto_cdc_flow`. The engine builds the graph, runs it, and tracks lineage.

## Side by side

| Concern | Classic Spark job (Lab 4) | Declarative pipeline (Lab 5) |
|---|---|---|
| Table creation | I write `CREATE TABLE ... USING DELTA` | Engine creates tables from the function |
| Idempotency / dedup | I write a window + `MERGE` | `create_auto_cdc_flow` (keys + sequence_by) |
| Data quality | `.filter(isNotNull)` + `CHECK` constraints | `expect_all_or_drop` / `expect_all_or_fail` |
| Run order | I declare task dependencies in the Job | Engine infers it from what each table reads |
| Checkpoints / state | I manage them | Managed automatically |
| Retries on failure | I configure them per task | Built in |
| Lineage | Not automatic (task DAG only) | Automatic table + column lineage |
| Quality metrics | Ad-hoc check cells | Per-rule pass/fail metrics in the UI |
| User friendliness | Lots of coding, but deep insight into the process | At first, seems pretty obscure, faster to deploy, but you have less insight under the hood |

## Quick gains

- **Orchestration and correct run order**, inferred from the table references.
- **Automatic retries** and **checkpoint/state management** for the streaming tables.
- **Data lineage** (the pipeline graph) with no extra code — usable for the lineage task.
- **Data-quality metrics** per expectation, per run, visible in the Data quality tab.

## Where classic jobs still win

- **Flexibility.** Arbitrary multi-step logic, custom `MERGE` conditions, and unusual transformations
  are easier to express imperatively than to fit into declared tables.
- **Compute choice.** A classic Job can run on a cluster I pick; the pipeline runs on its own managed
  (serverless) runtime.
- **Familiarity.** The imperative style maps directly to plain PySpark I already knew.

## Costs
**Declarative pipelines** may cost slightly more in compute because of built-in features, but they often reduce development and operational costs.

**Classic Spark pipelines may offer lower compute costs** when highly optimized, but they generally require more engineering effort and maintenance.

For most organizations, declarative pipelines are often more cost-effective for standard production workloads, while classic Spark pipelines can be preferable when maximum performance and low-level cost optimization are essential.