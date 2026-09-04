# Lab 7: testing and data quality

This lab adds two kinds of confidence to the ENTSO-E datacenter energy cost pipeline: that the
transformation code is correct, and that the data moving through the medallion can be trusted. The first
part is unit testing on pure functions. The second and larger part is data quality checking at every
layer, which is what makes CI/CD worth doing later.

## Part A: unit tests

### Where the transformation logic lives

The silver transformations used to sit in notebook cells. They now live in `silver_transformations.py` as
plain functions (`clean_prices`, `clean_sensor`) that take a DataFrame and return a DataFrame. Because the
pipeline and the tests import the same code, there is one place to fix a bug. The casts are ANSI-safe:
`try_cast` and `try_to_timestamp` turn a bad value into null instead of failing the whole batch, and
`silver_processed_ts` is written at silver time with `current_timestamp()` rather than read from bronze.

The tests are in `tests/test_silver_transformations.py` and run with pytest. `conftest.py` decides which
Spark to use from the installed package: plain pyspark gives a local session, databricks-connect gives a
remote serverless one. The same suite runs in three places with no change to the tests.

### Three ways to run the suite

| # | Scenario | Purpose | Compute | Command |
|---|----------|---------|---------|---------|
| 1 | Debug from the IDE (Databricks Connect) | Step through a transform on real data | Remote serverless (Free Edition) | run/debug in VS Code, `.venv-dbconnect` |
| 2 | Run the suite against remote Spark | Prove the tests pass on remote Spark | Remote serverless | `pytest` in `.venv-dbconnect` |
| 3 | Headless, no IDE | Unattended run, CI style | Local (CI) or serverless job | `pytest`, `databricks bundle run tests_job` |

### 1. Debug from the IDE with Databricks Connect

Databricks Connect lives in its own venv (`.venv-dbconnect`, databricks-connect and no pyspark, because the
two conflict). After authenticating against Free Edition, the session comes from
`DatabricksSession.builder.serverless().getOrCreate()`. With that set up I can put a breakpoint inside
`clean_prices` and step through it while the work runs on remote serverless. The
`pyspark.sql.connect.session.SparkSession` shown in the Variables panel is the proof that the compute is
remote and not local.

![VS Code stopped on a breakpoint in clean_prices, running against remote serverless through Databricks Connect](image.png)

### 2. Run the whole suite against remote Spark

Running `pytest` in `.venv-dbconnect` executes every test on that serverless session.

![pytest output with all tests passing against remote serverless](image-1.png)

### 3. Headless, no IDE

There are two headless forms. Local headless is `pytest` from the terminal in `.venv`, which is also what CI
uses.

![headless pytest run from the terminal](image-2.png)

On the platform, `tests_job` runs the same suite as a bundle job on serverless, triggered with
`databricks bundle run tests_job -t dev`.

![tests_job running pytest on serverless as a bundle job](image-3.png)

## Continuous integration gate

`.github/workflows/ci.yml` runs the unit tests on every push and pull request. GitHub starts an Ubuntu
runner, installs pyspark and pytest, and `conftest.py` builds a local Spark session because
databricks-connect is not present in CI. No workspace is involved.

![CI check passing on a pull request](image-6.png)

With branch protection on `main` and the check marked as required, a failing test blocks the merge.
Breaking a test on purpose shows the gate doing its job.

![failing CI check blocking the merge on a pull request](image-5.png)

## Part B: data quality across the medallion

Unit tests check the code. Data quality checks check the data, and the idea is to assert it, measure it, and
gate on it rather than hope for it.

### Quality checks in the pipeline

The declarative pipeline (`ldp_pipeline.py`) runs a DQX suite in silver. `prices_checks` and `sensor_checks`
cover the five dimensions:

- completeness: `is_not_null` on the keys and measures that must be present
- uniqueness: `is_unique` on the price key, and event_id uniqueness on the sensor side comes from the CDC
  dedup into `sensor_silver`
- validity: `is_in_range` for pue, `is_not_less_than` for consumption and price, `is_in_list` for the
  accepted bidding zones
- consistency: a cross-column `sql_expression`, plus referential checks in reconciliation
- timeliness: `is_data_fresh` on the event timestamp

`apply_checks_by_metadata` tags each row, and the pipeline splits the result into `valid_*` and
`quarantine_*` tables for prices and sensor.

### Quarantine tables

Bad rows are not dropped silently. They go to `quarantine_prices` and `quarantine_sensor` with the DQX
`_errors` detail, so you can group by failing rule and see exactly why a row was rejected. On the current
dev data prices run about 93.76% valid with 172 rows quarantined, and sensor is clean.

### Delta constraints

`dim_date` carries `NOT NULL` on its columns, an informational `PRIMARY KEY` on `date`, and a `CHECK`
constraint on the month range. The constraint is added idempotently, dropping it if it already exists and
then adding it, so the notebook is safe to rerun.

### Reconciliation between layers

The reconciliation notebook (`07_reconciliation.ipynb`) compares layers, writes each result to a
`reconciliation_results` table with a timestamp, and raises if any check fails so the job turns red. It
covers:

- bronze to silver row counts, where the only acceptable gap is the quarantined rows
- the dedup step, where distinct event_id in `valid_sensor` must equal the row count in `sensor_silver`
- referential integrity, where every fact key exists in its dimension, with zero orphans
- an aggregate check between silver and gold

### The date dimension

`dim_date` is generated from a date range with a recursive CTE instead of being scanned out of the fact
table, so it is always contiguous and does not depend on which dates happen to appear in the fact. The range
is parameterized with `dim_date_start` and `dim_date_end`.

## Data quality dashboard

An AI/BI dashboard reads the valid, quarantine, and reconciliation tables. It shows the pass rate per
dataset, valid against quarantined counts, data freshness, a quick status of the latest reconciliation run,
and the reconciliation history. The datasets are written as
`IDENTIFIER(:catalog || '.' || :schema || '.<table>')`, so the same dashboard runs on dev and prod by
switching two parameters.

## Running it

The bundle (`databricks.yml`) has a dev target on Free Edition serverless and a prod target on Azure.

- `databricks bundle deploy -t dev`
- `databricks bundle run tests_job -t dev` runs the unit tests headless on serverless
- `databricks bundle run entsoe_silver_ldp -t dev` runs the pipeline: silver with DQX, then gold
- `databricks bundle run gold_dim_job -t dev` seeds bronze, runs the pipeline, builds `dim_date`, and
  refreshes the dashboard

DQX is installed for the pipeline through the environment dependencies in `databricks.yml`, pinned to a
version so a later release cannot change the behaviour under it.

## Notes on dev and prod

On dev the pipeline runs with a full refresh, because the bronze generator overwrites its source on every
run and a streaming read cannot follow an overwrite. On prod the sensor stream is real and append-only, so
it stays incremental. Names in the dashboard and the reconciliation notebook are built from the catalog and
schema parameters rather than hardcoded, so nothing points at the dev catalog when the code runs on prod.
