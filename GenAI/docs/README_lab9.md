# Lab 9 - Databricks REST API Automation

Automating platform operations on Databricks with the Python SDK. A small command-line tool
(`orchestrator.py`, built with Typer) that provisions compute, runs a job, triggers a pipeline, and
reports status end to end, integrated into the CI/CD flow from Lab 8.

## Goal

Automate platform operations through the API instead of clicking in the Databricks UI. Done when a
script provisions compute, runs a job, and reports its status end to end, and the automation is wired
into the Week 8 CI/CD flow.

## Approach

The tool uses the **Databricks Python SDK** (`databricks-sdk`), not raw REST. `WorkspaceClient` wraps
the same REST API, handling authentication, pagination, retries and long-running-operation waiters, so
the code stays small. It reuses the exact same `DATABRICKS_HOST` / `DATABRICKS_TOKEN` PAT secrets set up
in Lab 8. Everything lives in one file, `orchestrator.py`, exposed as a Typer CLI.

## The CLI (`orchestrator.py`)

- `ensure-compute` - ensures the shared **GP1** cluster is running (starts it if terminated, polls until
  RUNNING).
- `run [--job NAME]` - the end-to-end flow: ensure GP1, run the named job, monitor its task states live,
  and print a report. Pass `--job` for non-interactive use (CI); omit it for an interactive prompt.
- `start-pipeline [--pipeline-name NAME]` - triggers the Lakeflow declarative pipeline and polls its
  update state until it reaches a terminal state.

Typer turns each function into a subcommand with `--help` for free. The SDK is what performs the work;
the CLI is the control panel that lets a human (or CI) trigger each action by name.

## Provision compute

- **Cost-safe default:** `ensure-compute` brings up the shared, auto-terminating **GP1** cluster and
  never deletes it. Jobs attach to GP1 via `existing_cluster_id`, so no throwaway compute is created.
- **Access mode:** any cluster created directly must use `data_security_mode = SINGLE_USER`, because this
  Unity Catalog workspace blocks legacy no-isolation clusters.
- **One-off creation demo:** a separate `lab9-experimentation` single-node cluster (cheapest option) was
  created to demonstrate literal cluster provisioning, run once with permission, then permanently deleted
  to avoid cost.

## Run a job and monitor status

`w.jobs.run_now(job_id=...)` triggers the job; the SDK waiter blocks until the run reaches a terminal
state, with a **callback** printing each task's life-cycle state as it changes
(PENDING -> RUNNING -> TERMINATED). The final `result_state` (SUCCESS / FAILED) is the verdict. A `rich`
table reports the job name, run id, state, result, and start/end times (epoch milliseconds formatted to
readable UTC).

![Interactive run with the status report](image.png)

The same flow driven non-interactively with the `--job` option, which is what CI uses:

![run with the --job option](image-1.png)

## Trigger a pipeline and monitor it

A pipeline is a different API surface from a job: it lives under `w.pipelines`, not `w.jobs`, and has no
one-line waiter. So `start-pipeline` calls `start_update` and then polls
`get_update(...).update.state` in a loop until a terminal state, printing each stage
(WAITING_FOR_RESOURCES -> INITIALIZING -> SETTING_UP_TABLES -> RUNNING -> COMPLETED).

![start-pipeline progressing to COMPLETED](image-2.png)

## Authentication

`WorkspaceClient()` reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment, the same PAT
secrets used by the Lab 8 deploy. Nothing is hardcoded, so the identical code runs locally and in CI.

## CI integration (`.github/workflows/automation.yml`)

The automation is wired into GitHub Actions, reusing the Lab 8 secrets:

- Trigger: `workflow_dispatch` (a manual "Run workflow" button).
- Steps: checkout, `setup-python`, `pip install databricks-sdk rich typer`, then
  `python Lab9/orchestrator.py run --job entsoe-unit-tests-prod`.
- CI-safety: the `--job` option supplies the job name so `input()` is never reached, and
  `sys.stdin.isatty()` guards the interactive prompt so the script fails fast instead of hanging when
  there is no terminal. The script exits non-zero on failure, so the run also acts as a gate.

![Green CI Automation run](image-3.png)

## Done when

- [x] A script provisions compute (ensures GP1 up), runs a job, and reports its status end to end.
- [x] The declarative pipeline is triggered and monitored programmatically (`start-pipeline`).
- [x] Clusters, jobs and notebooks are driven through the SDK (including a one-off cluster creation).
- [x] The automation is integrated with the Lab 8 CI/CD flow (`automation.yml`, green).
- [x] Optional platform-management CLI built with Typer.

## How to run

```powershell
pip install -r ../requirements.txt        # or: pip install databricks-sdk rich typer
# auth: DATABRICKS_HOST + DATABRICKS_TOKEN in the environment

python orchestrator.py ensure-compute
python orchestrator.py run --job entsoe-unit-tests-prod
python orchestrator.py start-pipeline
```

In CI the same `run --job ...` command is invoked by `automation.yml`.

## Assumptions and notes

- Shared Academy workspace: **GP1** (`0702-132442-toro5spu`) is the shared cluster jobs attach to;
  serverless for notebooks is not allowed, and the declarative pipeline runs on a classic single-node
  cluster (configured in Lab 8).
- Cost hygiene: GP1 is never deleted (shared, auto-terminating); any cluster created directly is given
  `autotermination_minutes` and deleted after use.
- Enum values from the SDK are read with `.value` (for example `state.value`, `result_state.value`) to
  get clean strings for comparisons and display.
- Dependencies: `databricks-sdk`, `rich`, `typer` (pinned in `requirements.txt`); everything else in the
  script is the Python standard library.
- Productionization next steps: replace the PAT with a service principal (as noted in Lab 8), and add a
  scheduled trigger (`on: schedule`) if a timed run is wanted.
