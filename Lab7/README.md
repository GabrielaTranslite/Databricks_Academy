## Testing

Transformation logic now lives in importable modules (`silver_transformations.py`), so the same
pytest suite can run in three ways. Which Spark engine is used is decided by `conftest.py`
based on the installed package (pyspark = local, databricks-connect = remote serverless).

| # | Scenario | Purpose | Compute | Command |
|---|----------|---------|---------|---------|
| 1 | Debug from local IDE (Databricks Connect) | Step through a transform on real data with breakpoints | Remote serverless (Free Edition) | run/debug in VS Code, `.venv-dbconnect` |
| 2 | Run tests against remote Spark (Connect) | Prove the suite passes on remote Spark | Remote serverless | `pytest` in `.venv-dbconnect` |
| 3 | Headless via CLI / bundle | Unattended run (CI-style), no IDE | Local (CI) / serverless job | `pytest` and `databricks bundle run` |

### 1. Interactive debug from the IDE (Databricks Connect)
- Env: `.venv-dbconnect` (databricks-connect 17.3.*, no pyspark).
- Auth: valid Free Edition profile, selected via `DATABRICKS_CONFIG_PROFILE`
  (not hardcoded, consistent with the bundle auth decision).
- Session: `DatabricksSession.builder.serverless().getOrCreate()`.
- Evidence: screenshot of VS Code stopped on a breakpoint inside `clean_prices`,
  Variables panel showing the DataFrame, run against remote serverless.

### 2. Run the tests against remote Spark (Connect)
```bash
# in .venv-dbconnect
$env:DATABRICKS_CONFIG_PROFILE = "gabrielajaniszews786"   # PowerShell
pytest -v
```
- Evidence: pytest output with all tests PASSED, plus a note/screenshot that the
  compute is remote (e.g. the serverless session id / Spark UI on the workspace).

### 3. Headless via the Databricks CLI / bundle
Two forms, both "no IDE":
- Local headless (this is what CI runs), in `.venv` with real pyspark:
```bash
pytest -q
```
- On-platform, as a bundle job (runs on serverless, triggered from the CLI):
```bash
databricks bundle deploy -t dev
databricks bundle run tests_job -t dev
```
- Evidence: terminal output of the headless `pytest`, and the bundle job run page
  (or CLI run output) showing the test task succeeded.