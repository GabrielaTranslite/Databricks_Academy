# Importing necessary libraries
import datetime
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from rich.console import Console
from rich.table import Table
import sys
import typer

app = typer.Typer()

console = Console()
w = WorkspaceClient()

@app.command()
def ensure_compute():
    """Ensures that the specified Databricks cluster is running. If the cluster is terminated, it starts the cluster and waits until it is in a RUNNING state."""
    cluster_id = "0702-132442-toro5spu"
    poll_interval = 30

    cluster = w.clusters.get(cluster_id)
    print(f"Initial state: {cluster.state}")

    if cluster.state.value == "TERMINATED":
        print(f"Starting cluster {cluster_id}...")
        w.clusters.start(cluster_id)

    while True:
        cluster = w.clusters.get(cluster_id)
        state = cluster.state.value
        print(f"Cluster {cluster_id} state: {state}")

        if state == "RUNNING":
            print("Cluster is ready.")
            return cluster

        if state in {"ERROR", "UNKNOWN"}:
            error_message = getattr(cluster, "state_message", None)
            raise RuntimeError(
                f"Cluster failed to start. State: {state}"
                + (f"; message: {error_message}" if error_message else "")
            )
        

        # Examples: PENDING, STARTING, RESTARTING, RESIZING
        time.sleep(poll_interval)
    

def ask_job_name(job_name = None):
    """Return the job name. Use the provided value, else prompt; in CI (no TTY) fail fast."""
    if job_name:
        return job_name
    if not sys.stdin.isatty():                 # CI / non-interactive
        print("No --job provided and no interactive terminal (CI). Aborting.")
        raise typer.Exit(code=1)
    return input("Enter the job name to run: ")

def run_job(job_name):
    """Runs a specified job by name and waits for its completion. Returns the run object."""
    jobs = list(w.jobs.list())
    names = [j.settings.name for j in jobs if j.settings.name and "entsoe" in j.settings.name] # Filter out jobs
    # Keep asking for a job name until a valid one is provided
    while job_name not in names:
        print(f"No job named {job_name!r}. Available jobs:")
        for n in sorted(names):
            print("  -", n)
        if not sys.stdin.isatty():             # in CI / non-interactive, fail fast
            print("No --job provided and no interactive terminal (CI). Aborting.")
            raise typer.Exit(code=1)
        job_name = input("Enter the job name to run: ")

    job = next(j for j in jobs if j.settings.name == job_name) # Get the job object corresponding to the provided name
    print(f"Running job: {job.settings.name} (ID: {job.job_id})")
    run = w.jobs.run_now(job_id=job.job_id).result(
        timeout=datetime.timedelta(minutes=30),
        callback=show_progress,
    )
    print("Final result:", run.state.result_state.value if run.state.result_state else "n/a")
    return run

def show_progress(run):
    """Displays the progress of a running job by printing its current state and result."""
    for task in run.tasks:
        print(task.task_key, "->", task.state.life_cycle_state.value)
        
def format_timestamp(timestamp):
    """Formats a timestamp into a human-readable string."""
    if timestamp is None:
        return "n/a"    
    
    return datetime.datetime.fromtimestamp(
        timestamp / 1000,
        tz=datetime.timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

def report(run, job_name):
    """Prints a report of the run's details including ID, state, result, start time, and end time."""
    table = Table(title="Job Run Report")
    table.add_column("Field")
    table.add_column("Value")
    table.add_row("Job Name", job_name)
    table.add_row("Run ID", str(run.run_id))
    table.add_row("State", run.state.life_cycle_state.value)
    table.add_row("Result", run.state.result_state.value if run.state.result_state else "n/a")
    table.add_row("Start Time", format_timestamp(run.start_time))
    table.add_row("End Time", format_timestamp(run.end_time))
    console.print(table)
    return run.state.result_state

@app.command()
def start_pipeline(pipeline_name: str = "entsoe-silver-ldp-prod"):
    """Triggers a Lakeflow declarative pipeline by name and polls its update state until terminal."""
    # 1. Find the pipeline id by name
    pipelines = list(w.pipelines.list_pipelines())
    pipe = next((p for p in pipelines if p.name == pipeline_name), None)
    if pipe is None:
        names = [p.name for p in pipelines if p.name and "entsoe" in p.name]
        print(f"No pipeline named {pipeline_name!r}. Available: {names}")
        raise typer.Exit(code=1)

    # 2. Trigger an update
    update = w.pipelines.start_update(pipeline_id=pipe.pipeline_id)
    print(f"Started update {update.update_id} on {pipe.name}")

    # 3. Poll the update's own state until terminal
    terminal = {"COMPLETED", "FAILED", "CANCELED"}
    while True:
        info = w.pipelines.get_update(
            pipeline_id=pipe.pipeline_id,
            update_id=update.update_id,
        ).update
        state = info.state.value
        print("pipeline update state:", state)
        if state in terminal:
            break
        time.sleep(15)

    # 4. Report
    print("Pipeline finished:", state)
    return state

@app.command()
def run(job: str = typer.Option(None, "--job", help="Job name to run; skips the prompt (use this in CI).")):
    """Main function to ensure compute is running, execute the job, and report the results."""
    cluster = ensure_compute()
    print(f"Cluster {cluster.cluster_name} is ready for job execution. Continuing with the next steps...")
    job_name = ask_job_name(job)
    run = run_job(job_name)
    result = report(run, job_name)
    ok = result is not None and result.value == "SUCCESS"
    raise typer.Exit(code=0 if ok else 1)

if __name__ == "__main__":
    app()