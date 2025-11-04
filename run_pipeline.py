# run_pipeline.py
"""
One-command runner for the PWHL take-home ETL:
1) Ingest weather (raw)
2) Transform weather (daily)
3) Clean ticket sales
4) Integrate ticket sales + weather (if present)
5) Load final dataset to BigQuery

Usage:
    python run_pipeline.py
"""

import os
import sys
import time
import subprocess
from pathlib import Path

# .env support
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

ROOT = Path(__file__).resolve().parent
SCRIPTS = ROOT / "scripts"

PIPELINE_STEPS = [
    ("Ingest weather (raw)",           SCRIPTS / "ingest_weather.py"),
    ("Transform weather (daily)",      SCRIPTS / "transform_weather.py"),
    ("Clean ticket sales",             SCRIPTS / "clean_ticket_sales.py"),
    ("Integrate sales + weather",      SCRIPTS / "integrate_weather_sales.py"),  # <==== runs only if present
    ("Load to BigQuery",               SCRIPTS / "load_to_bq.py"),
]

def _check_bq_auth():
    """Attempt to instantiate a BigQuery client to verify ADC."""
    try:
        from google.cloud import bigquery
        client = bigquery.Client()
        _ = client.project
        return True, f"Authenticated to GCP project: {client.project}"
    except Exception as e:
        return False, (
            "BigQuery auth failed. Run `gcloud auth application-default login` and try again.\n"
            f"Details: {e}"
        )

def run_step(name: str, path: Path) -> None:
    if not path.exists():
        print(f"- {name}: SKIPPED (not found at {path.name})")
        return

    print(f"> {name}")
    start = time.time()
    proc = subprocess.run([sys.executable, str(path)], capture_output=True, text=True)

    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.rstrip())
        raise SystemExit(f"Step failed: {name} (exit code {proc.returncode})")

    elapsed = time.time() - start
    print(f"{name} completed in {elapsed:.1f}s")

def main():
    print("Starting PWHL ETL pipeline\n")

    bq_project_id = os.getenv("BQ_PROJECT_ID")
    if not bq_project_id:
        print("Warning: BQ_PROJECT_ID is not set. Create a `.env` with BQ_PROJECT_ID=<your-project-id> or export it.")
    else:
        print(f"Using BQ_PROJECT_ID={bq_project_id}")

    ok, msg = _check_bq_auth()
    print(msg)
    if not ok:
        raise SystemExit(1)

    total_start = time.time()

    for name, script_path in PIPELINE_STEPS:
        run_step(name, script_path)

    total_elapsed = time.time() - total_start
    print(f"\nPipeline complete in {total_elapsed:.1f}s")
    print("Verify data in BigQuery → dataset: `pwhl_takehome`, table: `fact_ticket_sales_with_weather`")

if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        if str(e):
            print(e)
        sys.exit(getattr(e, "code", 1))
    except Exception as e:
        print(f"Unhandled error: {e}")
        sys.exit(1)

# run code:
# python run_pipeline.py