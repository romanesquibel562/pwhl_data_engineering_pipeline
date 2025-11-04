# scripts/load_to_bq.py
import os
import yaml
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()  # load .env if present

ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "config" / "settings.yml"

# ---------- helpers ----------
def load_cfg(path=CFG_PATH):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str, location: str = "US"):
    """Create dataset if missing."""
    ds_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    ds_ref.location = location
    client.create_dataset(ds_ref, exists_ok=True)

# ---------- main ----------
def main():
    cfg = load_cfg()
    bq_cfg = cfg["bigquery"]

    # Resolve project/dataset/table
    project_id = os.getenv("BQ_PROJECT_ID") or bq_cfg.get("project_id")
    if not project_id:
        raise RuntimeError("BQ_PROJECT_ID not set in environment or config.")

    dataset_id = bq_cfg["dataset_id"]
    table_id   = bq_cfg["table_id"]
    location   = bq_cfg.get("location", "US")
    write_disp = bq_cfg.get("write_disposition", "WRITE_TRUNCATE")

    source_csv = ROOT / bq_cfg["source_csv"]
    if not source_csv.exists():
        raise FileNotFoundError(f"Source CSV not found: {source_csv}")

    # ---------- read + type clean ----------
    df = pd.read_csv(source_csv, parse_dates=["event_date"])
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    float_cols = [
        "revenue","avg_price","utilization",
        "avg_temp_c","min_temp_c","max_temp_c",
        "avg_rh_pct","avg_wind_mps","total_precip_mm"
    ]
    int_cols = [
        "tickets_sold","section_capacity",
        "windy_hours","rainy_hours","freezing_hours","hours_observed"
    ]
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # ---------- schema ----------
    schema = [
        bigquery.SchemaField("event_date","DATE","REQUIRED"),
        bigquery.SchemaField("market","STRING"),
        bigquery.SchemaField("venue_id","STRING"),
        bigquery.SchemaField("venue","STRING"),
        bigquery.SchemaField("section","STRING"),
        bigquery.SchemaField("tickets_sold","INTEGER"),
        bigquery.SchemaField("revenue","FLOAT"),
        bigquery.SchemaField("avg_price","FLOAT"),
        bigquery.SchemaField("section_capacity","INTEGER"),
        bigquery.SchemaField("utilization","FLOAT"),
        bigquery.SchemaField("avg_temp_c","FLOAT"),
        bigquery.SchemaField("min_temp_c","FLOAT"),
        bigquery.SchemaField("max_temp_c","FLOAT"),
        bigquery.SchemaField("avg_rh_pct","FLOAT"),
        bigquery.SchemaField("avg_wind_mps","FLOAT"),
        bigquery.SchemaField("total_precip_mm","FLOAT"),
        bigquery.SchemaField("windy_hours","INTEGER"),
        bigquery.SchemaField("rainy_hours","INTEGER"),
        bigquery.SchemaField("freezing_hours","INTEGER"),
        bigquery.SchemaField("hours_observed","INTEGER"),
    ]

    client = bigquery.Client(project=project_id, location=location)
    ensure_dataset(client, project_id, dataset_id, location)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # If table missing, create with partitioning + clustering
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="event_date"
        )
        table.clustering_fields = ["market","venue_id","section"]
        client.create_table(table)

    # Write temp parquet for efficient load
    tmp_dir = ROOT / "data" / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_parquet = tmp_dir / "fact_ticket_sales_with_weather.parquet"
    df.to_parquet(tmp_parquet, index=False)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disp,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    with open(tmp_parquet, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)
    result = job.result()

    print(f"Loaded {result.output_rows} rows into {table_ref}")

if __name__ == "__main__":
    main()

# python scripts/load_to_bq.py