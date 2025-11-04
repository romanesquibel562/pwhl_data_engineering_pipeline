# transform_weather.py
from pathlib import Path
import logging
import pandas as pd
import yaml
from typing import Dict, Any
import re

def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )

def slugify(*parts: str) -> str:
    """Stable fallback key from text parts (lowercase alnum and underscores)."""
    s = "_".join([p or "" for p in parts])
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def build_markets_df(markets_cfg: Dict[str, Any]) -> pd.DataFrame:
    """Create a normalization table with market, venue, venue_id, country."""
    items = markets_cfg.get("markets", [])
    rows = []
    for it in items:
        rows.append({
            "market": str(it.get("market", "")).strip(),
            "venue": str(it.get("venue", "")).strip(),
            "venue_id": str(it.get("venue_id", "")).strip(),
            "country": str(it.get("country", "")).strip(),
        })
    md = pd.DataFrame(rows)
    # Drop empties if any
    md = md[(md["market"] != "") & (md["venue"] != "")]
    return md

def main():
    ROOT = Path(__file__).resolve().parents[1]
    CONFIG_DIR = ROOT / "config"
    CLEAN_DIR = ROOT / "data" / "cleaned"
    LOG_FILE = ROOT / "logs" / "transform_weather.log"

    setup_logging(LOG_FILE)

    # Load settings & markets
    settings = load_yaml(CONFIG_DIR / "settings.yml")
    markets_cfg = load_yaml(CONFIG_DIR / "markets.yml")
    markets_df = build_markets_df(markets_cfg)

    w = settings["weather"]
    raw_csv = ROOT / w["out_csv"]  # e.g., data/raw/weather/weather_hourly_2025-01_2025-02.csv
    if not raw_csv.exists():
        logging.error("Raw weather CSV not found: %s", raw_csv)
        return

    logging.info("Reading raw weather: %s", raw_csv)
    df = pd.read_csv(raw_csv)

    # REQUIRED MINIMAL COLUMNS present in your file
    required_min = {"time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "precipitation", "market", "venue"}
    missing_min = required_min.difference(df.columns)
    if missing_min:
        logging.error("Missing required columns: %s", ", ".join(sorted(missing_min)))
        return

    # Parse time & event_date
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["event_date"] = df["time"].dt.date

    # Rename weather columns
    df = df.rename(columns={
        "temperature_2m": "temp_c",
        "relative_humidity_2m": "rh_pct",
        "wind_speed_10m": "wind_mps",
        "precipitation": "precip_mm",
    })

    # Numeric coercion
    for c in ["temp_c", "rh_pct", "wind_mps", "precip_mm"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --- Enrich with venue_id, country from markets.yml ---
    pre_count = len(df)
    df = df.merge(
        markets_df,
        how="left",
        on=["market", "venue"],
        validate="m:1"
    )
    matched = df["venue_id"].notna().sum()
    logging.info("Enrichment: matched %d / %d rows to markets.yml", matched, pre_count)

    # Fallbacks for any unmatched rows
    df["venue_id"] = df["venue_id"].fillna(df.apply(lambda r: slugify(r["market"], r["venue"]), axis=1))
    df["country"] = df["country"].fillna("")

    # Reorder columns for readability
    hourly_cols = ["time", "event_date", "market", "country", "venue_id", "venue",
                   "temp_c", "rh_pct", "wind_mps", "precip_mm"]
    df_hourly = df[hourly_cols].sort_values(["market", "venue_id", "time"]).reset_index(drop=True)

    # Save hourly tidy
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    hourly_out = CLEAN_DIR / "weather_hourly_tidy.csv"
    df_hourly.to_csv(hourly_out, index=False)
    logging.info("Saved hourly tidy: %s (%d rows)", hourly_out, len(df_hourly))

    # Daily features by venue_id + date
    windy_thresh = 8.0        # m/s (~18 mph)
    rainy_thresh = 0.0        # >0mm counts as rainy hour
    freezing_thresh = 0.0     # <=0°C

    df_hourly["is_windy_hour"] = (df_hourly["wind_mps"] >= windy_thresh).astype("Int64")
    df_hourly["is_rainy_hour"] = (df_hourly["precip_mm"] > rainy_thresh).astype("Int64")
    df_hourly["is_freezing_hour"] = (df_hourly["temp_c"] <= freezing_thresh).astype("Int64")

    daily = (
        df_hourly
        .groupby(["event_date", "market", "country", "venue_id", "venue"], as_index=False)
        .agg(
            avg_temp_c=("temp_c", "mean"),
            min_temp_c=("temp_c", "min"),
            max_temp_c=("temp_c", "max"),
            avg_rh_pct=("rh_pct", "mean"),
            avg_wind_mps=("wind_mps", "mean"),
            total_precip_mm=("precip_mm", "sum"),
            windy_hours=("is_windy_hour", "sum"),
            rainy_hours=("is_rainy_hour", "sum"),
            freezing_hours=("is_freezing_hour", "sum"),
            hours_observed=("time", "count"),
        )
    )

    float_cols = ["avg_temp_c", "min_temp_c", "max_temp_c", "avg_rh_pct", "avg_wind_mps", "total_precip_mm"]
    daily[float_cols] = daily[float_cols].round(2)

    daily_out = CLEAN_DIR / "weather_daily_by_venue.csv"
    daily.to_csv(daily_out, index=False)
    logging.info("Saved daily features: %s (%d rows)", daily_out, len(daily))

    logging.info("transform_weather complete.")

if __name__ == "__main__":
    main()

# run code:
# python scripts/transform_weather.py