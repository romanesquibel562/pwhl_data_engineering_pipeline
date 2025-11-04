# ingest_weather.py
import logging
from pathlib import Path
import pandas as pd
import requests
import yaml
from typing import Dict, Any

def load_yaml(path: Path) -> Dict[str, Any]:
    """Load a YAML config file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(log_file: Path):
    """Initialize logging with both file and console output."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

def fetch_market_hourly(
    base_url: str,
    hourly_vars: str,
    start: str,
    end: str,
    lat: float,
    lon: float,
    tz: str,
    timeout: int,
) -> pd.DataFrame:
    """Fetch hourly weather data for a single market."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": hourly_vars,
        "timezone": tz,
    }
    r = requests.get(base_url, params=params, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    return pd.DataFrame(j["hourly"])

def main():
    # --- Universal, portable path setup ---
    ROOT = Path(__file__).resolve().parents[1]  # project root
    CONFIG_DIR = ROOT / "config"

    # Load configuration
    settings = load_yaml(CONFIG_DIR / "settings.yml")
    markets_cfg = load_yaml(CONFIG_DIR / "markets.yml")

    # Weather parameters
    w = settings["weather"]
    base_url = w["base_url"]
    start = w["start_date"]
    end = w["end_date"]
    hourly_vars = w["hourly"]
    timeout = int(w["timeout_seconds"])

    # Make paths portable by resolving relative to ROOT
    out_csv = ROOT / w["out_csv"]
    log_file = ROOT / w["log_file"]

    setup_logging(log_file)
    logging.info("Starting weather ingestion: %s → %s", start, end)

    markets = markets_cfg.get("markets", [])
    if not markets or len(markets) < 8:
        logging.warning("Expected 8 markets; found %d. Proceeding anyway.", len(markets))

    frames = []
    for m in markets:
        market = m["market"]
        venue = m.get("venue", "")
        lat = float(m["lat"])
        lon = float(m["lon"])
        tz = m["timezone"]

        try:
            logging.info(
                "Fetching %s (%s) lat=%.4f lon=%.4f tz=%s",
                market, venue, lat, lon, tz
            )
            df = fetch_market_hourly(base_url, hourly_vars, start, end, lat, lon, tz, timeout)
            df["market"] = market
            df["venue"] = venue
            frames.append(df)
            logging.info("Fetched %d rows for %s", len(df), market)
        except Exception as e:
            logging.exception("Failed for %s: %s", market, e)

    if not frames:
        logging.error("No weather data fetched — exiting without writing CSV.")
        return

    all_hourly = pd.concat(frames, ignore_index=True)

    # Save results
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    all_hourly.to_csv(out_csv, index=False)
    logging.info(
        "Saved %s (%d rows, %d columns)",
        out_csv,
        len(all_hourly),
        all_hourly.shape[1],
    )
    logging.info("Ingestion complete.")

if __name__ == "__main__":
    main()

# run code:
# python scripts/ingest_weather.py
