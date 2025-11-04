# scripts/clean_section_capacity.py

from pathlib import Path
import logging
import pandas as pd
import yaml
from typing import Dict, Any, List

# ----------------- utilities -----------------
def find_project_root(start: Path) -> Path:
    p = start.resolve()
    for parent in [p] + list(p.parents):
        if (parent / "config").exists() and (parent / "data").exists():
            return parent
    return start.parent

def load_yaml(p: Path) -> Dict[str, Any]:
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" ", "_", regex=False)
    )
    return df

def norm_section(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.title()
    )

# ----------------- main -----------------
def main():
    ROOT = find_project_root(Path(__file__))
    LOG  = ROOT / "logs" / "clean_section_capacity.log"
    setup_logging(LOG)

    raw_path  = ROOT / "data" / "raw" / "game_section_capacity.csv"
    out_dir   = ROOT / "data" / "cleaned"
    out_dir.mkdir(parents=True, exist_ok=True)

    mk_path = ROOT / "config" / "markets.yml"
    if not mk_path.exists():
        logging.error("Missing config: %s", mk_path)
        return

    # Load markets — authoritative keys for market/venue/venue_id
    mk = load_yaml(mk_path)
    markets: List[Dict[str, Any]] = mk.get("markets", []) if isinstance(mk, dict) else []
    if not markets:
        logging.error("No markets found in %s", mk_path)
        return

    mkt_df = pd.DataFrame([{
        "market":   str(m.get("market", "")).strip(),
        "venue":    str(m.get("venue", "")).strip(),
        "venue_id": str(m.get("venue_id", "")).strip(),
    } for m in markets])

    # Basic validation on market keys
    if mkt_df[["market","venue","venue_id"]].isin(["", None]).any().any():
        logging.error("markets.yml contains empty market/venue/venue_id values. Please fix.")
        return

    if not raw_path.exists():
        logging.error("Capacity file not found: %s", raw_path)
        return

    df = pd.read_csv(raw_path)
    normalize_cols(df)

    expected = {"event_date", "section", "section_capacity"}
    missing = expected.difference(df.columns)
    if missing:
        logging.error("Missing columns in capacity file: %s", ", ".join(sorted(missing)))
        return

    # Clean types
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    df["section"] = norm_section(df["section"])
    df["section_capacity"] = pd.to_numeric(df["section_capacity"], errors="coerce")

    # Optional sanity checks
    if df["event_date"].isna().any():
        bad = int(df["event_date"].isna().sum())
        logging.warning("Found %d rows with unparsable event_date; they will remain NaN.", bad)
    if df["section_capacity"].isna().any():
        bad = int(df["section_capacity"].isna().sum())
        logging.warning("Found %d rows with non-numeric section_capacity; they will remain NaN.", bad)

    # --------- replicate capacity rows for all markets (CROSS JOIN) ---------
    df["__key"] = 1
    mkt_df["__key"] = 1
    df = df.merge(mkt_df, on="__key").drop(columns="__key")

    
    cols = ["event_date", "market", "venue_id", "venue", "section", "section_capacity"]
    df = df[cols].sort_values(["event_date", "market", "venue_id", "section"]).reset_index(drop=True)

    out = out_dir / "section_capacity_clean.csv"
    df.to_csv(out, index=False)
    logging.info("Saved cleaned capacity (replicated per market): %s (%d rows, %d cols)", out, len(df), df.shape[1])

if __name__ == "__main__":
    main()

# run with: python scripts\clean_section_capacity.py