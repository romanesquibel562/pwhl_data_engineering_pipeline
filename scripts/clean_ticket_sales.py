# scripts/clean_ticket_sales.py
from pathlib import Path
import logging
import pandas as pd
import yaml
from typing import Dict, Any, List

# ---------- utilities ----------
def find_project_root(start: Path) -> Path:
    p = start.resolve()
    for parent in [p] + list(p.parents):
        if (parent / "config").exists() and (parent / "data").exists():
            return parent
    return start.parent

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

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(" ", "_", regex=False)
    )
    return df

def norm_section(s: pd.Series) -> pd.Series:
    return (s.astype(str)
              .str.strip()
              .str.replace(r"\s+", " ", regex=True)
              .str.title())

# ---------- main ----------
def main():
    ROOT = find_project_root(Path(__file__).resolve())
    LOG_FILE  = ROOT / "logs" / "clean_ticket_sales.log"
    setup_logging(LOG_FILE)

    # Load markets.yml (authoritative list of venues/markets)
    try:
        mk = load_yaml(ROOT / "config" / "markets.yml")
        MARKETS: List[Dict[str, Any]] = mk.get("markets", []) if isinstance(mk, dict) else []
    except FileNotFoundError:
        MARKETS = []
    if not MARKETS:
        logging.error("No markets found in config/markets.yml")
        return

    # Paths
    raw_tickets   = ROOT / "data" / "raw" / "pwhl_ticket_sales.csv"
    capacity_path = ROOT / "data" / "raw" / "game_section_capacity.csv"
    clean_dir     = ROOT / "data" / "cleaned"

    logging.info("Project root: %s", ROOT)
    logging.info("Expecting raw ticket sales at: %s", raw_tickets)

    if not raw_tickets.exists():
        logging.error("Raw ticket sales file not found: %s", raw_tickets)
        return
    if not capacity_path.exists():
        logging.error("Capacity file not found: %s", capacity_path)
        return

    # Load tickets
    df = pd.read_csv(raw_tickets)
    normalize_cols(df)
    logging.info("Loaded raw ticket sales: %s (%d rows, %d cols)", raw_tickets, len(df), df.shape[1])

    # Validate expected ticket columns
    expected = {
        "event_date", "section", "row", "seat",
        "ticket_price", "purchase_channel", "acct_id",
        "num_tickets", "total_spend"
    }
    missing = expected.difference(df.columns)
    if missing:
        logging.error("Missing columns in ticket sales: %s", ", ".join(sorted(missing)))
        return

    # Parse/clean ticket types
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    df["section"] = norm_section(df["section"])
    df["purchase_channel"] = df["purchase_channel"].astype(str).str.strip()
    for c in ["row", "seat", "ticket_price", "num_tickets", "total_spend"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Integrity check
    calc_spend = (df["ticket_price"] * df["num_tickets"]).round(2)
    mismatch = (df["total_spend"].round(2) != calc_spend)
    if mismatch.any():
        logging.warning("Found %d rows where total_spend != ticket_price * num_tickets", int(mismatch.sum()))

    # Load capacity (section capacities per event_date, section)
    cap = pd.read_csv(capacity_path)
    normalize_cols(cap)
    req_cap = {"event_date", "section", "section_capacity"}
    miss_cap = req_cap.difference(cap.columns)
    if miss_cap:
        logging.error("Capacity file missing columns: %s", ", ".join(sorted(miss_cap)))
        return

    cap["event_date"] = pd.to_datetime(cap["event_date"], errors="coerce").dt.date
    cap["section"] = norm_section(cap["section"])

    # Ensure uniqueness on capacity join keys (event_date, section)
    dupes = cap.duplicated(subset=["event_date", "section"], keep=False)
    if dupes.any():
        logging.error("Duplicate keys in capacity for (event_date, section). Sample:\n%s",
                      cap.loc[dupes, ["event_date", "section"]].head(10))
        return

    # Merge tickets with capacity once (base frame)
    base = df.merge(
        cap[["event_date", "section", "section_capacity"]],
        on=["event_date", "section"],
        how="left",
        validate="m:1"
    )

    # Loop over all markets and stamp venue_id/market/venue
    clean_dir.mkdir(parents=True, exist_ok=True)
    outputs = []
    for m in MARKETS:
        venue_id = str(m.get("venue_id", "")).strip()
        market   = str(m.get("market", "")).strip()
        venue    = str(m.get("venue", "")).strip()
        if not venue_id or not market or not venue:
            logging.warning("Skipping malformed market entry: %s", m)
            continue

        logging.info("Processing market: %s (%s)", market, venue_id)
        df_m = base.copy()
        df_m["venue_id"] = venue_id
        df_m["market"]   = market
        df_m["venue"]    = venue

        out = clean_dir / f"ticket_sales_clean_{venue_id}.csv"
        df_m.to_csv(out, index=False)
        outputs.append(out)
        logging.info("Saved: %s (%d rows)", out, len(df_m))

    # Optional: write a combined file with all markets
    if outputs:
        combined = pd.concat([pd.read_csv(p) for p in outputs], ignore_index=True)
        combined_out = clean_dir / "ticket_sales_clean_all_markets.csv"
        combined.to_csv(combined_out, index=False)
        logging.info("Saved combined all-markets file: %s (%d rows)", combined_out, len(combined))
    else:
        logging.error("No market files were produced. Check markets.yml format.")

if __name__ == "__main__":
    main()
    
# run with: python scripts\clean_ticket_sales.py