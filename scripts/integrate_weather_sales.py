# scripts/integrate_weather_sales.py
from pathlib import Path
import logging
import pandas as pd
from typing import List

# ----------------- utilities -----------------
def find_project_root(start: Path) -> Path:
    p = start.resolve()
    for parent in [p] + list(p.parents):
        if (parent / "config").exists() and (parent / "data").exists():
            return parent
    return start.parent

def setup_logging(log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
    )

def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize to an 'event_date' column of dtype date
    if "event_date" not in df.columns:
        if "date" in df.columns:
            df = df.rename(columns={"date": "event_date"})
        elif "time" in df.columns:
            df = df.rename(columns={"time": "event_date"})
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    return df

def _coerce_numeric(df: pd.DataFrame, cols: List[str], as_int: bool = False) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            if as_int:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            else:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    return df

# ----------------- main -----------------
def main():
    ROOT = find_project_root(Path(__file__))
    LOG  = ROOT / "logs" / "integrate_weather_sales.log"
    setup_logging(LOG)

    CLEAN_DIR   = ROOT / "data" / "cleaned"
    OUT_FACT    = CLEAN_DIR / "fact_ticket_sales_with_weather.csv"

    # Prefer the single combined sales file created by clean_ticket_sales.py
    sales_all  = CLEAN_DIR / "ticket_sales_clean_all_markets.csv"
    cap_path   = CLEAN_DIR / "section_capacity_clean.csv"
    wx_daily   = CLEAN_DIR / "weather_daily_by_venue.csv"

    # ---------- Load sales ----------
    if sales_all.exists():
        sales = pd.read_csv(sales_all)
        logging.info("Loaded sales (combined): %s (%d rows)", sales_all, len(sales))
    else:
        files = sorted(CLEAN_DIR.glob("ticket_sales_clean_*.csv"))
        files = [p for p in files if p.name != "ticket_sales_clean_all_markets.csv"]
        if not files:
            logging.error("No cleaned sales files found in %s", CLEAN_DIR)
            return
        sales = pd.concat((pd.read_csv(p) for p in files), ignore_index=True)
        logging.info("Loaded sales from %d per-market files (%d rows)", len(files), len(sales))

    # ---------- Load capacity & weather ----------
    for p in (cap_path, wx_daily):
        if not p.exists():
            logging.error("Missing required cleaned file: %s", p)
            return
    cap = pd.read_csv(cap_path)
    wx  = pd.read_csv(wx_daily)

    # ---------- Normalize dates ----------
    sales = _normalize_dates(sales)
    cap   = _normalize_dates(cap)
    wx    = _normalize_dates(wx)

    # ---------- Basic column checks ----------
    need_sales = {"event_date","market","venue_id","venue","section","ticket_price","num_tickets","total_spend"}
    miss_sales = need_sales.difference(sales.columns)
    if miss_sales:
        logging.error("Sales missing columns: %s", ", ".join(sorted(miss_sales)))
        return

    need_cap = {"event_date","market","venue_id","venue","section","section_capacity"}
    miss_cap = need_cap.difference(cap.columns)
    if miss_cap:
        logging.error("Capacity missing columns: %s", ", ".join(sorted(miss_cap)))
        return

    need_wx = {"event_date","market","venue_id","venue",
               "avg_temp_c","min_temp_c","max_temp_c","avg_rh_pct",
               "avg_wind_mps","total_precip_mm","windy_hours","rainy_hours",
               "freezing_hours","hours_observed"}
    miss_wx = {c for c in need_wx if c not in wx.columns}
    if miss_wx:
        logging.error("Weather missing columns: %s", ", ".join(sorted(miss_wx)))
        return

    logging.info("Rows: sales=%d, capacity=%d, weather=%d", len(sales), len(cap), len(wx))

    # ---------- Coerce numerics on sales before aggregation ----------
    sales = _coerce_numeric(sales, ["ticket_price","total_spend"], as_int=False)
    sales = _coerce_numeric(sales, ["num_tickets"], as_int=True)

    # ---------- Aggregate sales at (event_date, market, venue_id, venue, section) ----------
    sales_sec = (
        sales
        .groupby(["event_date","market","venue_id","venue","section"], as_index=False)
        .agg(
            tickets_sold=("num_tickets", "sum"),
            revenue=("total_spend", "sum"),
            avg_price=("ticket_price", "mean")
        )
    )

    # ---------- Validate uniqueness of capacity on join keys ----------
    dupes = cap.duplicated(subset=["event_date","market","venue_id","venue","section"], keep=False)
    if dupes.any():
        sample = cap.loc[dupes, ["event_date","market","venue_id","venue","section"]].head(10)
        logging.error("Duplicate capacity keys detected on (event_date, market, venue_id, venue, section). Sample:\n%s", sample)
        return

    # ---------- Join capacity (m:1) ----------
    merged = sales_sec.merge(
        cap[["event_date","market","venue_id","venue","section","section_capacity"]],
        how="left",
        on=["event_date","market","venue_id","venue","section"],
        validate="m:1"
    )

    # ---------- Utilization ----------
    merged["utilization"] = (merged["tickets_sold"] / merged["section_capacity"]).astype("float64")

    # ---------- Coerce numerics for weather ----------
    merged = _coerce_numeric(merged, ["tickets_sold","section_capacity"], as_int=True)
    merged = _coerce_numeric(merged, ["revenue","avg_price","utilization"], as_int=False)

    wx = _coerce_numeric(
        wx,
        ["avg_temp_c","min_temp_c","max_temp_c","avg_rh_pct","avg_wind_mps","total_precip_mm"],
        as_int=False
    )
    wx = _coerce_numeric(wx, ["windy_hours","rainy_hours","freezing_hours","hours_observed"], as_int=True)

    # ---------- Join weather (m:1) ----------
    fact = merged.merge(
        wx[[
            "event_date","market","venue_id","venue",
            "avg_temp_c","min_temp_c","max_temp_c","avg_rh_pct","avg_wind_mps",
            "total_precip_mm","windy_hours","rainy_hours","freezing_hours","hours_observed"
        ]],
        how="left",
        on=["event_date","market","venue_id","venue"],
        validate="m:1"
    )

    # ---------- Final ordering & write ----------
    fact = fact.sort_values(["event_date","market","venue_id","section"]).reset_index(drop=True)

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    fact.to_csv(OUT_FACT, index=False)

    logging.info("Saved integrated fact: %s (%d rows, %d cols)", OUT_FACT, len(fact), fact.shape[1])
    logging.info("Integration complete.")

if __name__ == "__main__":
    main()

# run code:
# python scripts/integrate_weather_sales.py