# materialize_dim_market.py
from pathlib import Path
import yaml, pandas as pd

ROOT = Path(__file__).resolve().parents[1]
mk_path = ROOT / "config" / "markets.yml"
out_dir = ROOT / "data" / "cleaned"
out_dir.mkdir(parents=True, exist_ok=True)

with open(mk_path, "r", encoding="utf-8") as f:
    mk = yaml.safe_load(f) or {}
rows = mk.get("markets", [])

df = pd.json_normalize(rows)
if not len(df):
    raise SystemError("No market data found in markets.yml")

df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)

# enforce minimal schema
need = ["venue_id","market","venue","country","lat","lon","timezone"]
for c in need:
    if c not in df.columns: df[c] = None

df = df[need]
df.to_csv(out_dir / "dim_market.csv", index=False)
print("Wrote", out_dir / "dim_market.csv")

# python scripts/materialize_dim_market.py