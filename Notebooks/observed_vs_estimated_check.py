"""What is the unprefixed column (e.g. `beans`) compared with `c_beans`?

Read the output like this:
* observed sparse, estimated nearly complete  -> c_ is a model-filled series
* identical where both exist                  -> c_ = observed + gap filling
* different where both exist                  -> the model also adjusts real
                                                 observations
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "NGA_RTFP_mkt_2007_2026-08-24.csv"
COMMODITIES = [
    "beans", "eggs", "fish", "gari_fao", "groundnuts", "maize_fao",
    "maize_flour", "meat_beef", "meat_goat", "milk", "millet",
    "onions", "rice", "rice_fao", "sorghum_fao", "yam",
]

df = pd.read_csv(RAW_PATH)
rows = []
for c in COMMODITIES:
    obs, est = df[c], df[f"c_{c}"]
    both = obs.notna() & est.notna()
    rel_diff = ((obs[both] - est[both]).abs() / obs[both]).fillna(0)
    rows.append({
        "commodity": c,
        "observed_null_%": round(obs.isna().mean() * 100, 1),
        "estimated_null_%": round(est.isna().mean() * 100, 1),
        "rows_with_both": int(both.sum()),
        "identical_%": round((rel_diff < 0.001).mean() * 100, 1) if both.any() else None,
        "median_diff_%": round(rel_diff.median() * 100, 2) if both.any() else None,
    })

print(pd.DataFrame(rows).to_string(index=False))
print("\ngeo_id per mkt_name (should be 1 everywhere):")
print(df.groupby("mkt_name")["geo_id"].nunique().loc[lambda s: s > 1])
