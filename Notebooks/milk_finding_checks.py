"""Two checks before claiming 'milk in Borno/Yobe is the most volatile':

1. Confounding: are the milk markets mostly in the northeast? If so,
   compare commodities WITHIN the same region, not across regions.
2. Data quality: does the most volatile series look like real price moves,
   or like prices jumping between a few fixed levels (a unit problem)?
"""

from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
NORTHEAST = ["Borno", "Yobe", "Adamawa"]

vol = pd.read_parquet(ROOT / "data" / "gold" / "volatility_by_market")
vol["region"] = np.where(vol["state"].isin(NORTHEAST), "northeast", "rest")

print("CHECK 1a: which states are the qualifying milk markets in?")
print(vol[vol["commodity"] == "milk"]["state"].value_counts().to_string())

print("\nCHECK 1b: median volatility by commodity, within each region")
table = vol.pivot_table(index="commodity", columns="region",
                        values="volatility", aggfunc="median")
counts = vol.pivot_table(index="commodity", columns="region",
                         values="volatility", aggfunc="count")
table = table.join(counts, rsuffix="_n_markets").round(3)
sort_col = "northeast" if "northeast" in table.columns else table.columns[0]
print(table.sort_values(sort_col, ascending=False).to_string())

silver = pd.read_parquet(ROOT / "data" / "silver" / "food_prices")
silver["date"] = pd.to_datetime(silver["date"])
silver["commodity"] = silver["commodity"].astype(str)

series = silver[(silver["market"] == "Monguno")
                & (silver["commodity"] == "milk")
                & (silver["date"] >= "2020-01-01")].sort_values("date")
series = series[["date", "price_observed", "price_estimated"]].copy()
series["change_x"] = (series["price_observed"]
                      / series["price_observed"].shift(1)).round(2)

print("\nCHECK 2: Monguno milk, month by month (change_x = this month / last month)")
print(series.to_string(index=False))

big = ((series["change_x"] >= 2) | (series["change_x"] <= 0.5)).sum()
print(f"\nMonths where the price doubled or halved: {big} of {series['change_x'].notna().sum()}")