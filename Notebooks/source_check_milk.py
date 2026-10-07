"""Check the December 2025 Borno milk anomaly against WFP's own price data.

The World Bank RTFP file is compiled partly from WFP. If WFP's file shows a
different unit for that round, the anomaly is a unit change the compilation
did not convert. If WFP shows normal prices in the same unit, the error was
introduced downstream of WFP.

Download "Nigeria - Food Prices" (WFP) from HDX and save the CSV as
data/raw/wfp_food_prices_nga.csv. Reads only; writes nothing.
"""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
WFP_PATH = ROOT / "data" / "raw" / "wfp_food_prices_nga.csv"
SILVER_PATH = ROOT / "data" / "silver" / "food_prices"
pd.set_option("display.width", 200)

# HDX files carry a second header row of HXL tags (#date, #adm1 ...): skip it.
wfp = pd.read_csv(WFP_PATH, skiprows=[1])
print("WFP columns:", list(wfp.columns))
wfp["date"] = pd.to_datetime(wfp["date"])
print("WFP file covers:", wfp["date"].min().date(), "to", wfp["date"].max().date())

window = wfp["date"].between("2025-09-01", "2026-03-31")
milk = wfp[window
           & wfp["admin1"].eq("Borno")
           & wfp["commodity"].str.contains("milk", case=False, na=False)]

if milk.empty:
    print("\nNo Borno milk prices in WFP's file for Sep 2025 to Mar 2026.")
    print("The anomaly cannot be confirmed from this source.")
else:
    print("\n=== WFP: Borno milk by month, unit and price type ===")
    summary = (milk.groupby(["date", "commodity", "unit", "pricetype"])
               .agg(n_markets=("market", "nunique"),
                    median_price=("price", "median"),
                    min_price=("price", "min"),
                    max_price=("price", "max"))
               .reset_index())
    print(summary.to_string(index=False))

    print("\n=== WFP: December 2025 by market ===")
    dec = milk[milk["date"].dt.to_period("M") == "2025-12"]
    print(dec[["market", "commodity", "unit", "pricetype", "price"]]
          .sort_values("market").to_string(index=False))

# Our side, for a market-by-market comparison by eye (names may differ).
silver = pd.read_parquet(SILVER_PATH)
silver["date"] = pd.to_datetime(silver["date"])
silver["commodity"] = silver["commodity"].astype(str)
ours = silver[(silver["state"] == "Borno")
              & (silver["commodity"] == "milk")
              & (silver["date"] == "2025-12-01")]
print("\n=== RTFP (our Silver): Borno milk, December 2025 ===")
print(ours[["market", "price_observed", "price_estimated", "price_is_spike"]]
      .sort_values("market").to_string(index=False))
