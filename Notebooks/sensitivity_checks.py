"""Robustness checks before rewriting the README findings.

1. Every row flagged as a spike at the default 3x threshold (all of them,
   not just the first 50), and which survey rounds they cluster in.
2. Does milk still rank first when the spike threshold changes?
   Settings: no spike removal, 2x, 3x (default), 5x.
3. Does the ranking survive a shorter window / lower evidence bar?

Reads the saved Silver table; writes nothing.
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from transformations import (  # noqa: E402
    flag_price_spikes, build_volatility_by_market, build_volatility_national,
)

SILVER_PATH = ROOT / "data" / "silver" / "food_prices"

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Sensitivity Checks")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
pd.set_option("display.width", 200)

silver = spark.read.parquet(str(SILVER_PATH)).drop("price_is_spike").cache()

# ---------------------------------------------------------------------------
# 1. All spikes at the default threshold
# ---------------------------------------------------------------------------
flagged_3x = flag_price_spikes(silver, factor=3.0).cache()
spikes = (
    flagged_3x.filter("price_is_spike")
    .select("state", "market", "commodity", "date", "price_observed", "price_estimated")
    .orderBy("commodity", "date", "state", "market")
    .toPandas()
)
print(f"\n=== 1a. ALL {len(spikes)} SPIKES AT 3x ===")
print(spikes.to_string(index=False))

print("\n=== 1b. SURVEY ROUNDS WITH 3+ SPIKES (same commodity, same month) ===")
rounds = (spikes.groupby(["commodity", "date"]).size()
          .rename("n_markets").reset_index()
          .query("n_markets >= 3").sort_values("n_markets", ascending=False))
print(rounds.to_string(index=False) if len(rounds) else "None")

# ---------------------------------------------------------------------------
# 2 and 3. Rankings under different settings
# ---------------------------------------------------------------------------
settings = [
    # label,             spike factor, start,        min_returns
    ("no_spike_removal", None,         "2020-01-01", 24),
    ("spikes_2x",        2.0,          "2020-01-01", 24),
    ("spikes_3x",        3.0,          "2020-01-01", 24),   # current pipeline
    ("spikes_5x",        5.0,          "2020-01-01", 24),
    ("3x_from_2022",     3.0,          "2022-01-01", 24),
    ("3x_min12",         3.0,          "2020-01-01", 12),
]

medians, ranks, spike_counts = {}, {}, {}
for label, factor, start, min_returns in settings:
    df = silver if factor is None else flag_price_spikes(silver, factor=factor)
    national = build_volatility_national(
        build_volatility_by_market(df, "price_observed", start, min_returns)
    ).toPandas().set_index("commodity")
    medians[label] = national["median_volatility"].round(3)
    ranks[label] = national["median_volatility"].rank(ascending=False).astype("Int64")
    spike_counts[label] = 0 if factor is None else df.filter("price_is_spike").count()

print("\n=== 2. SPIKES FLAGGED PER SETTING ===")
print(pd.Series(spike_counts).to_string())

print("\n=== 3a. MEDIAN VOLATILITY BY COMMODITY, PER SETTING ===")
med = pd.DataFrame(medians)
print(med.sort_values("spikes_3x", ascending=False).to_string())

print("\n=== 3b. RANK BY COMMODITY, PER SETTING (1 = most volatile) ===")
rk = pd.DataFrame(ranks)
print(rk.sort_values("spikes_3x").to_string())

print("\n=== VERDICT ===")
milk_ranks = rk.loc["milk"] if "milk" in rk.index else None
if milk_ranks is not None:
    print("Milk rank under each setting:", {k: int(v) for k, v in milk_ranks.items()})
    print("Milk ranks first under every setting:", bool((milk_ranks == 1).all()))

spark.stop()
