"""Northeast-only commodity ranking, tested under the same settings as
sensitivity_checks.py, plus the effect of the one unflagged December 2025
milk price (Kashuwan Shanu).

Why: milk, fish and onions are only measured in the northeast, while rice,
millet and yam also include calmer markets elsewhere. Ranking them on a
national median compares different geographies. This ranks every commodity
on northeast markets only.

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
NORTHEAST = ["Borno", "Yobe", "Adamawa"]

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Northeast Checks")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
pd.set_option("display.width", 200)

silver = spark.read.parquet(str(SILVER_PATH)).drop("price_is_spike").cache()

# ---------------------------------------------------------------------------
# 1. Northeast-only ranking under each setting
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

medians, ranks, n_markets = {}, {}, {}
for label, factor, start, min_returns in settings:
    df = silver if factor is None else flag_price_spikes(silver, factor=factor)
    by_market = build_volatility_by_market(df, "price_observed", start, min_returns)
    northeast = by_market.filter(F.col("state").isin(NORTHEAST))
    national = build_volatility_national(northeast).toPandas().set_index("commodity")
    medians[label] = national["median_volatility"].round(3)
    ranks[label] = national["median_volatility"].rank(ascending=False).astype("Int64")
    if label == "spikes_3x":
        n_markets = (northeast.groupBy("commodity").count()
                     .toPandas().set_index("commodity")["count"])

print("\n=== 1a. NORTHEAST ONLY: MEDIAN VOLATILITY PER SETTING ===")
med = pd.DataFrame(medians)
med["n_markets_3x"] = n_markets
print(med.sort_values("spikes_3x", ascending=False).to_string())

print("\n=== 1b. NORTHEAST ONLY: RANK PER SETTING (1 = most volatile) ===")
rk = pd.DataFrame(ranks)
print(rk.sort_values("spikes_3x").to_string())

top3_default = set(rk["spikes_3x"].nsmallest(3).index)
top3_every = all(set(rk[c].nsmallest(3).index) == top3_default for c in rk.columns)
print("\nTop three at default:", sorted(top3_default))
print("Same top three under every setting:", top3_every)

# ---------------------------------------------------------------------------
# 2. Kashuwan Shanu milk, with and without the unflagged December 2025 price
# ---------------------------------------------------------------------------
default = flag_price_spikes(silver, factor=3.0)
is_target = ((F.col("market") == "Kashuwan Shanu")
             & (F.col("commodity") == "milk")
             & (F.col("date") == F.lit("2025-12-01").cast("date")))
patched = default.withColumn(
    "price_is_spike", F.when(is_target, F.lit(True)).otherwise(F.col("price_is_spike"))
)
print("\nRows matched for Kashuwan Shanu, Dec 2025:", default.filter(is_target).count())

rows = []
for label, df in [("as_is", default), ("dec_2025_excluded", patched)]:
    vm = build_volatility_by_market(df, "price_observed", "2020-01-01", 24)
    ks = vm.filter((F.col("market") == "Kashuwan Shanu")
                   & (F.col("commodity") == "milk")).toPandas()
    ne_milk = vm.filter((F.col("commodity") == "milk")
                        & F.col("state").isin(NORTHEAST)).toPandas()
    rows.append({
        "version": label,
        "kashuwan_shanu_milk": None if ks.empty else round(ks["volatility"].iloc[0], 3),
        "northeast_milk_median": round(ne_milk["volatility"].median(), 3),
    })

print("\n=== 2. KASHUWAN SHANU MILK ===")
print(pd.DataFrame(rows).to_string(index=False))

spark.stop()
