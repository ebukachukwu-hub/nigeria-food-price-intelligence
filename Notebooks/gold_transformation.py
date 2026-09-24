"""Silver (Parquet) -> Gold analytical tables."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from local_io import clear_local_output  # noqa: E402
from transformations import (  # noqa: E402
    build_price_trends, build_volatility_by_market, build_volatility_national,
)

SILVER_PATH = ROOT / "data" / "silver" / "food_prices"
GOLD_PATH = ROOT / "data" / "gold"

# Analysis settings. These are judgement calls: state them in the README and
# check whether the rankings change when you vary them.
PRICE_COL = "price_observed"   # or "price_estimated"
START = "2020-01-01"           # common window for every series
MIN_RETURNS = 24               # minimum consecutive-month returns per pair

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Gold")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

silver = spark.read.parquet(str(SILVER_PATH))

price_trends = build_price_trends(silver, PRICE_COL)
vol_market = build_volatility_by_market(silver, PRICE_COL, START, MIN_RETURNS).cache()
vol_national = build_volatility_national(vol_market)

print(f"\nSettings: price={PRICE_COL}, start={START}, min_returns={MIN_RETURNS}")

print("\nVolatility by market (top 20)")
vol_market.orderBy(F.desc("volatility")).show(20, truncate=False)

print("\nVolatility national (median across markets)")
vol_national.orderBy(F.desc("median_volatility")).show(truncate=False)

for name, table in [("price_trends", price_trends),
                    ("volatility_by_market", vol_market),
                    ("volatility_national", vol_national)]:
    clear_local_output(GOLD_PATH / name)
    table.write.mode("overwrite").parquet(str(GOLD_PATH / name))

print(f"\nGold tables written to {GOLD_PATH}")

spark.stop()
