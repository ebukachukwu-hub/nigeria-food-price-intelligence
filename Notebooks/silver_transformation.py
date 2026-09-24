"""Bronze (raw CSV) -> Silver (validated, long format, written to Parquet)."""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402
from local_io import clear_local_output  # noqa: E402
from transformations import (  # noqa: E402
    COMMODITIES, read_raw, cast_raw, add_date, build_silver,
    count_cast_failures, count_date_mismatches, count_duplicate_keys,
    distinct_currencies, count_nonpositive_prices,
)

RAW_PATH = ROOT / "data" / "raw" / "NGA_RTFP_mkt_2007_2026-08-24.csv"
SILVER_PATH = ROOT / "data" / "silver" / "food_prices"

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Silver")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# 1. Read and type
raw = read_raw(spark, str(RAW_PATH))
typed = add_date(cast_raw(raw))

# 2. Validate the typed raw data
cast_cols = ["lat", "lon", "year", "month"] + [
    col for c in COMMODITIES for col in (c, f"c_{c}", f"trust_{c}")
]
failures = count_cast_failures(raw, typed, cast_cols)
if failures:
    print("WARNING: text values lost when casting to numbers:", failures)

mismatches = count_date_mismatches(typed)
assert mismatches == 0, f"{mismatches} rows where year/month disagree with DATES"

# 3. Build Silver
silver = build_silver(typed).cache()

# 4. Validate Silver
dupes = count_duplicate_keys(silver)
assert dupes == 0, f"{dupes} duplicate keys at grain (geo_id, commodity, date)"

currencies = distinct_currencies(silver)
assert currencies == ["NGN"], f"Unexpected currencies: {currencies}"

nonpositive = count_nonpositive_prices(silver)
if nonpositive:
    print(f"WARNING: {nonpositive} rows with zero or negative prices")

# 5. Report
n_rows = silver.count()
n_observed = silver.filter("price_observed IS NOT NULL").count()
print(f"Silver rows: {n_rows}")
print(f"Rows with an observed price: {n_observed} ({n_observed / n_rows:.1%})")
n_spikes = silver.filter("price_is_spike").count()
print(f"Observed prices flagged as one-month spikes: {n_spikes}")
silver.filter("price_is_spike").select(
    "market", "state", "commodity", "date", "price_observed", "price_estimated"
).orderBy("state", "market", "commodity", "date").show(50, truncate=False)
silver.show(10)

# 6. Persist
clear_local_output(SILVER_PATH)
silver.write.mode("overwrite").partitionBy("commodity").parquet(str(SILVER_PATH))
print(f"Silver written to {SILVER_PATH}")

spark.stop()
