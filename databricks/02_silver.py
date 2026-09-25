# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: typed, long format, validated
# MAGIC Uses the same functions as the local pipeline (`src/transformations.py`),
# MAGIC so the logic tested locally is the logic that runs here.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "nigeria_food"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_food_prices"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_food_prices"

# COMMAND ----------

import os
import sys

# In a Git folder, the notebook runs from its own directory, so src/ is one level up.
sys.path.insert(0, os.path.abspath("../src"))

from transformations import (
    COMMODITIES, cast_raw, add_date, build_silver,
    count_cast_failures, count_date_mismatches, count_duplicate_keys,
    distinct_currencies, count_nonpositive_prices,
)

# COMMAND ----------

raw = spark.table(BRONZE_TABLE)
typed = add_date(cast_raw(raw))

cast_cols = ["lat", "lon", "year", "month"] + [
    col for c in COMMODITIES for col in (c, f"c_{c}", f"trust_{c}")
]
failures = count_cast_failures(raw, typed, cast_cols)
if failures:
    print("WARNING: text values lost when casting to numbers:", failures)

mismatches = count_date_mismatches(typed)
assert mismatches == 0, f"{mismatches} rows where year/month disagree with DATES"

# COMMAND ----------

silver = build_silver(typed)

dupes = count_duplicate_keys(silver)
assert dupes == 0, f"{dupes} duplicate keys at grain (geo_id, commodity, date)"

currencies = distinct_currencies(silver)
assert currencies == ["NGN"], f"Unexpected currencies: {currencies}"

nonpositive = count_nonpositive_prices(silver)
if nonpositive:
    print(f"WARNING: {nonpositive} rows with zero or negative prices")

# COMMAND ----------

# Not partitioned: Databricks advises against partitioning small Delta tables.
(
    silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

s = spark.table(SILVER_TABLE)
n_rows = s.count()
n_observed = s.filter("price_observed IS NOT NULL").count()
n_spikes = s.filter("price_is_spike").count()
print(f"Silver rows: {n_rows}")
print(f"Rows with an observed price: {n_observed} ({n_observed / n_rows:.1%})")
print(f"Observed prices flagged as one-month spikes: {n_spikes}")
