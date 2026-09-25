# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: price trends and volatility

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "nigeria_food"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_food_prices"
GOLD_PREFIX = f"{CATALOG}.{SCHEMA}.gold_"

# Analysis settings: the same values as the local pipeline and the README.
PRICE_COL = "price_observed"
START = "2020-01-01"
MIN_RETURNS = 24

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from pyspark.sql import functions as F
from transformations import (
    build_price_trends, build_volatility_by_market, build_volatility_national,
)

# COMMAND ----------

silver = spark.table(SILVER_TABLE)

tables = {
    "price_trends": build_price_trends(silver, PRICE_COL),
    "volatility_by_market": build_volatility_by_market(silver, PRICE_COL, START, MIN_RETURNS),
}

for name, df in tables.items():
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_PREFIX + name)

# National volatility is built from the saved market table, not recomputed.
national = build_volatility_national(spark.table(GOLD_PREFIX + "volatility_by_market"))
national.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    GOLD_PREFIX + "volatility_national"
)

# COMMAND ----------

display(spark.table(GOLD_PREFIX + "volatility_by_market").orderBy(F.desc("volatility")).limit(20))

# COMMAND ----------

display(spark.table(GOLD_PREFIX + "volatility_national").orderBy(F.desc("median_volatility")))
