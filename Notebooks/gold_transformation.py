from pyspark.sql import SparkSession
from pyspark.sql.functions import col, make_date, explode, array, struct, lower, trim, lit
from pyspark.sql import functions as F
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

sys.path.append("../src")
from transformations import (
    build_coverage_lookup,
    explode_to_long,
    build_price_trends,
    build_volatility_by_market,
    build_volatility_national
)

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Gold Transformation")
    .master("local[*]")
    .getOrCreate()
)

# --------------------------------------------------
# Rebuild df_silver
# --------------------------------------------------

input_path = "../data/raw/NGA_RTFP_mkt_2007_2026-08-24.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
df = df.withColumn("date", make_date(col("year"), col("month"), lit(1)))

commodity_columns = [
    "beans", "eggs", "fish", "gari_fao", "groundnuts", "maize_fao",
    "maize_flour", "meat_beef", "meat_goat", "milk", "millet",
    "onions", "rice", "rice_fao", "sorghum_fao", "yam"
]

coverage_df = build_coverage_lookup(df, commodity_columns)
df_joined = df.join(coverage_df, on="mkt_name", how="left")
df_silver = explode_to_long(df_joined, commodity_columns)
df_silver = df_silver.withColumn("commodity", lower(trim(col("commodity"))))

# --------------------------------------------------
# Build Gold tables
# --------------------------------------------------

gold_price_trends = build_price_trends(df_silver)
gold_volatility_by_market = build_volatility_by_market(df_silver)
gold_volatility_national = build_volatility_national(df_silver)

print("\nGold: Price Trends (sample)")
gold_price_trends.show(20)

print("\nGold: Volatility by Market (top 20 most volatile)")
gold_volatility_by_market.show(20, truncate=False)

print("\nGold: Volatility National (all commodities, most volatile first)")
gold_volatility_national.show(20, truncate=False)

# --------------------------------------------------
# Write Gold tables to disk
# --------------------------------------------------

gold_price_trends.write.mode("overwrite").parquet("../data/gold/price_trends")
gold_volatility_by_market.write.mode("overwrite").parquet("../data/gold/volatility_by_market")
gold_volatility_national.write.mode("overwrite").parquet("../data/gold/volatility_national")

print("\nGold tables written to data/gold/")

spark.stop()