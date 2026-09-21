from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, make_date, explode, array, struct, lower, trim, lit)
from pyspark.sql import functions as F
import os
import sys


# --------------------------------------------------
# 1. Start Spark
# --------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price Silver Transformation")
    .master("local[*]")
    .getOrCreate()
)

print("Spark session started.")


# --------------------------------------------------
# 2. Read raw CSV
# --------------------------------------------------

input_path = "../data/raw/NGA_RTFP_mkt_2007_2026-08-24.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("\nRaw Data:")
df.show(5)

print("\nRaw Schema:")
df.printSchema()


# --------------------------------------------------
# 3. Create proper date column
# --------------------------------------------------

df = df.withColumn(
    "date",
    make_date(
        col("year"),
        col("month"),
        lit(1)
    )
)


# --------------------------------------------------
# 4. Define commodity columns
# --------------------------------------------------

commodity_columns = [
    "beans",
    "eggs",
    "fish",
    "gari_fao",
    "groundnuts",
    "maize_fao",
    "maize_flour",
    "meat_beef",
    "meat_goat",
    "milk",
    "millet",
    "onions",
    "rice",
    "rice_fao",
    "sorghum_fao",
    "yam"
]


import sys
sys.path.append("../src")
from transformations import build_coverage_lookup, explode_to_long

# --------------------------------------------------
# 5. Build commodity coverage lookup
# --------------------------------------------------

coverage_df = build_coverage_lookup(df, commodity_columns)

# --------------------------------------------------
# 6. Join coverage flags onto the raw data
# --------------------------------------------------

df_joined = df.join(coverage_df, on="mkt_name", how="left")

# --------------------------------------------------
# 7. Convert wide commodity columns to long format
# --------------------------------------------------

df_silver = explode_to_long(df_joined, commodity_columns)

# --------------------------------------------------
# 8. Clean commodity names
# --------------------------------------------------

df_silver = df_silver.withColumn(
    "commodity",
    lower(trim(col("commodity")))
)


# --------------------------------------------------
# 9. Display transformed data
# --------------------------------------------------

print("\nSilver Data:")
df_silver.show(20)

print("\nSilver Schema:")
df_silver.printSchema()

print("\nSilver Row Count:")
print(df_silver.count())


# --------------------------------------------------
# 10. Stop Spark
# --------------------------------------------------

spark.stop()