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


# --------------------------------------------------
# 5. Build commodity coverage lookup
#    (market -> which commodities it ever records a price for)
# --------------------------------------------------

coverage_exprs = [
    F.max(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(c)
    for c in commodity_columns
]

coverage_df = df.groupBy("mkt_name").agg(*coverage_exprs)

# Rename flag columns to avoid collision with price columns after join
coverage_renamed = coverage_df
for c in commodity_columns:
    coverage_renamed = coverage_renamed.withColumnRenamed(c, f"{c}_tracked")


# --------------------------------------------------
# 6. Join coverage flags onto the raw data
# --------------------------------------------------

df_joined = df.join(coverage_renamed, on="mkt_name", how="left")


# --------------------------------------------------
# 7. Convert wide commodity columns to long format,
#    dropping (market, commodity) pairs that were never tracked
# --------------------------------------------------

commodity_array = array(*[
    struct(
        col(c).alias("price"),
        lit(c).alias("commodity"),
        col(f"{c}_tracked").alias("tracked")
    )
    for c in commodity_columns
])

df_silver = (
    df_joined
    .select(
        "date",
        col("mkt_name").alias("market"),
        col("adm1_name").alias("state"),
        col("adm2_name").alias("lga"),
        "currency",
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        explode(commodity_array).alias("commodity_data")
    )
    .select(
        "date",
        "market",
        "state",
        "lga",
        "currency",
        "latitude",
        "longitude",
        col("commodity_data.commodity").alias("commodity"),
        col("commodity_data.price").alias("price"),
        col("commodity_data.tracked").alias("tracked")
    )
    .filter(col("tracked") == 1)
    .drop("tracked")
)


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