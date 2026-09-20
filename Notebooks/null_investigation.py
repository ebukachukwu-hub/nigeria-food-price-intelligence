from pyspark.sql import SparkSession
from pyspark.sql.functions import col, make_date, explode, array, struct, lower, trim, lit
from pyspark.sql import functions as F
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("Nigeria Food Price NULL Investigation")
    .master("local[*]")
    .getOrCreate()
)

# --------------------------------------------------
# Rebuild df_silver (same logic as silver_transformation.py)
# --------------------------------------------------

input_path = "../data/raw/NGA_RTFP_mkt_2007_2026-08-24.csv"

df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
df = df.withColumn("date", make_date(col("year"), col("month"), lit(1)))

commodity_columns = [
    "beans", "eggs", "fish", "gari_fao", "groundnuts", "maize_fao",
    "maize_flour", "meat_beef", "meat_goat", "milk", "millet",
    "onions", "rice", "rice_fao", "sorghum_fao", "yam"
]

coverage_exprs = [
    F.max(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(c)
    for c in commodity_columns
]
coverage_df = df.groupBy("mkt_name").agg(*coverage_exprs)

coverage_renamed = coverage_df
for c in commodity_columns:
    coverage_renamed = coverage_renamed.withColumnRenamed(c, f"{c}_tracked")

df_joined = df.join(coverage_renamed, on="mkt_name", how="left")

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
        "date", col("mkt_name").alias("market"), col("adm1_name").alias("state"),
        col("adm2_name").alias("lga"), "currency", col("lat").alias("latitude"),
        col("lon").alias("longitude"), explode(commodity_array).alias("commodity_data")
    )
    .select(
        "date", "market", "state", "lga", "currency", "latitude", "longitude",
        col("commodity_data.commodity").alias("commodity"),
        col("commodity_data.price").alias("price"),
        col("commodity_data.tracked").alias("tracked")
    )
    .filter(col("tracked") == 1)
    .drop("tracked")
)

df_silver = df_silver.withColumn("commodity", lower(trim(col("commodity"))))

# --------------------------------------------------
# NULL investigation
# --------------------------------------------------

total_rows = df_silver.count()
null_rows = df_silver.filter(F.col("price").isNull()).count()

print("\nTotal rows:", total_rows)
print("Rows with NULL price:", null_rows)
print("Percentage NULL: {:.2f}%".format((null_rows / total_rows) * 100))

print("\nNULLs by commodity:")
df_silver.groupBy("commodity").agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_count")
).withColumn("null_pct", F.round((F.col("null_count") / F.col("total_rows")) * 100, 2)) \
 .orderBy(F.col("null_pct").desc()).show(20, truncate=False)

print("\nNULLs by state:")
df_silver.groupBy("state").agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_count")
).withColumn("null_pct", F.round((F.col("null_count") / F.col("total_rows")) * 100, 2)) \
 .orderBy(F.col("null_pct").desc()).show(50, truncate=False)

print("\nNULLs by year:")
df_silver.withColumn("year", F.year("date")).groupBy("year").agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("null_count")
).withColumn("null_pct", F.round((F.col("null_count") / F.col("total_rows")) * 100, 2)) \
 .orderBy("year").show(30, truncate=False)

spark.stop()