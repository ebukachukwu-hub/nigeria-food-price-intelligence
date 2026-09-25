# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: raw CSV to Delta
# MAGIC Reads the World Bank CSV from a Unity Catalog volume and stores it unchanged
# MAGIC as a Delta table. Every column stays as text; types are set in Silver.
# MAGIC Two ingestion columns are added so every row records where and when it came from:
# MAGIC `_source_file` (the file name carries the World Bank version date) and `_ingested_at`.

# COMMAND ----------

CATALOG = "workspace"      # check the name in Catalog Explorer
SCHEMA = "nigeria_food"
RAW_FILE = f"/Volumes/{CATALOG}/{SCHEMA}/raw/NGA_RTFP_mkt_2007_2026-08-24.csv"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_food_prices"

# COMMAND ----------

from pyspark.sql import functions as F

bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(RAW_FILE)
    .withColumn("_source_file", F.col("_metadata.file_name"))
    .withColumn("_ingested_at", F.current_timestamp())
)

print(f"Rows: {bronze.count()}, columns: {len(bronze.columns)}")

# COMMAND ----------

(
    bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
print(f"Written {BRONZE_TABLE}")
