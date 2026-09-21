import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
sys.path.append("../src")
from transformations import build_coverage_lookup, explode_to_long


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("UnitTests")
        .master("local[1]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_coverage_lookup_flags_tracked_commodity(spark):
    schema = StructType([
        StructField("mkt_name", StringType(), True),
        StructField("beans", DoubleType(), True),
        StructField("rice", DoubleType(), True),
    ])
    df = spark.createDataFrame([
        Row(mkt_name="MarketA", beans=100.0, rice=None),
        Row(mkt_name="MarketA", beans=None, rice=None),
    ], schema=schema)

    coverage = build_coverage_lookup(df, ["beans", "rice"])
    row = coverage.collect()[0]

    assert row["beans_tracked"] == 1
    assert row["rice_tracked"] == 0


def test_coverage_lookup_flags_fully_untracked_commodity(spark):
    schema = StructType([
        StructField("mkt_name", StringType(), True),
        StructField("beans", DoubleType(), True),
        StructField("rice", DoubleType(), True),
    ])
    df = spark.createDataFrame([
        Row(mkt_name="MarketB", beans=None, rice=None),
        Row(mkt_name="MarketB", beans=None, rice=None),
    ], schema=schema)

    coverage = build_coverage_lookup(df, ["beans", "rice"])
    row = coverage.collect()[0]

    assert row["beans_tracked"] == 0
    assert row["rice_tracked"] == 0


def test_explode_drops_never_tracked_commodity(spark):
    schema = StructType([
        StructField("mkt_name", StringType(), True),
        StructField("adm1_name", StringType(), True),
        StructField("adm2_name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("date", StringType(), True),
        StructField("beans", DoubleType(), True),
        StructField("rice", DoubleType(), True),
    ])
    df = spark.createDataFrame([
        Row(mkt_name="MarketA", adm1_name="StateX", adm2_name="LgaX",
            lat=1.0, lon=1.0, currency="NGN", date="2024-01-01",
            beans=100.0, rice=None)
    ], schema=schema)

    coverage = build_coverage_lookup(df, ["beans", "rice"])
    df_joined = df.join(coverage, on="mkt_name", how="left")
    df_silver = explode_to_long(df_joined, ["beans", "rice"])

    commodities = [r["commodity"] for r in df_silver.collect()]
    assert "beans" in commodities
    assert "rice" not in commodities


def test_explode_preserves_genuine_null_for_tracked_commodity(spark):
    schema = StructType([
        StructField("mkt_name", StringType(), True),
        StructField("adm1_name", StringType(), True),
        StructField("adm2_name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("date", StringType(), True),
        StructField("beans", DoubleType(), True),
        StructField("rice", DoubleType(), True),
    ])
    df = spark.createDataFrame([
        Row(mkt_name="MarketA", adm1_name="StateX", adm2_name="LgaX",
            lat=1.0, lon=1.0, currency="NGN", date="2024-01-01",
            beans=100.0, rice=None),
        Row(mkt_name="MarketA", adm1_name="StateX", adm2_name="LgaX",
            lat=1.0, lon=1.0, currency="NGN", date="2024-02-01",
            beans=None, rice=None),
    ], schema=schema)

    coverage = build_coverage_lookup(df, ["beans", "rice"])
    df_joined = df.join(coverage, on="mkt_name", how="left")
    df_silver = explode_to_long(df_joined, ["beans", "rice"])

    beans_rows = df_silver.filter(df_silver.commodity == "beans").collect()
    prices = [r["price"] for r in beans_rows]

    assert len(beans_rows) == 2
    assert 100.0 in prices
    assert None in prices