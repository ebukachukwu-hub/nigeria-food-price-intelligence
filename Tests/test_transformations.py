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


from transformations import (
    build_coverage_lookup,
    explode_to_long,
    build_price_trends,
    build_volatility_by_market,
    build_volatility_national
)
from pyspark.sql.types import DateType


def test_price_trends_groups_by_market_commodity_year(spark):
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("market", StringType(), True),
        StructField("state", StringType(), True),
        StructField("commodity", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    df_silver = spark.createDataFrame([
        Row(date=__import__("datetime").date(2024, 1, 1), market="Aba", state="Abia", commodity="beans", price=100.0),
        Row(date=__import__("datetime").date(2024, 2, 1), market="Aba", state="Abia", commodity="beans", price=200.0),
        Row(date=__import__("datetime").date(2025, 1, 1), market="Aba", state="Abia", commodity="beans", price=300.0),
    ], schema=schema)

    result = build_price_trends(df_silver).collect()
    by_year = {r["year"]: r["avg_price"] for r in result}

    assert by_year[2024] == 150.0   # avg of 100 and 200
    assert by_year[2025] == 300.0   # only one value in 2025


def test_volatility_by_market_computes_cv_correctly(spark):
    schema = StructType([
        StructField("market", StringType(), True),
        StructField("state", StringType(), True),
        StructField("commodity", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    # Prices: 100, 200 -> mean=150, sample stddev=70.71..., CV = stddev/mean * 100
    df_silver = spark.createDataFrame([
        Row(market="Aba", state="Abia", commodity="beans", price=100.0),
        Row(market="Aba", state="Abia", commodity="beans", price=200.0),
    ], schema=schema)

    result = build_volatility_by_market(df_silver).collect()[0]

    assert result["avg_price"] == 150.0
    assert round(result["stddev_price"], 2) == 70.71
    assert round(result["coefficient_of_variation"], 2) == 47.14  # 70.71 / 150 * 100


def test_volatility_national_ignores_market_dimension(spark):
    schema = StructType([
        StructField("market", StringType(), True),
        StructField("commodity", StringType(), True),
        StructField("price", DoubleType(), True),
    ])
    # Two different markets, same commodity -> should be combined into ONE row
    df_silver = spark.createDataFrame([
        Row(market="Aba", commodity="beans", price=100.0),
        Row(market="Lagos", commodity="beans", price=300.0),
    ], schema=schema)

    result = build_volatility_national(df_silver).collect()

    assert len(result) == 1              # one row for beans, regardless of market count
    assert result[0]["commodity"] == "beans"
    assert result[0]["avg_price"] == 200.0  # avg of 100 and 300, across both markets