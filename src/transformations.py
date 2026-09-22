from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, array, struct, lit, explode


def build_coverage_lookup(df: DataFrame, commodity_columns: list, market_col: str = "mkt_name") -> DataFrame:
    """
    For each market, flag whether it has EVER recorded a non-null price
    for each commodity, across all rows. Returns one row per market with
    a '<commodity>_tracked' column (1 = tracked, 0 = never tracked).
    """
    coverage_exprs = [
        F.max(F.when(F.col(c).isNotNull(), 1).otherwise(0)).alias(c)
        for c in commodity_columns
    ]
    coverage_df = df.groupBy(market_col).agg(*coverage_exprs)

    for c in commodity_columns:
        coverage_df = coverage_df.withColumnRenamed(c, f"{c}_tracked")

    return coverage_df


def explode_to_long(df_joined: DataFrame, commodity_columns: list) -> DataFrame:
    """
    Converts wide commodity columns into long format, dropping any
    (market, commodity) pair the market never tracks. df_joined must
    already have '<commodity>_tracked' flag columns joined on.
    """
    commodity_array = array(*[
        struct(
            col(c).alias("price"),
            lit(c).alias("commodity"),
            col(f"{c}_tracked").alias("tracked")
        )
        for c in commodity_columns
    ])

    return (
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
            "date", "market", "state", "lga", "currency", "latitude", "longitude",
            col("commodity_data.commodity").alias("commodity"),
            col("commodity_data.price").alias("price"),
            col("commodity_data.tracked").alias("tracked")
        )
        .filter(col("tracked") == 1)
        .drop("tracked")
    )

from pyspark.sql import functions as F


def build_price_trends(df_silver):
    """
    Grain: (market, commodity, year)
    Answers: which markets/states have higher prices; how prices change over time.
    """
    return (
        df_silver
        .withColumn("year", F.year("date"))
        .groupBy("market", "state", "commodity", "year")
        .agg(
            F.avg("price").alias("avg_price")
        )
        .orderBy("market", "commodity", "year")
    )


def build_volatility_by_market(df_silver):
    """
    Grain: (market, commodity)
    Answers: which market-commodity pairs are most volatile.
    """
    return (
        df_silver
        .groupBy("market", "state", "commodity")
        .agg(
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("stddev_price")
        )
        .withColumn(
            "coefficient_of_variation",
            F.round((F.col("stddev_price") / F.col("avg_price")) * 100, 2)
        )
        .orderBy(F.col("coefficient_of_variation").desc())
    )


def build_volatility_national(df_silver):
    """
    Grain: (commodity)
    Answers: which commodity is most volatile nationally, across all markets.
    """
    return (
        df_silver
        .groupBy("commodity")
        .agg(
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("stddev_price")
        )
        .withColumn(
            "coefficient_of_variation",
            F.round((F.col("stddev_price") / F.col("avg_price")) * 100, 2)
        )
        .orderBy(F.col("coefficient_of_variation").desc())
    )