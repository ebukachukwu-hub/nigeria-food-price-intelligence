"""
Reusable Bronze -> Silver -> Gold transformations for the World Bank
Real Time Food Prices (RTFP) Nigeria dataset.

Design notes
------------
* Markets are keyed on `geo_id`, not `mkt_name`. Names are labels and are
  not guaranteed unique; geo_id is the source's market identifier.
* Silver keeps BOTH price series side by side:
    - price_observed  : the unprefixed column (e.g. `beans`)
    - price_estimated : the World Bank model's close estimate (`c_beans`)
    - trust           : the World Bank trust score (`trust_beans`)
  Gold picks which one to analyse via `price_col`, so the choice is explicit.
* Coverage is bounded by each (market, commodity) pair's first and last
  observed date. Months before a market started reporting are not rows.
* Volatility is the standard deviation of month-on-month log returns over a
  common window, not the coefficient of variation of price levels. CV of
  levels mostly measures the inflation trend.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

COMMODITIES = [
    "beans", "eggs", "fish", "gari_fao", "groundnuts", "maize_fao",
    "maize_flour", "meat_beef", "meat_goat", "milk", "millet",
    "onions", "rice", "rice_fao", "sorghum_fao", "yam",
]

SILVER_KEY = ["geo_id", "commodity", "date"]


# --------------------------------------------------------------------------
# Bronze
# --------------------------------------------------------------------------

def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the raw CSV with every column as string. Types are set
    explicitly in `cast_raw`, so a changed source file cannot silently
    change our schema through inference."""
    return spark.read.option("header", True).option("inferSchema", False).csv(path)


def _try_cast(column: str, dtype: str):
    """Cast that returns NULL on malformed input instead of failing the job
    (Spark 4 runs in ANSI mode, where plain cast raises). Losses are then
    measured by `count_cast_failures`, so nothing disappears silently."""
    return F.expr(f"try_cast(`{column}` AS {dtype})")


def cast_raw(df: DataFrame, commodities: list = COMMODITIES) -> DataFrame:
    """Cast only the columns the pipeline uses."""
    numeric = ["lat", "lon"]
    for c in commodities:
        numeric += [c, f"c_{c}", f"trust_{c}"]
    out = df
    for c in numeric:
        out = out.withColumn(c, _try_cast(c, "DOUBLE"))
    for c in ["year", "month", "spatially_interpolated"]:
        out = out.withColumn(c, _try_cast(c, "INT"))
    return out


def count_cast_failures(raw: DataFrame, casted: DataFrame, columns: list) -> dict:
    """Values that were non-empty text but became NULL after casting.
    Anything above zero means the source contains values we are discarding."""
    failures = {}
    raw_counts = raw.select([F.count(F.col(c)).alias(c) for c in columns]).first()
    cast_counts = casted.select([F.count(F.col(c)).alias(c) for c in columns]).first()
    for c in columns:
        lost = raw_counts[c] - cast_counts[c]
        if lost:
            failures[c] = lost
    return failures


def add_date(df: DataFrame) -> DataFrame:
    return df.withColumn("date", F.make_date(F.col("year"), F.col("month"), F.lit(1)))


def count_date_mismatches(df: DataFrame, source_col: str = "DATES") -> int:
    """Rows where our constructed date disagrees with the source's own date."""
    if source_col not in df.columns:
        return 0
    return df.filter(F.to_date(F.col(source_col)) != F.col("date")).count()


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------

def explode_to_long(df: DataFrame, commodities: list = COMMODITIES) -> DataFrame:
    """Wide -> long. One row per (market, commodity, month), carrying the
    observed price, the model estimate and the trust score together."""
    items = F.array(*[
        F.struct(
            F.lit(c).alias("commodity"),
            F.col(c).alias("price_observed"),
            F.col(f"c_{c}").alias("price_estimated"),
            F.col(f"trust_{c}").alias("trust"),
        )
        for c in commodities
    ])
    return (
        df.select(
            "date", "geo_id",
            F.col("mkt_name").alias("market"),
            F.col("adm1_name").alias("state"),
            F.col("adm2_name").alias("lga"),
            F.col("lat").alias("latitude"),
            F.col("lon").alias("longitude"),
            "currency", "spatially_interpolated",
            F.explode(items).alias("item"),
        )
        .select("*", "item.*")
        .drop("item")
    )


def build_coverage(df_long: DataFrame) -> DataFrame:
    """First and last month each (market, commodity) pair has an observed
    price. Pairs that never report anything do not appear."""
    return (
        df_long
        .filter(F.col("price_observed").isNotNull())
        .groupBy("geo_id", "commodity")
        .agg(
            F.min("date").alias("first_observed"),
            F.max("date").alias("last_observed"),
        )
    )


def apply_coverage(df_long: DataFrame) -> DataFrame:
    """Keep only rows inside each pair's observed window. NULLs inside the
    window are genuine gaps and are preserved."""
    coverage = build_coverage(df_long)
    return (
        df_long
        .join(coverage, on=["geo_id", "commodity"], how="inner")
        .filter(F.col("date").between(F.col("first_observed"), F.col("last_observed")))
        .drop("first_observed", "last_observed")
    )


def flag_price_spikes(df_long: DataFrame, factor: float = 3.0,
                      max_gap_months: int = 2) -> DataFrame:
    """Flag isolated one-month spikes in price_observed: a price at least
    `factor` times higher (or lower) than BOTH its previous and next observed
    prices, with those neighbours no more than `max_gap_months` away.

    A genuine step change (price jumps and stays) is not flagged, because the
    next price agrees with the new level. Flagged rows are kept in Silver with
    price_is_spike = True; Gold decides whether to exclude them."""
    key = Window.partitionBy("geo_id", "commodity").orderBy("date")
    before = key.rowsBetween(Window.unboundedPreceding, -1)
    after = key.rowsBetween(1, Window.unboundedFollowing)
    obs = F.col("price_observed")
    obs_date = F.when(obs.isNotNull(), F.col("date"))

    prev_p = F.last(obs, ignorenulls=True).over(before)
    prev_d = F.last(obs_date, ignorenulls=True).over(before)
    next_p = F.first(obs, ignorenulls=True).over(after)
    next_d = F.first(obs_date, ignorenulls=True).over(after)

    close = ((F.months_between(F.col("date"), prev_d) <= max_gap_months)
             & (F.months_between(next_d, F.col("date")) <= max_gap_months))
    up = (obs >= factor * prev_p) & (obs >= factor * next_p)
    down = (obs * factor <= prev_p) & (obs * factor <= next_p)

    return df_long.withColumn(
        "price_is_spike",
        F.coalesce(obs.isNotNull() & prev_p.isNotNull() & next_p.isNotNull()
                   & close & (up | down), F.lit(False)),
    )


def build_silver(df_raw_typed: DataFrame, commodities: list = COMMODITIES) -> DataFrame:
    """Typed raw data (after cast_raw and add_date) -> Silver."""
    return flag_price_spikes(apply_coverage(explode_to_long(df_raw_typed, commodities)))


# --------------------------------------------------------------------------
# Silver validation
# --------------------------------------------------------------------------

def count_duplicate_keys(df_silver: DataFrame, key: list = SILVER_KEY) -> int:
    return df_silver.groupBy(*key).count().filter(F.col("count") > 1).count()


def distinct_currencies(df_silver: DataFrame) -> list:
    return [r["currency"] for r in df_silver.select("currency").distinct().collect()]


def count_nonpositive_prices(df_silver: DataFrame) -> int:
    return df_silver.filter(
        (F.col("price_observed") <= 0) | (F.col("price_estimated") <= 0)
    ).count()


# --------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------

def build_price_trends(df_silver: DataFrame, price_col: str = "price_observed") -> DataFrame:
    """Grain: (market, commodity, year). n_months shows how many months the
    average rests on, so years with 1 month and 12 months are not confused."""
    return (
        df_silver
        .withColumn("year", F.year("date"))
        .groupBy("geo_id", "market", "state", "commodity", "year")
        .agg(
            F.avg(price_col).alias("avg_price"),
            F.count(price_col).alias("n_months"),
        )
    )


def build_monthly_returns(df_silver: DataFrame, price_col: str = "price_observed",
                          start: str = "2020-01-01") -> DataFrame:
    """Month-on-month log returns, only between consecutive calendar months.
    Rows flagged as spikes in Silver are excluded when the flag exists; the
    returns either side of a spike then disappear because the months are no
    longer consecutive."""
    w = Window.partitionBy("geo_id", "commodity").orderBy("date")
    df = df_silver
    if "price_is_spike" in df.columns:
        df = df.filter(~F.col("price_is_spike"))
    return (
        df
        .filter((F.col("date") >= F.lit(start).cast("date")) & (F.col(price_col) > 0))
        .withColumn("prev_price", F.lag(price_col).over(w))
        .withColumn("prev_date", F.lag("date").over(w))
        .filter(F.months_between("date", "prev_date") == 1)
        .withColumn("log_return", F.log(F.col(price_col) / F.col("prev_price")))
    )


def build_volatility_by_market(df_silver: DataFrame, price_col: str = "price_observed",
                               start: str = "2020-01-01", min_returns: int = 24) -> DataFrame:
    """Grain: (market, commodity). Volatility = stddev of monthly log returns
    over a common window. Pairs with fewer than `min_returns` consecutive-month
    returns are dropped rather than reported on thin evidence."""
    returns = build_monthly_returns(df_silver, price_col, start)
    result = (
        returns
        .groupBy("geo_id", "market", "state", "commodity")
        .agg(
            F.count("log_return").alias("n_returns"),
            F.stddev("log_return").alias("volatility"),
            F.max("spatially_interpolated").alias("spatially_interpolated"),
            F.avg("trust").alias("avg_trust"),
        )
        .filter(F.col("n_returns") >= min_returns)
    )
    if "price_is_spike" in df_silver.columns:
        spikes = (
            df_silver
            .filter(F.col("date") >= F.lit(start).cast("date"))
            .groupBy("geo_id", "commodity")
            .agg(F.sum(F.col("price_is_spike").cast("int")).alias("n_spikes_excluded"))
        )
        result = result.join(spikes, on=["geo_id", "commodity"], how="left")
    return result


def build_volatility_national(df_volatility_by_market: DataFrame) -> DataFrame:
    """Grain: (commodity). The typical market's volatility (median) and the
    number of markets behind it. Pooling raw prices across markets would mix
    in price differences BETWEEN markets, which is not volatility."""
    return (
        df_volatility_by_market
        .groupBy("commodity")
        .agg(
            F.percentile_approx("volatility", 0.5).alias("median_volatility"),
            F.count("*").alias("n_markets"),
        )
    )
