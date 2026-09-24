import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, IntegerType, LongType,
)

from transformations import (
    cast_raw, add_date, count_cast_failures, count_date_mismatches,
    explode_to_long, build_silver, count_duplicate_keys, flag_price_spikes,
    build_price_trends, build_volatility_by_market, build_volatility_national,
)

COMMS = ["beans", "rice"]

RAW_FIELDS = [
    "geo_id", "mkt_name", "adm1_name", "adm2_name", "lat", "lon", "currency",
    "spatially_interpolated", "year", "month", "DATES",
    "beans", "c_beans", "trust_beans", "rice", "c_rice", "trust_rice",
]
RAW_SCHEMA = StructType([StructField(f, StringType(), True) for f in RAW_FIELDS])

SILVER_SCHEMA = StructType([
    StructField("date", DateType()), StructField("geo_id", StringType()),
    StructField("market", StringType()), StructField("state", StringType()),
    StructField("commodity", StringType()), StructField("price_observed", DoubleType()),
    StructField("price_estimated", DoubleType()), StructField("trust", DoubleType()),
    StructField("spatially_interpolated", IntegerType()),
])

VOL_SCHEMA = StructType([
    StructField("geo_id", StringType()), StructField("market", StringType()),
    StructField("state", StringType()), StructField("commodity", StringType()),
    StructField("n_returns", LongType()), StructField("volatility", DoubleType()),
    StructField("spatially_interpolated", IntegerType()), StructField("avg_trust", DoubleType()),
])


def raw_df(spark, rows):
    return spark.createDataFrame([[r[f] for f in RAW_FIELDS] for r in rows], RAW_SCHEMA)


def silver_df(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def month(i):
    """i-th month counting from Jan 2020."""
    return 2020 + i // 12, i % 12 + 1


def raw_row(geo_id, mkt, i, beans=None, rice=None, state="StateX",
            c_beans=None, c_rice=None, trust=None, currency="NGN"):
    """One wide raw row with every value as a string, like the CSV."""
    y, m = month(i)
    s = lambda v: None if v is None else str(v)  # noqa: E731
    return Row(
        geo_id=geo_id, mkt_name=mkt, adm1_name=state, adm2_name="LgaX",
        lat="1.0", lon="1.0", currency=currency, spatially_interpolated="0",
        year=str(y), month=str(m), DATES=f"{y}-{m:02d}-01",
        beans=s(beans), c_beans=s(c_beans), trust_beans=s(trust),
        rice=s(rice), c_rice=s(c_rice), trust_rice=s(trust),
    )


def typed(spark, rows):
    return add_date(cast_raw(raw_df(spark, rows), COMMS))


def silver_rows(geo_id, market, prices, state="StateX", commodity="beans",
                start_index=0, trust=None):
    """Silver-shaped rows for Gold tests. None in `prices` = missing month."""
    out = []
    for k, p in enumerate(prices):
        y, m = month(start_index + k)
        out.append(Row(
            date=dt.date(y, m, 1), geo_id=geo_id, market=market, state=state,
            commodity=commodity, price_observed=None if p is None else float(p),
            price_estimated=None, trust=trust, spatially_interpolated=0,
        ))
    return out


# --------------------------------------------------------------------------
# Bronze typing
# --------------------------------------------------------------------------

def test_cast_failure_is_counted_not_hidden(spark):
    raw = raw_df(spark, [raw_row("g1", "A", 0, beans="n/a")])
    failures = count_cast_failures(raw, cast_raw(raw, COMMS), ["beans"])
    assert failures == {"beans": 1}


def test_constructed_date_matches_source_date(spark):
    assert count_date_mismatches(typed(spark, [raw_row("g1", "A", 5, beans=1)])) == 0


# --------------------------------------------------------------------------
# Silver
# --------------------------------------------------------------------------

def test_explode_carries_observed_estimated_and_trust(spark):
    df = typed(spark, [raw_row("g1", "A", 0, beans=100, c_beans=105, trust=8)])
    row = explode_to_long(df, COMMS).filter("commodity = 'beans'").first()
    assert (row.price_observed, row.price_estimated, row.trust) == (100.0, 105.0, 8.0)


def test_never_tracked_commodity_is_dropped(spark):
    df = typed(spark, [raw_row("g1", "A", 0, beans=100)])
    commodities = {r.commodity for r in build_silver(df, COMMS).collect()}
    assert commodities == {"beans"}


def test_months_before_first_observation_are_dropped(spark):
    # Market reports beans only from month 3 onwards. Months 0-2 must not
    # become NULL rows: they were never part of this market's series.
    rows = [raw_row("g1", "A", i, beans=None) for i in range(3)]
    rows += [raw_row("g1", "A", i, beans=100 + i) for i in range(3, 6)]
    silver = build_silver(typed(spark, rows), COMMS).filter("commodity = 'beans'")
    assert silver.count() == 3
    assert silver.filter("price_observed IS NULL").count() == 0


def test_gap_inside_observed_window_is_preserved_as_null(spark):
    rows = [
        raw_row("g1", "A", 0, beans=100),
        raw_row("g1", "A", 1, beans=None),
        raw_row("g1", "A", 2, beans=120),
    ]
    silver = build_silver(typed(spark, rows), COMMS).filter("commodity = 'beans'")
    assert silver.count() == 3
    assert silver.filter("price_observed IS NULL").count() == 1


def test_markets_sharing_a_name_are_not_merged(spark):
    # Same market name in two states. Keying on mkt_name would mix them.
    rows = [
        raw_row("g1", "Central Market", 0, beans=100, state="Oyo"),
        raw_row("g2", "Central Market", 0, beans=900, rice=50, state="Kano"),
    ]
    silver = build_silver(typed(spark, rows), COMMS)
    assert count_duplicate_keys(silver) == 0
    # g1 never tracked rice, so no rice row may appear for it
    assert silver.filter("geo_id = 'g1' AND commodity = 'rice'").count() == 0
    assert silver.filter("commodity = 'beans'").count() == 2


# --------------------------------------------------------------------------
# Gold
# --------------------------------------------------------------------------

def test_price_trends_reports_months_behind_each_average(spark):
    df = silver_df(spark, silver_rows("g1", "A", [100, 200] + [None] * 10 + [300]))
    by_year = {r.year: r for r in build_price_trends(df).collect()}
    assert (by_year[2020].avg_price, by_year[2020].n_months) == (150.0, 2)
    assert (by_year[2021].avg_price, by_year[2021].n_months) == (300.0, 1)


def test_steady_growth_has_zero_volatility(spark):
    # 10% a month, every month. No ups and downs, so volatility must be zero.
    # The old CV-of-levels metric scored this series above 100%.
    prices = [100 * 1.1 ** i for i in range(30)]
    df = silver_df(spark, silver_rows("g1", "A", prices))
    result = build_volatility_by_market(df, min_returns=24).first()
    assert result.volatility < 1e-9


def test_volatility_is_higher_for_a_series_that_swings(spark):
    steady = [100 * 1.02 ** i for i in range(30)]
    swinging = [100 if i % 2 == 0 else 130 for i in range(30)]
    df = silver_df(spark, silver_rows("g1", "Steady", steady) + silver_rows("g2", "Swing", swinging))
    vol = {r.market: r.volatility for r in build_volatility_by_market(df).collect()}
    assert vol["Swing"] > vol["Steady"]


def test_returns_are_not_computed_across_gaps(spark):
    # 25 months, then a gap, then 5 months. The jump across the gap must not
    # count as a return: 24 + 4 = 28 consecutive-month returns.
    prices = [100.0] * 25 + [None] + [500.0] * 5
    df = silver_df(spark, silver_rows("g1", "A", prices))
    result = build_volatility_by_market(df, min_returns=24).first()
    assert result.n_returns == 28
    assert result.volatility == pytest.approx(0.0)


def test_thin_series_are_excluded(spark):
    df = silver_df(spark, silver_rows("g1", "A", [100, 150, 90, 200]))
    assert build_volatility_by_market(df, min_returns=24).count() == 0


def test_series_before_window_start_are_ignored(spark):
    # 30 months of data, but all before the 2025 window start.
    df = silver_df(spark, silver_rows("g1", "A", [100 + (i % 3) for i in range(30)]))
    assert build_volatility_by_market(df, start="2025-01-01", min_returns=2).count() == 0


def test_national_volatility_is_median_of_markets(spark):
    by_market = spark.createDataFrame([
        Row(geo_id=g, market=g, state="S", commodity="beans", n_returns=30,
            volatility=v, spatially_interpolated=0, avg_trust=None)
        for g, v in [("g1", 0.1), ("g2", 0.2), ("g3", 0.9)]
    ], VOL_SCHEMA)
    result = build_volatility_national(by_market).first()
    assert result.median_volatility == pytest.approx(0.2)
    assert result.n_markets == 3


# --------------------------------------------------------------------------
# Spike handling
# --------------------------------------------------------------------------

def spike_flags(spark, prices):
    df = flag_price_spikes(silver_df(spark, silver_rows("g1", "A", prices)))
    return [r.price_is_spike for r in df.orderBy("date").collect()]


def test_isolated_spike_is_flagged(spark):
    # The Monguno milk pattern: ~4000, one month at 225, back to ~4000.
    assert spike_flags(spark, [4233.33, 225.0, 4000.0]) == [False, True, False]


def test_upward_spike_is_flagged(spark):
    assert spike_flags(spark, [100.0, 900.0, 110.0]) == [False, True, False]


def test_genuine_step_change_is_not_flagged(spark):
    # Price triples and STAYS: a real level shift, not an error.
    assert not any(spike_flags(spark, [100.0, 300.0, 310.0, 320.0]))


def test_spike_detection_skips_one_missing_month(spark):
    assert spike_flags(spark, [4000.0, None, 225.0, 4000.0])[2] is True


def test_spike_excluded_from_volatility(spark):
    prices = [4000.0] * 15 + [225.0] + [4000.0] * 15
    df = flag_price_spikes(silver_df(spark, silver_rows("g1", "A", prices)))
    result = build_volatility_by_market(df, min_returns=24).first()
    assert result.volatility == pytest.approx(0.0)
    assert result.n_spikes_excluded == 1
    assert result.n_returns == 28   # the two returns touching the spike are gone
