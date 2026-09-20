# Nigeria Cost-of-Living & Food Price Intelligence Pipeline

A data engineering pipeline that transforms the World Bank's Real Time Food
Prices dataset for Nigeria into an analysis-ready format, built to answer
questions like:

- Which food commodities are experiencing the largest price changes?
- Which markets/states have higher prices?
- How do prices change over time?
- Which commodities show the greatest volatility?

## Data Source

[World Bank Real Time Food Prices — Nigeria](https://microdata.worldbank.org/catalog/4503/data-api)
(`NGA_RTFP_mkt_2007_2026-08-24.csv`)

- 73 markets, monthly estimates from January 2007 to August 2026
- 17,464 market-level observations, 137 variables
- Compiled from WFP, FAO, and national statistical office sources, with
  ML-based estimation for missing prices
- Commodities tracked include beans, eggs, fish, gari, maize, meat, milk,
  millet, onions, rice, sorghum, and yam

Raw data is not committed to this repo (see `.gitignore`). To reproduce:
visit the link above, scroll down to the **Bulk Data Downloads** table,
and download the Nigeria market-level CSV (`NGA_RTFP_mkt_*.csv`). Place
it in `data/raw/`.

## Architecture
World Bank CSV
│
▼
┌─────────────┐
│ BRONZE │ Raw ingestion, untouched
└──────┬──────┘
│
▼
┌─────────────┐
│ SILVER │ Cleaned, validated, long/tidy format
└──────┬──────┘
│
▼
┌─────────────┐
│ GOLD │ Analytical tables (in progress)
└─────────────┘


Planned: Azure Blob Storage → Databricks → Delta Lake for the cloud version
of this pipeline.

## Key Engineering Decisions

**Grain of the Silver table:** one row = one commodity's price in one
market at one point in time (long/tidy format), rather than one row per
market-month with 16 separate commodity columns. This makes filtering,
aggregation, and time-series analysis dramatically simpler.

**Handling missing prices — a two-stage investigation:**

Naively pivoting all 16 commodity columns into long format for every
market produced NULL rates of 78–99% across nearly every commodity, state,
and year — suspiciously uniform, which pointed to a structural problem
rather than genuine missing data.

Investigation confirmed it: **most markets in this dataset only track a
subset of the 16 commodities** (commonly a fixed set of ~12, or a
different FAO-specific set of ~5), not all 16. The initial wide-to-long
transformation was blindly exploding all 16 commodities onto every
market, fabricating rows for commodity/market combinations that were
never surveyed in the first place.

**Fix:** a coverage lookup was built (per market, which commodities does
it ever record a non-null price for, across the full time series), joined
onto the raw data, and used to filter the exploded rows — so the Silver
table only contains (market, commodity) pairs that genuinely exist in the
source. This dropped the exploded row count from 279,424 to 164,964.

After the fix, remaining NULLs (78.5% overall) show a real, explainable
pattern instead of uniform noise:
- Sharply higher in 2007–2015 (~95–99%) vs. 2017 onward (~50–60%),
  consistent with the data collection network expanding over time
- Concentrated in Sokoto, Adamawa, Borno, and Yobe — Nigeria's
  conflict-affected northeast — suggesting data collection reliability
  correlates with regional insecurity rather than random gaps

These NULLs are preserved as-is rather than imputed, since replacing them
would fabricate observations that were never collected. Any future
imputation will be flagged explicitly (e.g. `price_is_imputed`) rather
than silently blended with real observations.

## Project Structure

nigeria-food-price-intelligence/
│
├── data/
│ ├── raw/ # source CSV (not committed)
│ ├── silver/ # cleaned, long-format data
│ └── gold/ # analytical tables
│
├── Notebooks/
│ ├── data_profiling.py # initial pandas inspection of raw data
│ ├── silver_transformation.py # bronze -> silver, coverage-filtered
│ └── null_investigation.py # missingness analysis by commodity/state/year
│
├── sql/
├── src/
├── tests/
├── requirements.txt
└── README.md
# Status

- [x] Bronze ingestion
- [x] Silver transformation (coverage-filtered long format)
- [x] NULL investigation and root-cause analysis
- [ ] Gold layer aggregations
- [ ] Azure Blob Storage + Databricks migration
- [ ] SQL analytical queries