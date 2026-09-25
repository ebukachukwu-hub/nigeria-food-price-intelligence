# Nigeria Food Price Intelligence Pipeline

A PySpark pipeline that turns the World Bank's Real Time Food Prices dataset
for Nigeria into validated, analysis-ready tables, and uses them to measure
food price volatility across markets.

The most useful thing it found was not a volatility ranking but a data quality
problem: a monitoring round in which milk prices across 20 Borno markets were
reported at roughly 5% of their normal level.

## Data Source

[World Bank Real Time Food Prices, Nigeria](https://microdata.worldbank.org/catalog/4503)
(`NGA_RTFP_mkt_2007_2026-08-24.csv`)

- 73 markets, monthly, January 2007 to August 2026
- 17,464 rows, 137 variables
- Compiled by the World Bank from WFP, FAO and national statistical office
  sources, with machine learning estimation of missing prices

For each commodity the file carries several columns. This pipeline uses three:

| Column | Meaning | Used as |
|---|---|---|
| `beans` | price as reported | `price_observed` |
| `c_beans` | World Bank model's close estimate | `price_estimated` |
| `trust_beans` | World Bank trust score | `trust` |

The raw data is not committed. To reproduce, download the Nigeria
market-level CSV from the Bulk Data Downloads table on the page above and
place it in `data/raw/`.

## Architecture

The pipeline runs in two places from the same transformation code
(`src/transformations.py`):

```
                  LOCAL                        DATABRICKS (Free Edition)

Source            data/raw/*.csv               Unity Catalog volume
                        |                              |
BRONZE            (the CSV itself)             bronze_food_prices
                        |                      Delta, all columns as text,
                        |                      plus _source_file, _ingested_at
                        |                              |
SILVER            data/silver (Parquet)        silver_food_prices (Delta)
                        |                              |
GOLD              data/gold (Parquet)          gold_* tables (Delta)
```

Local runs are for development and testing. On Databricks, each layer is a
Delta table in Unity Catalog (`workspace.nigeria_food`), and each layer reads
the previous one from its saved table. Both runs produce identical results:
44,112 Silver rows, 62 flagged spikes, and the same volatility figures to full
precision.

## Key Engineering Decisions

**Silver grain.** One row is one commodity in one market in one month, keyed
on `geo_id`, the source's market identifier. Every run checks that no key
appears twice.

**Observed prices, not model estimates.** Observed prices are 79% to 93% empty
in the raw file; the model estimates are 100% complete. Where both exist, the
model changes the observed price in roughly 10% to 17% of cases, sometimes
heavily. Analysing the estimates would mean analysing the model's view of the
market. Silver keeps both side by side, and Gold states which one it uses.

**Coverage bounded by each series' own history.** A (market, commodity) pair
only has rows between its first and last observed month. An earlier version
kept every month from 2007 for any commodity a market ever reported, which
created rows for years before the market was surveyed. Bounding the window
reduced Silver from 164,964 rows to 44,112, and 80.2% of the remaining rows
have an observed price. Gaps inside a series stay as NULL; nothing is imputed.

**Volatility from monthly changes, not price levels.** Volatility is the
standard deviation of month-on-month log price changes, from January 2020,
using consecutive months only, for series with at least 24 such changes.
An earlier version used the coefficient of variation of price levels over the
full period. That mostly measured inflation: a unit test shows a price rising
a steady 10% a month scores zero volatility under the current method and over
100% under the old one.

**Spikes are flagged, not deleted.** A price at least 3 times higher or lower
than both its previous and next observed prices (within two months) is marked
`price_is_spike` in Silver and excluded from volatility in Gold. A price that
jumps and stays at the new level is not flagged.

## Findings

### 1. A reporting anomaly in the December 2025 milk round

In December 2025, 20 Borno markets report milk at ₦150 to ₦871, against
₦2,877 to ₦5,424 in the World Bank's estimates and similar levels in the
surrounding months. The same month, commodity and direction across one
state points to a change in what was recorded for that round, such as a
smaller unit, rather than to 20 separate errors. This has not been confirmed
against the underlying surveys.

Smaller clusters appear in May 2017 (5 markets, milk, upward) and August
2017 (3 markets, fish). Across all commodities, 62 observed prices are
flagged.

This matters for analysis: before spike handling, a single one of these
entries made Monguno milk the most volatile series in the dataset. With that
one month removed, its volatility falls from 0.84 to 0.09.

### 2. Milk, fish and onions are the most volatile group

| Commodity | Median volatility (default settings) |
|---|---|
| milk | 0.368 |
| onions | 0.299 |
| fish | 0.299 |
| yam | 0.188 |
| maize flour | 0.175 |
| beans | 0.175 |
| ... | ... |
| rice | 0.090 |
| rice (FAO series) | 0.056 |

These three are the top three under every setting tested: no spike
removal, spike thresholds of 2x, 3x and 5x, a window starting in 2022, and a
lower minimum of 12 monthly changes. Their order within the group is not
stable. Milk ranks first in five of six settings but third when shorter
series are included, so this project does not claim milk is the single most
volatile commodity.

### 3. The ranking describes the northeast, not Nigeria

Every qualifying market for milk, fish, onions, beans, eggs, groundnuts and
meat is in the northeast (Borno, Yobe or Adamawa). The ranking above is therefore a ranking of the
northeast monitoring network. Only yam, millet and rice have enough markets
elsewhere to compare regions:

| Commodity | Northeast (median) | Rest of Nigeria (median) |
|---|---|---|
| rice | 0.109 (19 markets) | 0.055 (9 markets) |
| millet | 0.151 (27 markets) | 0.107 (11 markets) |
| yam | 0.189 (25 markets) | 0.182 (10 markets) |

Rice and millet are more volatile in the northeast; yam is not. This is
consistent with, but does not demonstrate, an effect of insecurity on
markets.

## How to Run

From the project root:

```
python -m pip install -r requirements.txt
python -m pytest Tests -v
python Notebooks/silver_transformation.py
python Notebooks/gold_transformation.py
```

Supporting checks, which read the saved tables and write nothing:

```
python Notebooks/observed_vs_estimated_check.py
python Notebooks/milk_finding_checks.py
python Notebooks/sensitivity_checks.py
```

On Databricks Free Edition:

1. Create the schema and volume:
   `CREATE SCHEMA IF NOT EXISTS workspace.nigeria_food;`
   `CREATE VOLUME IF NOT EXISTS workspace.nigeria_food.raw;`
2. Upload the CSV to the `raw` volume.
3. Clone this repository into the workspace as a Git folder.
4. Run `databricks/01_bronze`, `02_silver` and `03_gold` in order on
   serverless compute.

## Project Structure

```
nigeria-food-price-intelligence/
├── data/                      (not committed)
│   ├── raw/                   source CSV
│   ├── silver/                written by silver_transformation.py
│   └── gold/                  written by gold_transformation.py
├── databricks/                Databricks notebooks (Delta tables)
│   ├── 01_bronze.py
│   ├── 02_silver.py
│   └── 03_gold.py
├── Notebooks/                 local pipeline and checks
│   ├── silver_transformation.py
│   ├── gold_transformation.py
│   ├── observed_vs_estimated_check.py
│   ├── milk_finding_checks.py
│   └── sensitivity_checks.py
├── src/
│   ├── transformations.py     all transformation logic
│   └── local_io.py            local output handling (Windows)
├── Tests/
│   ├── conftest.py
│   └── test_transformations.py   19 tests
└── requirements.txt
```

## Status

- [x] Bronze to Silver with explicit typing and validation
- [x] Silver persisted to Parquet, Gold reads from it
- [x] Returns-based volatility with spike handling
- [x] Sensitivity checks on the main findings
- [x] 19 unit tests
- [x] Databricks Free Edition: Bronze, Silver and Gold as Delta tables,
      results verified against the local run
- [ ] Automated test runs on each push and pull request (GitHub Actions)
- [ ] Confirm the December 2025 anomaly against source survey data
