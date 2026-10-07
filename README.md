# Nigeria Food Price Intelligence Pipeline

![tests](https://github.com/Ebukachukwu-hub/nigeria-food-price-intelligence/actions/workflows/tests.yml/badge.svg)

A PySpark pipeline that turns the World Bank's Real Time Food Prices dataset
for Nigeria into validated, analysis-ready tables, and uses them to measure
food price volatility across markets.

The most useful thing it found was not a volatility ranking but a data quality
problem: a monitoring round in which milk prices across 21 Borno markets were
reported at a median of roughly 5% of their normal level. The same prices
appear in WFP's own published data, so the problem originates upstream of
the World Bank.

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

One check also uses WFP's own price database for Nigeria
("Nigeria - Food Prices" on HDX, saved as `data/raw/wfp_food_prices_nga.csv`),
to trace the December 2025 anomaly back to its source.

## Architecture

The pipeline runs in two places from the same transformation code
(`src/transformations.py`). The Databricks notebooks import it from the
repository's Git folder rather than keeping their own copy.

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

The unit tests run automatically on GitHub Actions on every push and pull
request, on a clean Linux machine with Python 3.14 and Java 17.

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

**Commodities compared within one region.** Most commodities are only
measured in northeast markets, so a national median would compare different
geographies. The commodity ranking uses northeast markets only, and regions
are compared only where a commodity has enough markets in both.

**Pinned dependencies.** `requirements.txt` pins PySpark and PyArrow to the
versions the tests were run against. The first CI run failed because the
file still pinned an old PyArrow that the local environment had long since
replaced; CI is what caught the mismatch.

## Findings

### 1. A reporting anomaly in the December 2025 milk round

In December 2025, 21 Borno markets report powdered milk (400 g, retail) at
₦150 to ₦1,173, against a median of about ₦3,800 to ₦4,000 in the months
either side. The same month, commodity and direction across one state points
to a change in what was recorded for that round, such as a smaller unit,
rather than to 21 separate errors.

The same prices, under the same unit label, appear in WFP's own published
price database on HDX, so the anomaly was not introduced by the World Bank's
compilation. Because the unit label does not change for that round, the cause
cannot be confirmed from public data. An unrecorded unit change is the most
likely explanation but is not demonstrated.

Twenty of the 21 entries are flagged as spikes. The 21st (Kashuwan Shanu,
₦1,172.50) is not caught by the 3x rule. It does not affect any result,
because that series has too few consecutive months to qualify for the
volatility table, but it shows the limit of a per-series threshold: it
catches most of a bad survey round, not all of it.

Smaller clusters appear in May 2017 (5 markets, milk, upward) and August
2017 (3 markets, fish). Across all commodities, 62 observed prices are
flagged.

This matters for analysis: before spike handling, a single one of these
entries made Monguno milk the most volatile series in the dataset. With that
one month removed, its volatility falls from 0.84 to 0.09.

### 2. Within the northeast, milk, fish and onions are the most volatile group

Every qualifying market for milk, fish, onions, beans, eggs, groundnuts and
meat is in the northeast (Borno, Yobe or Adamawa), so commodities are ranked
on northeast markets only.

| Commodity | Median volatility | Markets |
|---|---|---|
| milk | 0.368 | 21 |
| fish | 0.299 | 21 |
| onions | 0.299 | 23 |
| yam | 0.189 | 25 |
| maize flour | 0.175 | 21 |
| beans | 0.175 | 22 |
| millet | 0.151 | 27 |
| eggs | 0.147 | 23 |
| groundnuts | 0.136 | 22 |
| beef | 0.128 | 21 |
| goat meat | 0.126 | 21 |
| rice | 0.109 | 19 |

Default settings: observed prices, from January 2020, at least 24 monthly
changes, spikes at 3x excluded. The FAO-sourced series (gari, maize, rice and
sorghum) cover a single northeast market each and are left out, since a
one-market figure is not comparable with a median over twenty.

Milk, fish and onions are the top three under every setting tested: no
spike removal, spike thresholds of 2x, 3x and 5x, a window starting in 2022,
and a lower minimum of 12 monthly changes. Their order within the group is
not stable. Milk ranks first in five of six settings but third when shorter
series are included, where fish ranks first, so this project does not claim
milk is the single most volatile commodity.

### 3. Regions can only be compared for three commodities

Only yam, millet and rice have enough markets outside the northeast to
compare regions:

| Commodity | Northeast (median) | Rest of Nigeria (median) |
|---|---|---|
| rice | 0.109 (19 markets) | 0.055 (9 markets) |
| millet | 0.151 (27 markets) | 0.107 (11 markets) |
| yam | 0.189 (25 markets) | 0.182 (10 markets) |

Rice and millet are more volatile in the northeast; yam is not. This is
consistent with, but does not demonstrate, an effect of insecurity on
markets.

## How to Run

Requirements: Python 3.14 and a Java runtime supported by PySpark 4.2
(Java 17, 21 or 25).

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
python Notebooks/northeast_ranking_checks.py
python Notebooks/source_check_milk.py      (needs the WFP CSV in data/raw/)
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
├── .github/
│   └── workflows/
│       └── tests.yml          runs the unit tests on push and pull request
├── data/                      (not committed)
│   ├── raw/                   World Bank CSV, WFP CSV
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
│   ├── sensitivity_checks.py
│   ├── northeast_ranking_checks.py   commodity ranking, northeast only
│   └── source_check_milk.py          December 2025 anomaly vs WFP data
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
- [x] Sensitivity checks on the main findings, with commodities compared
      within the northeast
- [x] 19 unit tests
- [x] Databricks Free Edition: Bronze, Silver and Gold as Delta tables,
      results verified against the local run
- [x] Automated test runs on each push and pull request (GitHub Actions)
- [x] Traced the December 2025 anomaly to WFP's published data
      (same prices, same unit label)
- [ ] Establish the cause of the December 2025 anomaly with WFP
