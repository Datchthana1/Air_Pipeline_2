---
pretty_name: Thailand Air Quality & Earthquake Star Schema (AIR4Thai + OpenWeather)
tags:
  - air-quality
  - thailand
  - timeseries
  - environmental
  - earthquake
language:
  - th
  - en
size_categories:
  - 100K<n<1M
---

# Thailand Air Quality & Earthquake Star Schema

## Dataset Description

This dataset is the output of an Apache Airflow pipeline that ingests, per-station transforms,
and stars-schema-marts two independent Thai environmental data streams into a Supabase
(hosted Postgres) database:

1. **Air quality + weather**, joined at station-hour granularity, from:
   - **AIR4Thai** (`http://air4thai.pcd.go.th/services/getNewAQI_JSON.php`) — the Thai
     Pollution Control Department's public real-time station network (AQI, PM2.5, PM10,
     O3, CO, NO2, SO2, per-pollutant sub-AQI, station metadata/coordinates).
   - **OpenWeather** Air Pollution API (`https://api.openweathermap.org/data/2.5/air_pollution`)
     and Weather API (`https://api.openweathermap.org/data/2.5/weather`) — pollutant
     concentrations plus temperature, humidity, pressure, wind, cloud cover and weather
     description, fetched per station's lat/lon as a satellite/model cross-check alongside
     the ground-truth AIR4Thai reading.
2. **Earthquake events**, from a Thai earthquake-report API polled hourly, deduplicated into
   discrete events (domestic + regional, with Thai-language location fields).

**Update cadence:** AIR4Thai only ever serves the *current* reading per station (no history
API), so ingestion runs hourly via Airflow (`PL0_ingestion_air_station`, cron `45 * * * *`
Asia/Bangkok; `PL0_ingestion_earthquake`, `@hourly`) and the pipeline itself is the historical
archive — there is no way to backfill air-quality history from the source beyond what this
pipeline has already captured.

**Geographic scope:** Thailand — all stations reported by AIR4Thai (up to 180 station IDs
observed), nationwide earthquake reports with a `is_domestic` flag distinguishing
Thailand-based events from regional ones felt in Thailand.

**Not yet measured:** total distinct provinces/regions covered, exact AIR4Thai polling
uptime/SLA, full earthquake magnitude distribution — none of this has been independently
verified and should not be assumed.

## Dataset Structure

The database has three logical layers: **raw** (as ingested), **per-station transformed**
(one table per station, typed/cleaned), and **mart** (conformed star schema). Only the mart
layer is described here in schema depth; see Data Collection & Processing below for how the
layers relate and their current relative trustworthiness.

### `dim_station`

| column | type | notes |
|---|---|---|
| `station_key` | bigint, identity PK | surrogate key |
| `station_id` | text, unique, not null | AIR4Thai station code (e.g. `33t`, `o24`) |
| `area_th` | text | station area/description, Thai |
| `area_en` | text | station area/description, English |
| `location` | text | derived: trailing comma-segment of `area_en` |
| `station_type` | text | AIR4Thai station type |
| `lat`, `lon` | float | station coordinates |
| `updated_at` | timestamptz, default now() | last upsert time |

As of the last verification pass, `dim_station` held **177 rows** against **180 distinct
station IDs** seen in the raw/per-station layer — 3 stations (`o24`, `o29`, `o67`) are missing
from this dimension (see Known Limitations).

### `dim_date`

Standard date dimension, one row per calendar date present in the source data: `date_key`
(int, `YYYYMMDD`, PK), `full_date`, `year`, `quarter`, `month`, `month_name`, `day`,
`day_of_week`, `day_name`, `week_of_year`, `is_weekend`.

### `fact_air_quality`

| column | type | notes |
|---|---|---|
| `fact_key` | bigint, identity PK | surrogate key |
| `station_key` | bigint, FK → `dim_station` | |
| `date_key` | int, FK → `dim_date` | |
| `station_id` | text, not null | denormalized natural key |
| `recorded_at` | timestamp, not null | AIR4Thai reading timestamp |
| `created_at` | timestamp | ingest-time timestamp |
| `aqi`, `aqi_param` | numeric, text | AIR4Thai overall AQI and the pollutant that drove it |
| `pm25_value`, `pm25_aqi` | numeric | PM2.5 µg/m³ and its sub-index |
| `pm10_value`, `pm10_aqi` | numeric | PM10 µg/m³ and its sub-index |
| `o3_value`, `o3_aqi` | numeric | Ozone and its sub-index |
| `co_value`, `co_aqi` | numeric | Carbon monoxide and its sub-index |
| `no2_value`, `no2_aqi` | numeric | NO2 and its sub-index |
| `so2_value`, `so2_aqi` | numeric | SO2 and its sub-index |
| `ow_aqi` | numeric | AQI computed from OpenWeather PM2.5 (see AQI computation below) |
| `ow_no`, `ow_no2`, `ow_o3`, `ow_so2`, `ow_pm25`, `ow_pm10`, `ow_nh3` | numeric | OpenWeather Air Pollution components |
| `ow_temp`, `ow_feels_like`, `ow_humidity`, `ow_pressure` | numeric | OpenWeather Weather API fields |
| `ow_wind_speed`, `ow_wind_deg`, `ow_clouds` | numeric | wind/cloud fields |
| `ow_weather` | text | short weather description |

`unique (station_id, recorded_at)` is the fact table's grain and upsert conflict key — one
row per station per AIR4Thai reading timestamp. Row count at last check: **149,380**.
Note: `ow_co` (OpenWeather CO) exists in the per-station tables but is **not** carried into
`fact_air_quality` — the mart's column list omits it while the per-station transform includes
it.

### Earthquake tables

- `earthquake_reports_raw`: `id` (PK), `fetched_at` (text), `payload` (jsonb, full API
  response), `created_at` (timestamptz). 715 rows at last check, hourly cadence.
- `earthquake_events`: `id` (PK), `datetime_utc`, `datetime_thai` (text, not null),
  `magnitude` (numeric), `depth_km` (int), `lat`/`lon` (numeric), `title_th`, `tambon_th`,
  `amphoe_th`, `province_th`, `location_en` (text), `is_domestic` (boolean, default false),
  `created_at` (timestamptz). `unique (datetime_utc, lat, lon)` is the dedup/upsert key.
  962 rows at last check. This table is event-driven, not time-driven — gaps between
  `created_at` values are expected when no earthquakes occur.

### AQI computation method

`ow_aqi` (the AQI computed from the OpenWeather PM2.5 reading, for cross-checking against
AIR4Thai's own `aqi`) is computed in `dags/function/ingest.py` (`calculate_aqi_pm25`) via
linear interpolation over a fixed PM2.5 breakpoint table:

| PM2.5 (µg/m³) low–high | AQI low–high |
|---|---|
| 0.0 – 15.0 | 0 – 25 |
| 15.1 – 25.0 | 26 – 50 |
| 25.1 – 37.5 | 51 – 100 |
| 37.6 – 75.0 | 101 – 200 |
| 75.1 – 150.0 | 201 – 300 |
| 150.1 – 250.0 | 301 – 400 |
| 250.1 – 500.0 | 401 – 500 |

PM2.5 values above 500.0 clamp to AQI 500; the function returns `None`/0 outside the table.
These breakpoints and the 0–500 index range match Thailand's PCD AQI scale for PM2.5, not
the (differently bucketed) US EPA table — note this if you were expecting literal EPA
breakpoints. The `aqi`/`pm25_aqi`/etc. columns sourced directly from AIR4Thai are whatever
sub-index AIR4Thai itself computed and reported; only `ow_aqi` is computed in this pipeline's
own code.

## Data Collection & Processing

Three-stage Airflow pipeline, one DAG per stage, chained via `TriggerDagRunOperator`:

1. **PL0 (ingest)** — `PL0_ingestion_air_station` (hourly, `45 * * * *` Asia/Bangkok) calls
   AIR4Thai for all stations, then calls OpenWeather Air Pollution + Weather per station by
   lat/lon, and upserts the joined row into raw table `air_stations`
   (conflict key `station_id, recorded_at`). Per-station try/except: a malformed payload or a
   flaky OpenWeather call (timeout, 429, missing field) on one station is skipped and logged,
   not allowed to fail the whole batch — but if *every* station fails in one run (e.g. an
   expired API key or a systemic outage), the task raises and fails loudly rather than
   silently pushing zero rows. `PL0_ingestion_earthquake` (hourly, `@hourly`) polls the
   earthquake report API and appends the raw JSON payload to `earthquake_reports_raw`.
2. **PL1 (per-station / per-event transform)** — `PL1_transform_air_station` fans out one
   Airflow mapped task per station (dynamic task mapping, capped at 5 concurrent) calling
   `transform_station()`, which casts raw text fields to numeric via `safe_numeric(v, lo, hi)`:
   any value that isn't a plain number, or falls outside a hardcoded plausible range
   (e.g. PM2.5/PM10/O3/CO/NO2 clamped to 0–900, SO2 to 0–600, all AQI sub-indices to 0–500),
   is nulled rather than propagated or allowed to abort the insert. Output lands in one table
   per station, `station_<sanitized_id>` (e.g. `station_33t`), upserted on
   `(station_id, recorded_at)`. `PL1_transform_earthquake` deduplicates raw reports into
   `earthquake_events` on `(datetime_utc, lat, lon)`.
3. **PL2 (mart build)** — `PL2_build_dim_fact` calls `build_dim_fact()`, which
   `UNION ALL`s every `station_*` table into a temp table, casts `recorded_at` via
   `safe_timestamp()` (malformed timestamp → null row, dropped, rather than aborting the
   union), and upserts into `dim_station` / `dim_date` / `fact_air_quality`. Fact upsert
   conflict key: **`(station_id, recorded_at)`**.

All three stages support a `mode` parameter (`latest` / `day` / `range`) propagated end to
end via `dag_run.conf`, so a date-range re-run can reprocess already-ingested history through
PL1/PL2 without needing to re-fetch from AIR4Thai (useful for backfills — AIR4Thai itself
cannot be backfilled, only the pipeline's own historical rows can be reprocessed).

**Design intent behind the null-and-skip approach:** upstream data quality problems (a bad
sensor reading, a station momentarily offline, an API field renamed) should degrade
gracefully to nulls on that one row/column rather than blocking ingestion or transform for
every other station. This is deliberate but means **downstream consumers should expect nulls
and must not assume a non-null value is validated-good** — it's validated as
"parseable and in a plausible range," not validated as "correct."

## Known Limitations / Considerations

Grounded in a live verification pass (2026-08-18 09:28 Bangkok time) and a follow-on
value-assessment pass. Stated verbatim from those findings, not softened:

- **The mart is ~63 hours stale.** `fact_air_quality`'s newest `created_at` is
  2026-08-15 18:45:03 while raw ingestion (`air_stations`) and per-station transform tables
  were current as of the check (raw max `created_at` 2026-08-18 08:45:04; 173/180 station
  tables fresh to ~1.5h). `PL2_build_dim_fact` stopped succeeding on 2026-08-15 while PL0/PL1
  kept running normally. **This is the single biggest problem with the dataset as currently
  published**: the star-schema mart — the layer this pipeline exists to produce — is not
  currently reflecting recent data and should not be treated as up to date until PL2 is
  re-run and the backlog cleared.
- **One invalid AQI outlier propagated into the mart.** `station_33t` has AQI = 501
  (out of the valid 0–500 range) in ≥10 rows, predating the `safe_numeric` clamp-bound fix in
  commit `e4dbf4f`. Those rows were never backfilled and the bad value is present in
  `fact_air_quality` at `fact_key = 2496` (and any other pre-fix rows for that station).
  Any aggregate (max AQI, average AQI, percentile) computed over the mart without excluding
  this is corrupted by it.
- **`dim_station` is missing 3 of 180 known stations**: `o24`, `o29`, `o67` appear in the
  per-station/raw layer but not in `dim_station`, which breaks referential completeness for
  any join expecting every station to have a dimension row. This cascades from those stations
  being dark at the source (below) combined with the PL2 backlog.
- **7 stations are dark at the source**, unreported by AIR4Thai for 32–67+ days as of the
  check: `32t`, `o24`, `o28`, `o29`, `o38`, `o67`, `o72`. This is an upstream AIR4Thai gap,
  not a pipeline bug, but it reduces effective spatial coverage below the nominal 180-station
  network and is not currently documented anywhere in the schema itself.
- **No staleness monitoring currently exists** (suggested but not implemented as of this
  writing): an alert comparing mart lag against raw lag would have caught the PL2 stall
  immediately instead of it going unnoticed for ~63 hours.
- **Raw/per-station layers, by contrast, are current and trustworthy**: `air_stations`
  (217,533 rows), 0 nulls and 0 duplicates found in the latest 1,000 rows checked, and
  `safe_numeric` correctly nulled garbage `pm25`/`aqi` text values in a spot check (0/180
  station tables flagged). `earthquake_reports_raw` (715 rows) and `earthquake_events`
  (962 rows) were current and consistent with expected hourly/event-driven cadence
  respectively.
- **Bottom line for consumers**: treat `air_stations` and the per-station `station_*` tables
  as the current source of truth today. Treat `dim_station` / `dim_date` /
  `fact_air_quality` as stale and containing at least one known bad value until PL2 has been
  re-run and the AQI=501 rows backfilled — do not rely on the mart for anything requiring
  data from after 2026-08-15 18:45 or for AQI aggregates until the outlier is corrected.

## Licensing / Attribution

- **AIR4Thai** data originates from Thailand's Pollution Control Department (a public
  government agency) via a publicly accessible API
  (`http://air4thai.pcd.go.th/services/getNewAQI_JSON.php`). No explicit license terms for
  this endpoint were located or verified as part of producing this card — attribute AIR4Thai
  / Thailand PCD as the source, and confirm the department's redistribution terms before
  republishing or commercially using this data.
- **OpenWeather** data (Air Pollution API and Weather API) is sourced from a commercial API
  requiring an API key. OpenWeather's terms of service govern redistribution of derived data
  and were **not verified** as part of producing this card — check OpenWeather's current
  terms (including any restrictions on caching/redistributing raw API responses) before
  redistributing the `ow_*` columns of this dataset, especially for commercial use.
- **Earthquake report data** source API and its terms were not identified/verified as part
  of producing this card.
- No overall license is asserted for this derived dataset pending confirmation of the above
  upstream terms. Do not treat this card as granting or implying redistribution rights beyond
  what the underlying sources permit.
