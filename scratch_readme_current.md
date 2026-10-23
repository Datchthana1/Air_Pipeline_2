---
pretty_name: Thailand Air Quality & Weather Dataset
language:
  - th
  - en
license: apache-2.0
tags:
  - air-quality
  - weather
  - thailand
  - aqi
size_categories:
  - 100K<n<1M
configs:
  - config_name: air_stations
    default: true
    data_files:
      - split: train
        path: air_stations.csv
  - config_name: dim_station
    data_files:
      - split: train
        path: dim_station.csv
  - config_name: dim_date
    data_files:
      - split: train
        path: dim_date.csv
  - config_name: fact_air_quality
    data_files:
      - split: train
        path: fact_air_quality.csv
---

# Thailand Air Quality & Weather Dataset

## Dataset Description

Hourly air-quality readings from **AIR4Thai** (Thailand Pollution Control Department)
enriched with matching weather/air-pollution data from the **OpenWeather** API, collected
by an Airflow pipeline (`Air_Pipeline_2`) that ingests every hour and builds a small
star-schema mart on top:

- **AIR4Thai** (`getNewAQI_JSON.php`) — the PCD's public real-time station network: overall
  AQI, PM2.5, PM10, O3, CO, NO2, SO2, per-pollutant sub-AQI, station metadata/coordinates.
- **OpenWeather** Air Pollution + Weather APIs — pollutant concentrations plus temperature,
  humidity, pressure, wind, cloud cover and weather description, fetched per station's
  lat/lon as a cross-check alongside the ground-truth AIR4Thai reading.

**Update cadence:** AIR4Thai only ever serves the *current* reading per station (no history
API), so ingestion runs hourly (`45 * * * *` Asia/Bangkok) and this dataset **is** the
historical archive — there is no way to backfill air-quality history from the source beyond
what this pipeline has already captured.

**Geographic scope:** Thailand — all stations reported by AIR4Thai (up to 180 station IDs
observed).

**Not yet measured:** total distinct provinces/regions covered, exact AIR4Thai polling
uptime/SLA — not independently verified, don't assume.

## Dataset Structure

Four configs, matching the pipeline's raw and mart layers:

### `air_stations` (raw, one row per station-hour as ingested)

40 columns: station identity/location (`station_id`, `area_th`, `area_en`, `station_type`,
`lat`, `lon`), AIR4Thai readings (`aqi`, `aqi_param`, `pm25_value`, `pm25_aqi`, `pm10_value`,
`pm10_aqi`, `o3_value`, `o3_aqi`, `co_value`, `co_aqi`, `no2_value`, `no2_aqi`, `so2_value`,
`so2_aqi`), OpenWeather fields (`ow_aqi`, `ow_co`, `ow_no`, `ow_no2`, `ow_o3`, `ow_so2`,
`ow_pm25`, `ow_pm10`, `ow_nh3`, `ow_temp`, `ow_feels_like`, `ow_humidity`, `ow_pressure`,
`ow_wind_speed`, `ow_wind_deg`, `ow_clouds`, `ow_weather`), and `recorded_at`/`created_at`
timestamps. 182.6K rows.

### `dim_station` (mart dimension)

`station_key` (surrogate PK), `station_id` (natural key), `area_th`, `area_en`, `location`
(derived), `station_type`, `lat`, `lon`, `updated_at`. 177 rows.

### `dim_date` (mart dimension)

Standard date dimension: `date_key` (`YYYYMMDD`), `full_date`, `year`, `quarter`, `month`,
`month_name`, `day`, `day_of_week`, `day_name`, `week_of_year`, `is_weekend`. 45 rows.

### `fact_air_quality` (mart fact table)

36 columns: `fact_key` (PK), `station_key`/`date_key` (FKs), `station_id`, `recorded_at`,
`created_at`, the same AIR4Thai + OpenWeather measurement columns as `air_stations` **except**
`ow_co`, which is captured upstream but not currently carried into this table. Grain and
upsert key: one row per `(station_id, recorded_at)`. 141.3K rows.

### AQI computation

`ow_aqi` (computed from the OpenWeather PM2.5 reading, for cross-checking against AIR4Thai's
own `aqi`) uses linear interpolation over this PM2.5 breakpoint table, which matches
Thailand's PCD PM2.5 AQI scale (not the differently-bucketed US EPA table):

| PM2.5 (µg/m³) low–high | AQI low–high |
|---|---|
| 0.0 – 15.0 | 0 – 25 |
| 15.1 – 25.0 | 26 – 50 |
| 25.1 – 37.5 | 51 – 100 |
| 37.6 – 75.0 | 101 – 200 |
| 75.1 – 150.0 | 201 – 300 |
| 150.1 – 250.0 | 301 – 400 |
| 250.1 – 500.0 | 401 – 500 |

PM2.5 above 500.0 clamps to AQI 500. All other `aqi`/`*_aqi` columns are AIR4Thai's own
reported sub-indices, not recomputed by this pipeline.

## Data Collection & Processing

Three-stage Airflow pipeline:

1. **Ingest** (hourly) — calls AIR4Thai for all stations, then OpenWeather per station by
   lat/lon, upserts into `air_stations` on `(station_id, recorded_at)`. A malformed payload
   or flaky OpenWeather call on one station is skipped and logged, not allowed to fail the
   whole batch — but if *every* station fails in one run, the task fails loudly instead of
   silently publishing zero rows.
2. **Per-station transform** — casts raw text fields to numeric with range clamping (e.g.
   PM2.5/PM10/O3/CO/NO2 to 0–900, SO2 to 0–600, AQI sub-indices to 0–500); anything unparseable
   or out of range is nulled rather than propagated.
3. **Mart build** — unions per-station tables and upserts into `dim_station`/`dim_date`/
   `fact_air_quality`.

**Design intent:** upstream data-quality problems (a bad sensor reading, a station
momentarily offline, a renamed API field) degrade gracefully to nulls on that one row/column
rather than blocking processing for every other station — so a non-null value means
"parseable and in a plausible range," not "independently verified correct."

## Known Limitations / Considerations

From a live verification pass against the source database (2026-08-18):

- Ingestion and per-station transform layers were current to within ~1.5 hours and free of
  nulls/duplicates at check time.
- The mart (`dim_station`/`dim_date`/`fact_air_quality`) can lag behind the raw layer when the
  mart-build stage doesn't run on schedule — at one point it was ~63 hours behind. If you need
  guaranteed-current data, check `fact_air_quality`'s max `created_at` against `air_stations`'
  before relying on it as "now."
- At least one row (`station_id = 33t`) has an AQI value of 501, outside the valid 0–500
  range, predating a range-clamp fix — not yet backfilled at the time of the check. Exclude or
  clip outlier AQI values above 500 before computing aggregates.
- A small number of stations (observed: `32t`, `o24`, `o28`, `o29`, `o38`, `o67`, `o72`) can go
  dark at the AIR4Thai source for extended periods — this is an upstream gap, not a pipeline
  defect, but it reduces effective spatial coverage below the nominal station count; check for
  gaps per `station_id` before assuming uniform coverage.
- `ow_co` is captured during ingestion but not currently included in `fact_air_quality` — use
  `air_stations` if you need OpenWeather CO readings.

## Licensing / Attribution

Released under **Apache 2.0**. Underlying source data originates from:
- **AIR4Thai** (Thailand Pollution Control Department) — a public government API.
- **OpenWeather** — a commercial API; if redistributing raw `ow_*` values at scale, check
  OpenWeather's current terms of service.
