# Data cleaning rules

This document lists every cleaning rule the pipeline applies, where each rule lives in the code,
and which test or data-quality check proves it. The rule IDs are used in code comments and in
`dags/sqlscript/dq-checks.sql`.

## Layers

| Layer | Tables | Written by |
|---|---|---|
| Raw air quality | `air_stations` | PL0 (`dags/function/ingest.py`) |
| Per station | `station_<sanitized_id>`, e.g. `station_33t` | PL1 (`air-station-transform.sql`) |
| Mart (star schema) | `dim_station`, `dim_date`, `fact_air_quality` | PL2 (`build-dim-fact.sql`) |
| Earthquake raw | `earthquake_reports_raw` (jsonb payload) | PL0 earthquake (`ingest_earthquake.py`) |
| Earthquake events | `earthquake_events` | PL1 earthquake (`ingest_earthquake.py`) |

**Design rule:** malformed values degrade to NULL, or a row or station is skipped. They never fail
the batch. The only exception is a systemic failure, such as every station failing or the source
being unreachable. In that case the task fails loudly so Airflow can retry it.

All timestamps are Asia/Bangkok wall-clock time stored without a UTC offset (rule S10).

## Rules

`Proven by` lists pytest node IDs (in `tests/`) and/or data-quality check IDs (`DQnn` in
`dags/sqlscript/dq-checks.sql`). "manual" means the rule is only checked by a real run and has no
automated test.

### Air quality: ingest (Python)

| Rule | What it does | Implemented in | Proven by |
|---|---|---|---|
| A1 | AIR4Thai GET uses a 30 s timeout and `raise_for_status()`. A payload without a top-level `stations` key raises `KeyError`, which is not caught, so the task fails. | `dags/function/ingest.py::fetch_air4thai` | manual (PL0 run) |
| A2 | Each station needs `stationID`, `areaTH`, `areaEN`, `stationType`, `lat`, `long`, and the `AQILast` fields. A missing or wrongly typed field skips only that station with a WARNING. | `dags/function/ingest.py::ingest_all`, `build_station_row` | `test_ingest_all_skips_bad_stations` |
| A3 | `lat` and `lon` are cast with `float()`. A non-numeric value skips the station. Values are not range-checked in Python. | `dags/function/ingest.py::build_station_row` | `test_build_station_row_bad_lat_raises_valueerror`, `test_build_station_row_preserves_raw_values`; DQ04 |
| A4 | OpenWeather air and weather GETs use a 30 s timeout and `raise_for_status()`. A `RequestException`, a missing component key, an empty `list` or `weather` array (FIX-1), or a non-dict station entry skips only that station. The station is checked before OpenWeather is called, so malformed stations do not use quota. | `dags/function/ingest.py::fetch_openweather`, `parse_openweather`, `ingest_all` | `test_parse_openweather_maps_fields`, `test_parse_openweather_empty_list_raises_indexerror`, `test_parse_openweather_missing_component_raises_keyerror`, `test_ingest_all_empty_ow_list_is_skipped_not_fatal`, `test_ingest_all_does_not_call_openweather_for_malformed_station` |
| A5 | If every station fails, `RuntimeError("All N station(s) failed ...")` is raised. An empty station list returns an empty frame, and `push_to_supabase` skips the upsert with a warning. | `dags/function/ingest.py::ingest_all`, `push_to_supabase` | `test_ingest_all_all_failed_raises_runtimeerror`, `test_ingest_all_empty_station_list_returns_empty_df`, `test_push_to_supabase_empty_df_skips` |
| A6 | AIR4Thai pollutant and AQI values pass through unchanged. Strings and sentinels such as `-1` or `N/A` are cleaned later in SQL (S1). | `dags/function/ingest.py::build_station_row` | `test_build_station_row_preserves_raw_values`; S1 tests |
| A7 | `recorded_at = "<date> <time>"` as AIR4Thai supplies it. No timezone conversion. Validated downstream by `safe_timestamp` (S6). | `dags/function/ingest.py::build_station_row` | `test_build_station_row_preserves_raw_values`; DQ03, DQ15 |
| A8 | `created_at` is computed once per run as Bangkok time in `%Y-%m-%d %H:%M:%S`. Every row in the batch gets the same value. | `dags/function/common.py::now_bkk_text`, `ingest.py::ingest_all` | `test_now_bkk_text_format`, `test_now_bkk_text_is_bangkok`, `test_ingest_all_skips_bad_stations`; DQ15 |
| A9 | `ow_aqi` comes from `calculate_aqi_pm25(pm2_5)`. None, NaN and inf return None (FIX-2). Non-numeric input raises and skips the station. Output is linear interpolation over the Thai PCD breakpoints, rounded with Python `round()` (half-to-even). Above 500 returns 500, below 0 returns 0. | `dags/function/ingest.py::calculate_aqi_pm25` | `test_calculate_aqi_pm25_values`, `test_calculate_aqi_pm25_non_numeric_raises`, `test_calculate_aqi_pm25_nan_is_not_zero`, `test_aqi_breakpoints_contiguous`; DQ12 |
| A10 | OpenWeather `ow_*` fields are copied as-is. No range checks in Python or SQL. Implausible values are reported, not blocked. | `dags/function/ingest.py::parse_openweather` | `test_parse_openweather_maps_fields`; DQ14 |
| A11 | Upsert on `on_conflict='station_id,recorded_at'`, so re-ingesting a reading deduplicates it (last write wins). | `dags/function/ingest.py::push_to_supabase` | `test_push_to_supabase_upserts_on_natural_key`, `test_push_to_supabase_empty_df_skips`; DQ01, DQ02 |
| A12 | Reload parameters: `dag_run.conf` takes precedence over `params`. `''` and `None` count as missing. Defaults are `mode='latest'` and the other fields `None`. RPC wrappers turn `''` into `None`. `transform_station` passes `station_id` through unchanged. | `dags/function/common.py::resolve_reload`, `ingest.py::_reload_rpc_params` | `test_resolve_reload_precedence`, `test_resolve_reload_no_dag_run_uses_defaults`, `test_resolve_reload_conf_none`, `test_resolve_reload_blank_station_id_is_none`, `test_rpc_wrappers_blank_to_none`, `test_transform_station_rpc_args`, `test_list_stations_returns_ids` |

### Air quality: SQL

| Rule | What it does | Implemented in | Proven by |
|---|---|---|---|
| S1 | `safe_numeric(v, lo, hi)`: NULL, or text that does not match `^-?\d+(\.\d+)?$`, becomes NULL. A value outside the inclusive range `[lo, hi]` becomes NULL. It is not clamped. Bounds: `*_value` 0..900 (SO2 0..600), `aqi` and every `*_aqi` 0..500. | `dags/sqlscript/air-station-transform.sql::safe_numeric` | `test_safe_numeric_bounds`, `test_safe_numeric_regex_semantics`, `test_safe_numeric_accepts`, `test_safe_numeric_rejects`, `test_safe_numeric_reference_model`; DQ12, DQ13 |
| S2 | `lat`, `lon`, `aqi_param` and all `ow_*` columns are copied without range checks or casting. | `dags/sqlscript/air-station-transform.sql::transform_station` | DQ14 (warn only) |
| S3 | `location = TRIM(SPLIT_PART(area_en, ',', -1))`, the last comma-separated part. NULL `area_en` gives NULL `location`. | `dags/sqlscript/air-station-transform.sql::transform_station` | manual |
| S4 | Per-station table name is `'station_' \|\| regexp_replace(station_id, '[^a-zA-Z0-9]', '_', 'g')`, quoted with `%I`, so case is preserved. | `dags/sqlscript/air-station-transform.sql::transform_station` | DQ16 |
| S5 | Mode `latest` takes rows at `MAX(created_at)` for the station. `day` and `range` filter on `recorded_at::date`. `full` takes all rows. Rows are upserted on `(station_id, recorded_at)`. Known gap, see below. | `dags/sqlscript/air-station-transform.sql::transform_station`, `get_distinct_stations` | DQ03 (detects the gap) |
| S6 | `safe_timestamp(text)` returns NULL when the cast to `timestamp` fails. Rows with a NULL `recorded_at` are left out of `dim_station`, `dim_date` and `fact_air_quality`. | `dags/sqlscript/build-dim-fact.sql::safe_timestamp` | `test_build_dim_fact_drops_bad_timestamps`; DQ03 |
| S7 | `dim_station` has one row per `station_id`, taken from the latest valid `recorded_at` in the window (`DISTINCT ON`). It is overwritten on conflict (SCD type 1) and `updated_at = now()`. `lat` and `lon` are cast to float. | `dags/sqlscript/build-dim-fact.sql::build_dim_fact` (dim_station) | DQ05, DQ06 |
| S8 | `dim_date`: `date_key = to_char(d,'YYYYMMDD')::int`, ISO `day_of_week` (1=Mon, 7=Sun), `is_weekend` = isodow in (6,7), ISO `week_of_year`. Insert-only (`ON CONFLICT DO NOTHING`). | `dags/sqlscript/build-dim-fact.sql::build_dim_fact` (dim_date) | DQ07, DQ11 |
| S9 | `fact_air_quality` uses an INNER JOIN to `dim_station`, so `station_key` is never an orphan. `date_key` comes from `safe_timestamp`. Upsert on `(station_id, recorded_at)`. `ow_co` is not carried into the fact table. | `dags/sqlscript/build-dim-fact.sql::build_dim_fact` (fact) | `test_build_dim_fact_drops_bad_timestamps`; DQ08, DQ09, DQ10, DQ11. Known gap on `created_at`, see below. |
| S10 | `recorded_at` and `created_at` are naive timestamps in Bangkok wall-clock time. No conversion happens anywhere. | all SQL layers | DQ15 |
| S11 | `build_dim_fact` mode `latest` takes `created_at = MAX(created_at)` across the UNION of all `station_*` tables. `day` and `range` filter on `safe_timestamp(recorded_at)::date`. Any other mode uses TRUE. `p_station_id` narrows the result. `statement_timeout` is 300 s. | `dags/sqlscript/build-dim-fact.sql::build_dim_fact` | DQ17 |

### Earthquake (Python and SQL)

| Rule | What it does | Implemented in | Proven by |
|---|---|---|---|
| E1 | TMD GET uses a 30 s timeout and `raise_for_status()`. Connectivity checks raise `AirflowException` on any `RequestException`. The Supabase check uses a 10 s timeout. | `dags/function/ingest_earthquake.py::_get_tmd`, `check_api_connection`, `check_supabase_connection` | `test_check_api_connection_wraps_errors` |
| E2 | The raw report is stored unchanged as jsonb with `fetched_at` in Bangkok text. The event count is `len(payload.get('DailyEarthquakes', []))`, so a missing key counts as 0. | `dags/function/ingest_earthquake.py::push_report_to_supabase` | `test_push_report_to_supabase_counts_events`, `test_push_report_to_supabase_missing_key_counts_zero`; DQ23 |
| E3 | `DateTimeUTC` and `DateTimeThai` are parsed as `%Y-%m-%d %H:%M:%S.%f` or `%Y-%m-%d %H:%M:%S`, then written as `%Y-%m-%d %H:%M:%S`. Fractions are truncated. Anything else returns None. | `dags/function/ingest_earthquake.py::_to_datetime_text` | `test_to_datetime_text`; DQ18 |
| E4 | `Magnitude`, `Latitude` and `Longitude` are converted with `float()`. Whitespace is allowed. Invalid, NaN and infinite input return None (FIX-3). | `dags/function/ingest_earthquake.py::_to_float` | `test_to_float`; DQ20 |
| E5 | `depth_km = int(float(v))`, which truncates toward zero. Invalid, NaN and infinite input return None (FIX-4). | `dags/function/ingest_earthquake.py::_to_int` | `test_to_int`; DQ20 |
| E6 | `TitleThai` is parsed with a regex. The province is required and must be followed by `(`. Tambon and amphoe are optional. `location_en` is the text after the last `(` with the trailing `)` removed. | `dags/function/ingest_earthquake.py::_parse_location` | `test_parse_location` |
| E7 | `is_domestic` is true when a province was parsed (`province_th IS NOT NULL`). | `dags/function/ingest_earthquake.py::process_report` | `test_process_report_types_and_flags`; DQ22 |
| E8 | Output columns are exactly `_EVENT_COLUMNS`, with dtypes `Int64`, `float64` and `boolean`. An empty report or an empty list gives a zero-row frame with all columns. Reports are concatenated in input order. | `dags/function/ingest_earthquake.py::process_report`, `process_reports` | `test_process_report_empty_has_columns`, `test_process_reports_empty_list_has_columns`, `test_process_reports_concat_preserves_order` |
| E9 | Events are deduplicated on `(datetime_utc, lat, lon)` with `keep='first'` within a batch. The upsert on `datetime_utc,lat,lon` means the latest run wins across runs. | `dags/function/ingest_earthquake.py::prepare_events_for_upsert`, `push_events_to_supabase` | `test_prepare_events_dedupes_keep_first`, `test_push_events_to_supabase_upsert_key`; DQ19 |
| E10 | Rows with a NULL `datetime_utc` or `datetime_thai` are dropped with a WARNING (FIX-6). NaN and `pd.NA` values become None, so the JSON body is valid (FIX-7). | `dags/function/ingest_earthquake.py::prepare_events_for_upsert` | `test_prepare_events_drops_null_datetimes`, `test_prepare_events_records_are_json_safe`; DQ18 |
| E11 | `latest` returns the newest payload and raises if the table is empty. `day` and `range` filter `fetched_at` between `D 00:00:00` and `D 23:59:59`. `full` returns everything. Invalid combinations raise `AirflowException`. | `dags/function/ingest_earthquake.py::get_reports`, `report_time_window`, `get_latest_report` | `test_get_reports_latest_empty_raises`, `test_get_reports_day_filters_fetched_at`, `test_report_time_window`, `test_report_time_window_invalid_raises` |
| E12 | Not enforced in code: `datetime_thai = datetime_utc + 7h`, magnitude 0..10, depth 0..800, and global lat/lon bounds. | SQL checks only | DQ20, DQ21 |

## Bug fixes in this refactor

Each fix changes behaviour only for the bad input described. Valid input is unchanged.

| ID | Before | After | Proven by |
|---|---|---|---|
| FIX-1 | An empty OpenWeather `list` or `weather` array raised `IndexError`. It was not caught, so the whole batch crashed. A station entry that was not a dict also crashed the batch, because `station.get()` ran outside the `try`. | `IndexError` is caught and the station is skipped. A non-dict entry is skipped with a warning. | `test_ingest_all_empty_ow_list_is_skipped_not_fatal`, `test_ingest_all_skips_bad_stations` |
| FIX-2 | `calculate_aqi_pm25(nan)` returned `0`, and `inf` returned `500`. | NaN and inf return `None`. | `test_calculate_aqi_pm25_values` (nan and inf cases), `test_calculate_aqi_pm25_nan_is_not_zero` |
| FIX-3 | `_to_float('nan')` and `_to_float('inf')` returned NaN and inf. | Non-finite input returns `None`. | `test_to_float` |
| FIX-4 | `_to_int('inf')` raised `OverflowError`, which was not caught. | `OverflowError` is caught and the function returns `None`. | `test_to_int` |
| FIX-5 | `_to_datetime_text('2026-01-01 03:04:05')` returned `None`, because only the fractional format was accepted. | The format without fractional seconds is also accepted. | `test_to_datetime_text` |
| FIX-6 | An event with a NULL `datetime_utc` or `datetime_thai` broke the NOT NULL constraint and stopped the whole earthquake upsert. | Those rows are dropped with a WARNING, and the count is logged. | `test_prepare_events_drops_null_datetimes` |
| FIX-7 | Any NaN in the records (for example a missing magnitude, lat or lon) made httpx raise "Out of range float values are not JSON compliant", because it sends JSON with `allow_nan=False`. Under pandas 3, None in a string column also became NaN. | NaN and `pd.NA` become `None` before the upsert. | `test_prepare_events_records_are_json_safe` |

## Known gaps (not fixed)

These are documented and detected by data-quality checks. They are not changed by this refactor.

- **S5, day and range mode:** `transform_station` casts `recorded_at::date`. A malformed
  `recorded_at` makes that cast raise, which fails the station task. DQ03 finds such rows.
- **S9, `created_at` in the fact load:** the load uses `nullif(created_at,'')::timestamp`, which is
  not a safe cast. A bad value would fail the load. The cast is not wrapped in `safe_timestamp`.
- **A10 and S2, OpenWeather fields:** `ow_*` values are not range-checked anywhere. DQ14 reports
  them as warnings.
- **E6, `_parse_location` without `(`:** a title that has `จ.` but no `(` gives a NULL province. That
  event is then classed as non-domestic (E7). This is the current behaviour, and the test pins it.
- **E9, dedup within a batch:** the earliest report in the batch wins for a duplicated event key.

## How to run

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Data-quality checks run against the live database. They are read-only:

```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f dags/sqlscript/dq-checks.sql
```

In the output, every `error` check should return 0 rows. A `warn` check returning rows means a known
or upstream issue needs review. The file header lists the currently expected exceptions, such as the
`station_33t` AQI 501 rows (DQ12) and the `o24`, `o29`, `o67` stations (DQ16).
