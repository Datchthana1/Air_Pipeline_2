"""AIR4Thai + OpenWeather ingestion and the Supabase RPC wrappers for the air-station pipeline.

Cleaning rules for this module are listed in docs/DATA_CLEANING.md (A1-A12).
Supabase client, Bangkok clock and reload resolution live in function/common.py.
"""
from __future__ import annotations

import logging
import math
import os

import pandas as pd
import requests

from function.common import (  # noqa: F401 (re-exported)
    HTTP_TIMEOUT,
    SUPABASE_KEY,
    SUPABASE_URL,
    get_client,
    now_bkk_text,
    resolve_reload,
)

AIR4THAI_URL = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
OPENWEATHER_AIR_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
OPENWEATHER_WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_air4thai() -> list:
    """Return the 'stations' list from the AIR4Thai current-AQI endpoint."""
    response = requests.get(AIR4THAI_URL, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json()['stations']


_PM25_BREAKPOINTS = [
    (0.0, 15.0, 0, 25),
    (15.1, 25.0, 26, 50),
    (25.1, 37.5, 51, 100),
    (37.6, 75.0, 101, 200),
    (75.1, 150.0, 201, 300),
    (150.1, 250.0, 301, 400),
    (250.1, 500.0, 401, 500),
]


def calculate_aqi_pm25(pm25: float | str | None) -> int | None:
    """Convert PM2.5 (ug/m3) to Thai PCD AQI by linear interpolation over the breakpoints.

    The input is rounded to 1 decimal, then rounded to an int with Python round()
    (half-to-even). None returns None, and so do NaN and inf. Values above 500 return 500
    and values below 0 return 0.
    """
    if pm25 is None:
        return None
    pm25 = round(float(pm25), 1)
    if not math.isfinite(pm25):  # FIX-2: NaN/inf used to return 0/500
        return None
    for bp_lo, bp_hi, aqi_lo, aqi_hi in _PM25_BREAKPOINTS:
        if bp_lo <= pm25 <= bp_hi:
            return round((aqi_hi - aqi_lo) / (bp_hi - bp_lo) * (pm25 - bp_lo) + aqi_lo)
    return 500 if pm25 > 500.0 else 0


def parse_openweather(air_json: dict, wx_json: dict) -> dict:
    """Map the OpenWeather air_pollution and weather payloads to the 17 ow_* fields.

    Raises KeyError or IndexError when a component is missing or the 'list' is empty.
    """
    c = air_json['list'][0]['components']

    return {
        "ow_aqi":         calculate_aqi_pm25(c['pm2_5']),
        "ow_co":          c['co'],
        "ow_no":          c['no'],
        "ow_no2":         c['no2'],
        "ow_o3":          c['o3'],
        "ow_so2":         c['so2'],
        "ow_pm25":        c['pm2_5'],
        "ow_pm10":        c['pm10'],
        "ow_nh3":         c['nh3'],
        "ow_temp":        wx_json['main']['temp'],
        "ow_feels_like":  wx_json['main']['feels_like'],
        "ow_humidity":    wx_json['main']['humidity'],
        "ow_pressure":    wx_json['main']['pressure'],
        "ow_wind_speed":  wx_json['wind']['speed'],
        "ow_wind_deg":    wx_json['wind']['deg'],
        "ow_clouds":      wx_json['clouds']['all'],
        "ow_weather":     wx_json['weather'][0]['description'],
    }


def fetch_openweather(lat: float, lon: float) -> dict:
    """Call OpenWeather air_pollution and weather for a point and return the ow_* fields."""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    params  = {"lat": lat, "lon": lon, "appid": api_key}

    air     = requests.get(OPENWEATHER_AIR_URL, params=params, timeout=HTTP_TIMEOUT)
    weather = requests.get(OPENWEATHER_WEATHER_URL, params={**params, "units": "metric"}, timeout=HTTP_TIMEOUT)

    air.raise_for_status()
    weather.raise_for_status()

    return parse_openweather(air.json(), weather.json())


def build_station_row(station: dict, ow: dict, created_at: str) -> dict:
    """Build one air_stations row from an AIR4Thai station and its ow_* fields.

    Raises KeyError/TypeError/ValueError on malformed input; the caller skips the station.
    """
    aqi_last = station['AQILast']
    return {
        'station_id':   station['stationID'],
        'area_th':      station['areaTH'],
        'area_en':      station['areaEN'],
        'station_type': station['stationType'],
        'lat':          float(station['lat']),
        'lon':          float(station['long']),
        'recorded_at':  f"{aqi_last['date']} {aqi_last['time']}",
        'aqi':          aqi_last['AQI']['aqi'],
        'aqi_param':    aqi_last['AQI']['param'],
        'pm25_value':   aqi_last['PM25']['value'],
        'pm25_aqi':     aqi_last['PM25']['aqi'],
        'pm10_value':   aqi_last['PM10']['value'],
        'pm10_aqi':     aqi_last['PM10']['aqi'],
        'o3_value':     aqi_last['O3']['value'],
        'o3_aqi':       aqi_last['O3']['aqi'],
        'co_value':     aqi_last['CO']['value'],
        'co_aqi':       aqi_last['CO']['aqi'],
        'no2_value':    aqi_last['NO2']['value'],
        'no2_aqi':      aqi_last['NO2']['aqi'],
        'so2_value':    aqi_last['SO2']['value'],
        'so2_aqi':      aqi_last['SO2']['aqi'],
        **ow,
        'created_at': created_at,
    }


def ingest_all() -> pd.DataFrame:
    """Fetch every AIR4Thai station, enrich it with OpenWeather, and return one row per usable station.

    Malformed stations are skipped with a warning. If every station fails, RuntimeError is raised.
    """
    rows = []
    skipped = 0
    created_at = now_bkk_text()
    stations = fetch_air4thai()
    for station in stations:
        station_id = station.get('stationID', '<unknown>') if isinstance(station, dict) else '<unknown>'
        try:
            aqi_last = station['AQILast']
            lat = station['lat']
            lon = station['long']

            ow = fetch_openweather(lat, lon)

            rows.append(build_station_row(station, ow, created_at))
        except (KeyError, IndexError, TypeError, ValueError, AttributeError, requests.RequestException) as e:
            # One station's malformed payload or a flaky OpenWeather call (timeout,
            # 429 rate-limit, missing field) must not blow away the other ~170
            # stations already fetched in this batch. Skip and retry next run.
            # IndexError covers an empty OpenWeather 'list'/'weather' array (FIX-1).
            skipped += 1
            logging.warning("Skipping station %s due to %s: %s", station_id, type(e).__name__, e)

    total = len(stations)
    if skipped:
        logging.warning("Ingest completed with %d/%d station(s) skipped", skipped, total)
    if total and skipped == total:
        # Every station hit the same exception (expired API key, AIR4Thai/OpenWeather
        # outage, a field renamed pipeline-wide, ...) rather than one bad station -
        # that's a systemic failure, not an isolated one. Raise so the Airflow task
        # fails loudly (and retries per default_args) instead of silently pushing an
        # empty DataFrame and logging a green "Ingested 0 rows successfully".
        raise RuntimeError(f"All {total} station(s) failed to ingest; see warnings above for causes")

    return pd.DataFrame(rows)


def push_to_supabase(df: pd.DataFrame, table: str = "air_stations") -> None:
    """Upsert the ingest DataFrame into `table` on (station_id, recorded_at). Skips empty frames."""
    if df.empty:
        logging.warning("No rows to push to '%s' (empty ingest result); skipping upsert", table)
        return
    records = df.to_dict(orient='records')
    get_client().table(table).upsert(records, on_conflict='station_id,recorded_at').execute()
    logging.info("Pushed %d rows to '%s'", len(records), table)


def get_stationfromsupabase() -> list:
    """Return all distinct station_ids via the get_distinct_stations RPC. Not used by the DAGs; kept for ad-hoc use."""
    response = get_client().rpc('get_distinct_stations', {}).execute()
    return [row['station_id'] for row in response.data]


def get_latest_created_at(table: str = 'air_stations') -> str | None:
    """Return the newest created_at in `table`, or None when empty. Not used by the DAGs; kept for ad-hoc use."""
    response = get_client().table(table).select('created_at').order('created_at', desc=True).limit(1).execute()
    if response.data:
        return response.data[0]['created_at']
    return None


def truncate_all_stations() -> None:
    """Truncate every public station_* table via the exec_sql RPC."""
    sql = """
    DO $$
    DECLARE r RECORD;
    BEGIN
      FOR r IN SELECT tablename FROM pg_tables
               WHERE schemaname = 'public' AND tablename LIKE 'station_%'
      LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename);
      END LOOP;
    END;
    $$;
    """
    get_client().rpc('exec_sql', {'sql': sql}).execute()
    logging.info("Truncated all station_* tables")


def _reload_rpc_params(mode: str, date: str | None = None, date_from: str | None = None,
                       date_to: str | None = None) -> dict:
    """Build the p_mode/p_date/p_date_from/p_date_to RPC args, turning blank strings into None."""
    return {
        'p_mode':       mode,
        'p_date':       date or None,
        'p_date_from':  date_from or None,
        'p_date_to':    date_to or None,
    }


def transform_all(mode: str = 'latest', date: str | None = None, date_from: str | None = None,
                  date_to: str | None = None, station_id: str | None = None) -> int:
    """Run transform_all_stations RPC and return its count. Not used by the DAGs; kept for ad-hoc use."""
    response = get_client().rpc('transform_all_stations', {
        **_reload_rpc_params(mode, date, date_from, date_to),
        'p_station_id': station_id or None,
    }).execute()
    count = response.data
    logging.info("Batch-transformed %s stations [%s]", count, mode)
    return count


def list_stations(mode: str = 'latest', date: str | None = None, date_from: str | None = None,
                  date_to: str | None = None, station_id: str | None = None) -> list:
    """Return the station_ids that a reload window selects (get_distinct_stations RPC)."""
    response = get_client().rpc('get_distinct_stations', {
        **_reload_rpc_params(mode, date, date_from, date_to),
        'p_station_id': station_id or None,
    }).execute()
    stations = [row['station_id'] for row in response.data]
    logging.info("%d station(s) to transform [%s]", len(stations), mode)
    return stations


def build_dim_fact(mode: str = 'full', date: str | None = None, date_from: str | None = None,
                   date_to: str | None = None, station_id: str | None = None) -> int:
    """Run the build_dim_fact RPC for the given window and return the fact row count."""
    response = get_client().rpc('build_dim_fact', {
        **_reload_rpc_params(mode, date, date_from, date_to),
        'p_station_id': station_id or None,
    }).execute()
    count = response.data
    logging.info("Built dim/fact mart: %s fact rows [%s]", count, mode)
    return count


def transform_station(station_id: str, mode: str = 'latest', date: str | None = None,
                      date_from: str | None = None, date_to: str | None = None) -> int:
    """Run the transform_station RPC for one station and return the row count."""
    response = get_client().rpc('transform_station', {
        'p_station_id': station_id,
        **_reload_rpc_params(mode, date, date_from, date_to),
    }).execute()
    count = response.data
    logging.info("Transformed station %s -> %s rows [%s]", station_id, count, mode)
    return count
