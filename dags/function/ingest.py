import os
import requests
import pandas as pd
import pytz
import re
import logging
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime
from pprint import pprint

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../assets/.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

air4thai_url      = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
openweather_air   = "https://api.openweathermap.org/data/2.5/air_pollution"
openweather_wx    = "https://api.openweathermap.org/data/2.5/weather"

client = create_client(SUPABASE_URL, SUPABASE_KEY)

import os

def load_sql(filename: str) -> str:
    path = os.path.join(os.path.dirname(__file__), '../../dags/sqlscript', filename)
    return open(path).read()


def fetch_air4thai() -> list:
    response = requests.get(air4thai_url, timeout=30)
    response.raise_for_status()
    return response.json()['stations']


def fetch_openweather(lat: float, lon: float) -> dict:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    params  = {"lat": lat, "lon": lon, "appid": api_key}

    air     = requests.get(openweather_air, params=params, timeout=30)
    weather = requests.get(openweather_wx,  params={**params, "units": "metric"}, timeout=30)

    air.raise_for_status()
    weather.raise_for_status()

    air_data = air.json()['list'][0]
    wx_data  = weather.json()

    c = air_data['components']

    return {
        "ow_aqi":         air_data['main']['aqi'],
        "ow_co":          c['co'],
        "ow_no":          c['no'],
        "ow_no2":         c['no2'],
        "ow_o3":          c['o3'],
        "ow_so2":         c['so2'],
        "ow_pm25":        c['pm2_5'],
        "ow_pm10":        c['pm10'],
        "ow_nh3":         c['nh3'],
        "ow_temp":        wx_data['main']['temp'],
        "ow_feels_like":  wx_data['main']['feels_like'],
        "ow_humidity":    wx_data['main']['humidity'],
        "ow_pressure":    wx_data['main']['pressure'],
        "ow_wind_speed":  wx_data['wind']['speed'],
        "ow_wind_deg":    wx_data['wind']['deg'],
        "ow_clouds":      wx_data['clouds']['all'],
        "ow_weather":     wx_data['weather'][0]['description'],
    }


def ingest_all() -> pd.DataFrame:
    rows = []
    bkk_tz = pytz.timezone("Asia/Bangkok")
    created_at = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M:%S')
    for station in fetch_air4thai():
        aqi_last = station['AQILast']
        lat = station['lat']
        lon = station['long']

        ow = fetch_openweather(lat, lon)

        rows.append({
            'station_id':   station['stationID'],
            'area_th':      station['areaTH'],
            'area_en':      station['areaEN'],
            'station_type': station['stationType'],
            'lat':          float(lat),
            'lon':          float(lon),
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
        })

    return pd.DataFrame(rows)


def push_to_supabase(df: pd.DataFrame, table: str = "air_stations"):
    records = df.to_dict(orient='records')
    client.table(table).upsert(records, on_conflict='station_id,recorded_at').execute()
    print(f"Pushed {len(records)} rows to '{table}'")


def get_stationfromsupabase():
    response = client.rpc('get_distinct_stations', {}).execute()
    return [row['station_id'] for row in response.data]

def get_latest_created_at(table: str = 'air_stations') -> str:
    response = client.table(table).select('created_at').order('created_at', desc=True).limit(1).execute()
    if response.data:
        return response.data[0]['created_at']
    return None

def truncate_all_stations():
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
    client.rpc('exec_sql', {'sql': sql}).execute()
    logging.info("Truncated all station_* tables")

def transform_all(snapshot: str = 'latest') -> int:
    """Transform every station for `snapshot` in a single server-side call.

    Calls the transform_all_stations() Postgres function (see
    dags/sqlscript/transform-all-stations.sql), which loops the stations inside
    the database — one round-trip instead of one per station. Returns the number
    of stations processed.
    """
    response = client.rpc('transform_all_stations', {'p_snapshot': snapshot}).execute()
    count = response.data
    logging.info(f"Batch-transformed {count} stations [{snapshot}]")
    return count


def transform_station(station_id: str, snapshot_at: str = None, date_from: str = None, date_to: str = None):
    tbl = 'station_' + re.sub(r'[^a-zA-Z0-9]', '_', station_id)

    if snapshot_at == 'latest' or (snapshot_at is None and date_from is None and date_to is None):
        date_filter = "AND created_at = (SELECT MAX(created_at) FROM air_stations)"
    elif snapshot_at:
        date_filter = f"AND created_at = '{snapshot_at}'"
    elif date_from and date_to:
        date_filter = f"AND created_at::date BETWEEN '{date_from}' AND '{date_to}'"
    elif date_from:
        date_filter = f"AND created_at::date = '{date_from}'"
    else:
        date_filter = ""

    sql = load_sql('air-station-transform.sql').format(
        tbl=tbl,
        station_id=station_id,
        date_filter=date_filter,
    )
    client.rpc('exec_sql', {'sql': sql}).execute()
    logging.info(f"Transformed station {station_id} → {tbl} [{snapshot_at or date_filter or 'full dump'}]")